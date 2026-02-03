from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, udf, lit, 
    concat, first, last, when, coalesce, row_number
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import unicodedata
import re
import os

# ----------------------------
# CONFIGURATION
# ----------------------------
GTFS_NIGHT_FOLDERS = [
    "data/raw/night/Switzerland/",
    "data/raw/night/long_distance/",
    "data/raw/night/sncf-data/",
    "data/raw/night/open_data/",
    "data/raw/night/long_distance_rail/"
]

OUTPUT_FILE = "data/extracted/night_routes.csv"

# Country code per folder
FOLDER_COUNTRY_MAP = {
    "data/raw/night/Switzerland/": "CH",
    "data/raw/night/long_distance/": None,
    "data/raw/night/sncf-data/": "FR",
    "data/raw/night/open_data/": None,
    "data/raw/night/long_distance_rail/": None
}

# For cross-border folders, map stations to countries manually
STATION_COUNTRY_MAP = {
    "basel": "CH",
    "zurich": "CH",
    "brig": "CH",
    "lausanne": "CH",
    "geneve": "CH",
    "geneva": "CH",
    "montreux": "CH",
    "olten": "CH",
    "liestal": "CH",
    "como": "IT",
    "chiasso": "IT",
    "lugano": "IT",
    "bellinzona": "IT",
    "domodossola": "IT",
    "simplontunnel": "IT",
    "paris nord": "FR",
    "paris gare du nord": "FR",
    "lyon": "FR",
    "marseille": "FR",
    "nice": "FR",
    "bruxelles midi": "BE",
    "brussels midi": "BE",
    "amsterdam centraal": "NL",
    "rotterdam centraal": "NL",
    "wien": "AT",
    "vienna": "AT",
    "munchen": "DE",
    "munich": "DE",
    "berlin": "DE",
    "hamburg": "DE",
    "koln": "DE",
    "cologne": "DE",
    "frankfurt": "DE",
    "salzburg": "AT",
    "innsbruck": "AT",
    "graz": "AT",
    "prague": "CZ",
    "budapest": "HU",
}

# ----------------------------
# Initialize Spark Session
# ----------------------------
def init_spark():
    """Initialize and return a Spark session"""
    spark = SparkSession.builder \
        .appName("GTFS Night Routes Extraction") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

# ----------------------------
# UDF: Normalize station names
# ----------------------------
def normalize_text(name):
    """Normalize station name by removing accents and special characters"""
    if not name or name.strip() == "":
        return ""
    name = name.lower().strip()
    # Remove accents
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    # Keep only alphanumeric and spaces
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    # Collapse multiple spaces
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

normalize_udf = udf(normalize_text, StringType())

# ----------------------------
# UDF: Simplify station for dashboard
# ----------------------------
def simplify_station_text(name):
    """Simplify station name for dashboard display"""
    if not name or name.strip() == "":
        return ""
    
    name = name.strip().lower()
    
    # Remove common terms
    remove_terms = ["bf", "hbf", "tief", "bad", "gare", "routiere"]
    pattern = r'\b(' + '|'.join(remove_terms) + r')\b'
    name = re.sub(pattern, '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Capitalize appropriately
    small_words = ["de", "du", "des", "la", "le", "les", "d'", "l'", "à"]
    words = []
    for w in name.split():
        if w in small_words:
            words.append(w)
        else:
            words.append(w.capitalize())
    
    return ' '.join(words)

simplify_station_udf = udf(simplify_station_text, StringType())

# ----------------------------
# UDF: Resolve country from station name
# ----------------------------
def resolve_country_from_station(station_normalized, folder_country):
    """Resolve country code from station name for cross-border routes"""
    if folder_country is not None and folder_country != "":
        return folder_country
    
    for key, code in STATION_COUNTRY_MAP.items():
        if key in station_normalized.lower():
            return code
    
    return None

resolve_country_udf = udf(resolve_country_from_station, StringType())

# ----------------------------
# FUNCTION: Extract routes from a GTFS folder using PySpark
# ----------------------------
def extract_routes_pyspark(spark, folder, folder_country):
    """Extract routes from a GTFS folder using PySpark"""
    
    # Check if required files exist
    required_files = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
    missing = [f for f in required_files if not os.path.exists(os.path.join(folder, f))]
    
    if missing:
        print(f"⚠ Skipping {folder}, missing files: {missing}")
        return None
    
    try:
        # Read GTFS files
        stops = spark.read.csv(
            os.path.join(folder, "stops.txt"),
            header=True,
            inferSchema=True
        )
        
        stop_times = spark.read.csv(
            os.path.join(folder, "stop_times.txt"),
            header=True,
            inferSchema=True
        )
        
        trips = spark.read.csv(
            os.path.join(folder, "trips.txt"),
            header=True,
            inferSchema=True
        )
        
        routes = spark.read.csv(
            os.path.join(folder, "routes.txt"),
            header=True,
            inferSchema=True
        )
        
        # Cast stop_sequence to integer for proper sorting
        stop_times = stop_times.withColumn("stop_sequence", col("stop_sequence").cast("int"))
        
        # Get first and last stops for each trip
        window_spec = Window.partitionBy("trip_id").orderBy("stop_sequence")
        
        stop_times_with_rank = stop_times.withColumn(
            "rank", row_number().over(window_spec)
        )
        
        # First stops
        first_stops = stop_times_with_rank.filter(col("rank") == 1) \
            .select("trip_id", "stop_id") \
            .join(stops.select("stop_id", col("stop_name").alias("origin_name")), 
                  on="stop_id", how="left")
        
        # Last stops - get max rank per trip
        window_spec_desc = Window.partitionBy("trip_id").orderBy(col("stop_sequence").desc())
        stop_times_with_rank_desc = stop_times.withColumn(
            "rank_desc", row_number().over(window_spec_desc)
        )
        
        last_stops = stop_times_with_rank_desc.filter(col("rank_desc") == 1) \
            .select("trip_id", "stop_id") \
            .join(stops.select("stop_id", col("stop_name").alias("destination_name")), 
                  on="stop_id", how="left")
        
        # Join everything together
        df = trips.join(
            routes.select("route_id", "route_short_name"), 
            on="route_id", 
            how="left"
        )
        
        df = df.join(first_stops.select("trip_id", "origin_name"), on="trip_id", how="left")
        df = df.join(last_stops.select("trip_id", "destination_name"), on="trip_id", how="left")
        
        # Filter out null or empty stations
        df = df.filter(
            (col("origin_name").isNotNull()) & 
            (col("destination_name").isNotNull()) &
            (trim(col("origin_name")) != "") &
            (trim(col("destination_name")) != "")
        )
        
        # Normalize station names
        df = df.withColumn("origin", normalize_udf(col("origin_name")))
        df = df.withColumn("destination", normalize_udf(col("destination_name")))
        
        # Set service type
        df = df.withColumn("service_type", lit("night"))
        
        # Create route_name - fix if empty or same as origin
        df = df.withColumn(
            "route_name",
            when(
                (col("route_short_name").isNull()) | 
                (trim(col("route_short_name")) == "") |
                (trim(col("route_short_name")) == trim(col("origin_name"))),
                concat(col("origin_name"), lit(" → "), col("destination_name"))
            ).otherwise(col("route_short_name"))
        )
        
        # Deduplicate bidirectional routes
        # Create a sorted pair for deduplication
        df = df.withColumn(
            "station_pair",
            when(col("origin") < col("destination"),
                 concat(col("origin"), lit("_"), col("destination")))
            .otherwise(concat(col("destination"), lit("_"), col("origin")))
        )
        
        df = df.dropDuplicates(["station_pair"])
        
        # Resolve countries
        df = df.withColumn(
            "origin_country",
            resolve_country_udf(col("origin"), lit(folder_country))
        )
        
        df = df.withColumn(
            "destination_country",
            resolve_country_udf(col("destination"), lit(folder_country))
        )
        
        # Check for unresolved countries
        unresolved = df.filter(
            col("origin_country").isNull() | col("destination_country").isNull()
        )
        
        unresolved_count = unresolved.count()
        if unresolved_count > 0:
            print(f"   ⚠ {unresolved_count} routes with unresolved country:")
            unresolved.select("origin", "destination", "origin_country", "destination_country").show(truncate=False)
        
        # Create simplified names for dashboard
        df = df.withColumn("origin_simple", simplify_station_udf(col("origin")))
        df = df.withColumn("destination_simple", simplify_station_udf(col("destination")))
        df = df.withColumn(
            "route_name_simple",
            concat(col("origin_simple"), lit(" → "), col("destination_simple"))
        )
        
        # Select final columns
        result = df.select(
            "route_name",
            "origin",
            "destination",
            "service_type",
            "route_name_simple",
            "origin_country",
            "destination_country"
        )
        
        return result
        
    except Exception as e:
        print(f"❌ Error processing {folder}: {str(e)}")
        return None

# ----------------------------
# MAIN SCRIPT
# ----------------------------
def main():
    """Main execution function"""
    
    print("🚀 Starting GTFS Night Routes Extraction with PySpark")
    
    # Initialize Spark
    spark = init_spark()
    
    all_routes = []
    
    for folder in GTFS_NIGHT_FOLDERS:
        print(f"\n➡ Processing folder: {folder}")
        folder_country = FOLDER_COUNTRY_MAP.get(folder)
        
        df_routes = extract_routes_pyspark(spark, folder, folder_country)
        
        if df_routes is not None and df_routes.count() > 0:
            route_count = df_routes.count()
            print(f"   ✓ Extracted {route_count} routes")
            all_routes.append(df_routes)
        else:
            print("   No routes extracted")
    
    if all_routes:
        # Union all dataframes
        from functools import reduce
        master_night = reduce(lambda df1, df2: df1.union(df2), all_routes)
        
        # Remove duplicates across all sources
        master_night = master_night.dropDuplicates(["origin", "destination", "route_name"])
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(OUTPUT_FILE)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Write to CSV (coalesce to single file)
        master_night.coalesce(1).write.csv(
            OUTPUT_FILE.replace('.csv', '_temp'),
            header=True,
            mode='overwrite'
        )
        
        # Rename the part file to the expected output name
        import glob
        temp_files = glob.glob(OUTPUT_FILE.replace('.csv', '_temp') + '/part-*.csv')
        if temp_files:
            import shutil
            shutil.move(temp_files[0], OUTPUT_FILE)
            shutil.rmtree(OUTPUT_FILE.replace('.csv', '_temp'))
        
        total_routes = master_night.count()
        print(f"\n✅ Master night routes CSV created: {OUTPUT_FILE}")
        print(f"Total routes: {total_routes}")
        
        print(f"\nCountry breakdown (origin):")
        master_night.groupBy("origin_country").count().orderBy(col("count").desc()).show()
        
        print(f"\nSample routes:")
        master_night.show(10, truncate=False)
        
    else:
        print("\n❌ No night routes were extracted from any folder")
    
    # Stop Spark session
    spark.stop()
    print("\n✅ Spark session finished")

if __name__ == "__main__":
    main()