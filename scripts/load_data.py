import pandas as pd
from sqlalchemy import create_engine

# ----------------------------
# CONFIGURATION — update your connection string here
# ----------------------------
DATABASE_URL = "postgresql://user:password@localhost:5432/obrail"

engine = create_engine(DATABASE_URL)

# ----------------------------
# STEP 1: Load CO2 data
# ----------------------------
print("Loading CO2 data...")
co2 = pd.read_csv("data/transformed/EEA_cleaned.csv")
co2 = co2.rename(columns={
    "country": "country_code",
    "value": "value",
    "unit": "unit",
    "sector": "sector",
    "source": "source",
    "year": "year"
})
# Keep only the columns we need
co2 = co2[["country_code", "year", "value", "unit", "sector", "source"]]
co2 = co2.drop_duplicates(subset=["country_code", "year", "sector"])

co2.to_sql("co2_emission", engine, if_exists="append", index=False)
print(f"  ✅ CO2: {len(co2)} rows loaded")

# ----------------------------
# STEP 2: Build station table from routes
# ----------------------------
print("Building station table...")

day = pd.read_csv("data/transformed/day_routes_cleaned.csv")
night = pd.read_csv("data/transformed/night_routes_cleaned.csv")

# Combine all routes
all_routes = pd.concat([day, night], ignore_index=True)

# Extract unique stations from origins
origins = all_routes[["origin", "origin_simple", "origin_country"]].drop_duplicates(subset=["origin"])
origins.columns = ["station_name", "display_name", "country_code"]

# Extract unique stations from destinations
destinations = all_routes[["destination", "destination_simple", "destination_country"]].drop_duplicates(subset=["destination"])
destinations.columns = ["station_name", "display_name", "country_code"]

# Merge and deduplicate
stations = pd.concat([origins, destinations]).drop_duplicates(subset=["station_name"])
stations = stations.reset_index(drop=True)

# Filter out stations with no country (they won't link to CO2 anyway, but keeps DB clean)
stations_with_country = stations[stations["country_code"].notna()].copy()
stations_no_country = stations[stations["country_code"].isna()] if "country_country" in stations.columns else stations[stations["country_code"].isna()]

print(f"  Stations with country: {len(stations_with_country)}")
print(f"  Stations without country (skipped): {len(stations_no_country)}")

stations_with_country.to_sql("station", engine, if_exists="append", index=False)
print(f"  ✅ Stations: {len(stations_with_country)} rows loaded")

# ----------------------------
# STEP 3: Load routes
# ----------------------------
print("Loading routes...")

# We need to map station names back to station_ids
# Read the station table back to get the IDs assigned by the DB
station_df = pd.read_sql("SELECT station_id, station_name FROM station", engine)
station_map = dict(zip(station_df["station_name"], station_df["station_id"]))

# Map origin and destination to station IDs
all_routes["origin_station_id"] = all_routes["origin"].map(station_map)
all_routes["destination_station_id"] = all_routes["destination"].map(station_map)

# Only keep routes where BOTH stations resolved
routes_clean = all_routes.dropna(subset=["origin_station_id", "destination_station_id"]).copy()
routes_dropped = len(all_routes) - len(routes_clean)

# Rename and select columns for the route table
routes_clean = routes_clean.rename(columns={
    "route_name": "route_name",
    "service_type": "service_type",
    "origin_country": "origin_country",
    "destination_country": "destination_country"
})

routes_final = routes_clean[[
    "route_name",
    "origin_station_id",
    "destination_station_id",
    "service_type",
    "origin_country",
    "destination_country"
]].drop_duplicates(subset=["origin_station_id", "destination_station_id", "service_type"])

# Convert IDs to int
routes_final["origin_station_id"] = routes_final["origin_station_id"].astype(int)
routes_final["destination_station_id"] = routes_final["destination_station_id"].astype(int)

routes_final.to_sql("route", engine, if_exists="append", index=False)
print(f"  ✅ Routes: {len(routes_final)} rows loaded")
print(f"  ⚠ Routes dropped (unresolved stations): {routes_dropped}")

# ----------------------------
# SUMMARY
# ----------------------------
print("\n===== DATABASE LOAD SUMMARY =====")
for table in ["country", "station", "route", "co2_emission"]:
    count = pd.read_sql(f"SELECT COUNT(*) as n FROM {table}", engine).iloc[0]["n"]
    print(f"  {table}: {count} rows")
print("=================================\n")