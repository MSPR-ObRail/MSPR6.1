import pandas as pd
import unicodedata
import re
import os

# ----------------------------
# CONFIGURATION
# ----------------------------
GTFS_DAY_FOLDERS = [
    "data/raw/day/Denmark/",
    "data/raw/day/Eurostar_international/",
    "data/raw/day/France/",
    "data/raw/day/Germany/",
    "data/raw/day/Switzerland/"
]

OUTPUT_FILE = "data/extracted/day_routes.csv"

# Country code per folder
# Eurostar is cross-border so we resolve it per station instead
FOLDER_COUNTRY_MAP = {
    "data/raw/day/Denmark/": "DK",
    "data/raw/day/Eurostar_international/": None,
    "data/raw/day/France/": "FR",
    "data/raw/day/Germany/": "DE",
    "data/raw/day/Switzerland/": "CH"
}

# For cross-border folders, map origin stations to countries manually
STATION_COUNTRY_MAP = {
    "st pancras international": "GB",
    "london st pancras": "GB",
    "paris nord": "FR",
    "paris gare du nord": "FR",
    "bruxelles midi": "BE",
    "brussels midi": "BE",
    "amsterdam centraal": "NL",
    "rotterdam centraal": "NL",
    "lille europe": "FR",
    "calais ville": "FR",
    "dunkerque": "FR",
    "basel": "CH",
    "zurich": "CH",
    "koln": "DE",
    "cologne": "DE",
    "dortmund": "DE",
    "essen": "DE",
    "marne la vallee": "FR",
    "bourg st maurice": "FR",
}

# ----------------------------
# FUNCTION: Normalize station names
# ----------------------------
def normalize(name):
    if not isinstance(name, str) or name.strip() == "":
        return ""
    name = name.lower().strip()
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode()
    name = re.sub(r'[^a-z0-9 ]', ' ', name)
    name = re.sub(r'\s+', ' ', name)
    return name.strip()

# ----------------------------
# FUNCTION: Simplify station for dashboard
# ----------------------------
def simplify_station_advanced(name):
    if not isinstance(name, str) or name.strip() == "":
        return ""
    
    name = name.strip().lower()
    remove_terms = ["bf", "hbf", "tief", "bad", "gare", "routiere"]
    pattern = r'\b(' + '|'.join(remove_terms) + r')\b'
    name = re.sub(pattern, '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    small_words = ["de","du","des","la","le","les","d'","l'","à"]
    words = []
    for w in name.split():
        if w in small_words:
            words.append(w)
        else:
            words.append(w.capitalize())
    
    return ' '.join(words)

# ----------------------------
# FUNCTION: Fix route name if empty or same as origin
# ----------------------------
def fix_route_name(row):
    if pd.isna(row['route_name']) or row['route_name'].strip() == "" or row['route_name'].strip() == row['origin'].strip():
        return f"{row['origin']} → {row['destination']}"
    else:
        return row['route_name']

# ----------------------------
# FUNCTION: Resolve country from station name (for cross-border folders)
# ----------------------------
def resolve_country(station_normalized, folder_country):
    if folder_country is not None:
        return folder_country
    for key, code in STATION_COUNTRY_MAP.items():
        if key in station_normalized:
            return code
    return None

# ----------------------------
# FUNCTION: Extract routes from a GTFS folder
# ----------------------------
def extract_routes(folder):
    files = os.listdir(folder)
    has_stop_times = "stop_times.txt" in files
    has_stops = "stops.txt" in files
    has_trips = "trips.txt" in files
    has_routes = "routes.txt" in files

    if not (has_stops and has_trips and has_routes):
        print(f"⚠ Skipping {folder}, missing essential files")
        return pd.DataFrame()

    # FIX: if no stop_times.txt, check trips.txt has start/end columns before continuing
    if not has_stop_times:
        trips_check = pd.read_csv(os.path.join(folder, "trips.txt"), dtype=str, nrows=1)
        if 'start_stop_id' not in trips_check.columns or 'end_stop_id' not in trips_check.columns:
            print(f"⚠ Skipping {folder}, no stop_times.txt and no start/end stop columns in trips.txt")
            return pd.DataFrame()

    stops = pd.read_csv(os.path.join(folder, "stops.txt"), dtype=str, low_memory=False)
    trips = pd.read_csv(os.path.join(folder, "trips.txt"), dtype=str, low_memory=False)
    routes = pd.read_csv(os.path.join(folder, "routes.txt"), dtype=str, low_memory=False)
    
    if has_stop_times:
        stop_times = pd.read_csv(os.path.join(folder, "stop_times.txt"), dtype=str, low_memory=False)
        stop_times = stop_times.sort_values(["trip_id", "stop_sequence"])
        first_stops = stop_times.groupby("trip_id").first().reset_index()
        last_stops = stop_times.groupby("trip_id").last().reset_index()
        
        first_stops = first_stops.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')
        last_stops = last_stops.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')
    else:
        # Safe fallback — only reaches here if start_stop_id/end_stop_id confirmed above
        first_stops = trips[['trip_id']].copy()
        first_stops['stop_id'] = trips['start_stop_id']
        last_stops = trips[['trip_id']].copy()
        last_stops['stop_id'] = trips['end_stop_id']
        
        first_stops = first_stops.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')
        last_stops = last_stops.merge(stops[['stop_id', 'stop_name']], on='stop_id', how='left')

    df = trips.merge(routes[['route_id', 'route_short_name']], on='route_id', how='left')
    df = df.merge(first_stops[['trip_id', 'stop_name']], on='trip_id')
    df = df.merge(last_stops[['trip_id', 'stop_name']], on='trip_id', suffixes=('_origin', '_destination'))
    
    df = df[['route_short_name', 'stop_name_origin', 'stop_name_destination']]
    df = df.dropna(subset=['stop_name_origin', 'stop_name_destination'])
    df = df[df['stop_name_origin'].str.strip() != ""]
    df = df[df['stop_name_destination'].str.strip() != ""]
    
    df['origin'] = df['stop_name_origin'].apply(normalize)
    df['destination'] = df['stop_name_destination'].apply(normalize)
    df['route_name'] = df['route_short_name']
    df['service_type'] = "day"
    
    df['route_name'] = df.apply(fix_route_name, axis=1)
    
    # Deduplicate bidirectional routes
    df['pair'] = df.apply(lambda r: tuple(sorted([r['origin'], r['destination']])), axis=1)
    df = df.drop_duplicates(subset='pair')
    
    # Country mapping
    folder_country = FOLDER_COUNTRY_MAP.get(folder)
    df['origin_country'] = df['origin'].apply(lambda o: resolve_country(o, folder_country))
    df['destination_country'] = df['destination'].apply(lambda d: resolve_country(d, folder_country))
    
    # Flag unresolved
    unresolved = df[df['origin_country'].isna() | df['destination_country'].isna()]
    if not unresolved.empty:
        print(f"   ⚠ {len(unresolved)} routes with unresolved country:")
        print(unresolved[['origin', 'destination', 'origin_country', 'destination_country']].to_string())

    # Dashboard-friendly names
    df['origin_simple'] = df['origin'].apply(simplify_station_advanced)
    df['destination_simple'] = df['destination'].apply(simplify_station_advanced)
    df['route_name_simple'] = df['origin_simple'] + " → " + df['destination_simple']
    
    return df[['route_name', 'origin', 'destination', 'service_type', 'route_name_simple', 'origin_country', 'destination_country']]

# ----------------------------
# MAIN SCRIPT
# ----------------------------
all_routes = []

for folder in GTFS_DAY_FOLDERS:
    print(f"\n➡ Processing folder: {folder}")
    df_routes = extract_routes(folder)
    if not df_routes.empty:
        print(f"   Extracted {len(df_routes)} routes")
        all_routes.append(df_routes)
    else:
        print("   No routes extracted")

if all_routes:
    master_day = pd.concat(all_routes).drop_duplicates(subset=['origin', 'destination', 'route_name'])
    master_day.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Master day routes CSV created: {OUTPUT_FILE}")
    print(f"Total routes: {len(master_day)}")
    print(f"\nCountry breakdown (origin):")
    print(master_day['origin_country'].value_counts(dropna=False).to_string())
else:
    print("❌ No day routes were extracted from any folder")