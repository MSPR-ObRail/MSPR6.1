import pandas as pd
import os

# Paths
night_file = 'data/extracted/night_routes.csv'
day_file = 'data/extracted/day_routes.csv'
processed_folder = 'data/transformed/'
os.makedirs(processed_folder, exist_ok=True)

def clean_routes(df, train_type):
    """
    Cleans a routes DataFrame:
    - Fill missing route_name_simple
    - Deduplicate bidirectional routes
    - Normalize capitalization
    - Add 'type' column
    """
    # Strip whitespace
    df['origin'] = df['origin'].str.strip()
    df['destination'] = df['destination'].str.strip()
    
    # Fill route_name_simple if missing
    if 'route_name_simple' not in df.columns:
        df['route_name_simple'] = ""
    df['route_name_simple'] = df.apply(
        lambda row: row['route_name_simple'] if pd.notna(row['route_name_simple']) and row['route_name_simple'].strip() != ""
        else f"{row['origin'].title()} → {row['destination'].title()}", axis=1
    )
    
    # Normalize capitalization for origin/destination
    df['origin'] = df['origin'].str.title()
    df['destination'] = df['destination'].str.title()
    
    # Deduplicate bidirectional routes
    df['route_pair'] = df.apply(lambda r: tuple(sorted([r['origin'], r['destination']])), axis=1)
    df = df.drop_duplicates(subset=['route_pair'], keep='first')
    df = df.drop(columns=['route_pair'])
    
    # Add type column
    df['type'] = train_type
    
    return df

# --- CLEAN NIGHT ROUTES ---
print("Cleaning night routes...")
night_df = pd.read_csv(night_file)
night_df_cleaned = clean_routes(night_df, 'night')
night_cleaned_file = os.path.join(processed_folder, 'night_routes_cleaned.csv')
night_df_cleaned.to_csv(night_cleaned_file, index=False)
print(f"✓ Night routes cleaned and saved: {night_cleaned_file} ({len(night_df_cleaned)} routes)")

# --- CLEAN DAY ROUTES ---
print("Cleaning day routes...")
day_df = pd.read_csv(day_file)
day_df_cleaned = clean_routes(day_df, 'day')
day_cleaned_file = os.path.join(processed_folder, 'day_routes_cleaned.csv')
day_df_cleaned.to_csv(day_cleaned_file, index=False)
print(f"✓ Day routes cleaned and saved: {day_cleaned_file} ({len(day_df_cleaned)} routes)")
