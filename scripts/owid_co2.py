import pandas as pd

# Load OWID CSV
df = pd.read_csv("data/raw/co2/owid-energy-data.csv")

# Columns to keep
keep_cols = [
    'country', 'iso_code', 'year', 'population', 'gdp',
    'fossil_electricity', 'renewables_electricity', 'carbon_intensity_elec',
    'greenhouse_gas_emissions', 'coal_consumption', 'oil_consumption',
    'gas_consumption', 'biofuel_consumption'
]

df = df[keep_cols]

# Strip whitespace from string columns
df['country'] = df['country'].str.strip()

# Convert numeric columns
numeric_cols = ['population','gdp','fossil_electricity','renewables_electricity',
                'carbon_intensity_elec','greenhouse_gas_emissions','coal_consumption',
                'oil_consumption','gas_consumption','biofuel_consumption']

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Filter for year 2024 (optional)
df_2024 = df[df['year'] == 2024]

# Save cleaned dataset
df_2024.to_csv("data/transformed/owid_cleaned.csv", index=False)
print("✓ OWID CO2 data cleaned and saved")
