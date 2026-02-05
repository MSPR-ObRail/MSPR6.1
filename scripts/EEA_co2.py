import pandas as pd

# 1️⃣ Load CSV
eea = pd.read_csv('data/raw/co2/EEA.csv', engine='python', encoding='utf-8-sig')

# 2️⃣ Strip whitespace from all column names
eea.columns = eea.columns.str.strip()

# 3️⃣ Keep only relevant columns
eea = eea[['CountryShort', 'Year', 'ValueNumeric', 'Unit', 'Market_Sector', 'Data_source']]

# 4️⃣ Rename columns to standard format
eea = eea.rename(columns={
    'CountryShort': 'country',
    'Year': 'year',
    'ValueNumeric': 'value',
    'Unit': 'unit',
    'Market_Sector': 'sector',
    'Data_source': 'source'
})

# 5️⃣ Convert numeric columns to proper type
eea['value'] = pd.to_numeric(eea['value'], errors='coerce')
eea['year'] = pd.to_numeric(eea['year'], errors='coerce')

# 6️⃣ Drop rows with missing country or value
eea = eea.dropna(subset=['country', 'value'])

# 7️⃣ Filter to only the countries in your train datasets
european_countries = ['BE','BG','CZ','DK','DE','EE','EL','ES','FR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK','CH','AT'] 
eea = eea[eea['country'].isin(european_countries)]

# 8️⃣ Optional: sort by country and year
eea = eea.sort_values(['country', 'year']).reset_index(drop=True)

# 9️⃣ Save cleaned CSV
eea.to_csv('data/transformed/EEA_cleaned.csv', index=False)

print("✅ EEA CO₂ dataset cleaned and saved.")
print(eea.head())
