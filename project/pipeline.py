import pandas as pd
import sqlite3
import os
import requests
import matplotlib.pyplot as plt

# Data directory
data_dir = os.path.join("../data")
climate_csv_path = os.path.join(data_dir, "GlobalLandTemperaturesByCountry.csv")
agri_csv_path = os.path.join(data_dir, "FAOSTAT_data_en_customize.csv")  
db_path = os.path.join(data_dir, "project_data.db")

# Check data directory exists
os.makedirs(data_dir, exist_ok=True)

# Download Climate Data if not already downloaded
if not os.path.exists(climate_csv_path):
    print("Downloading Climate Data...")
    climate_url = "https://downloads.sourceforge.net/project/faostat-data-en-11-13-2024-csv/GlobalLandTemperaturesByCountry.csv"
    
    response = requests.get(climate_url)
    with open(climate_csv_path, "wb") as file:
        file.write(response.content)

# Load Datasets
print("Loading Climate Data...")
climate_data = pd.read_csv(climate_csv_path)

# Process climate data: Ensure 'dt' is in datetime format, extract year, and aggregate by year
climate_data['dt'] = pd.to_datetime(climate_data['dt'])
climate_data['Year'] = climate_data['dt'].dt.year
annual_climate_data = climate_data.groupby(['Year', 'Country']).agg(
    {
        'AverageTemperature': 'mean',
        'AverageTemperatureUncertainty': 'mean'
    }).reset_index()

# Check the aggregated data
annual_climate_data.head()

# Download Agricultural Data if not already downloaded
if not os.path.exists(agri_csv_path):
    print("Downloading Agricultural Data...")
    agri_url = "https://sourceforge.net/projects/faostat-data-en-11-13-2024-csv/files/FAOSTAT_data_en_customize.csv/download"
    
    response = requests.get(agri_url, allow_redirects=True)
    with open(agri_csv_path, "wb") as file:
        file.write(response.content)

# Load Agricultural Data
print("Loading Agricultural Data...")
agri_data = pd.read_csv(agri_csv_path)

# Mapping of the countries
country_mapping ={
    "United States of America": "United States",
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Côte D'Ivoire": "Ivory Coast",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Russian Federation": "Russia",
    "Iran (Islamic Republic of)": "Iran",
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
}

# List of countries in North and South America
american_countries = [
    "United States", "Canada", "Mexico", "Brazil", "Argentina", "Colombia", "Chile", "Peru",
    "Venezuela", "Bolivia", "Paraguay", "Uruguay", "Ecuador", "Guyana", "Suriname", "Belize",
    "Costa Rica", "Cuba", "Dominican Republic", "El Salvador", "Guatemala", "Honduras",
    "Nicaragua", "Panama", "Trinidad and Tobago", "Jamaica", "Bahamas", "Barbados"
]

# List of crops
crops = [
    "Maize (corn)", "Rice", "Sugar cane", "Wheat"
]

# Apply country mapping
annual_climate_data["Country"] = annual_climate_data["Country"].replace(country_mapping)
agri_data["Area"] = agri_data["Area"].replace(country_mapping)

# Filter data for American countries and selected crops
annual_climate_data = annual_climate_data[annual_climate_data["Country"].isin(american_countries)]
agri_data = agri_data[agri_data["Area"].isin(american_countries)]
agri_data = agri_data[agri_data["Item"].isin(crops)]

# Merge Climate and Agricultural Data on 'Country' and 'Year'
merged_data = pd.merge(agri_data, annual_climate_data, left_on=["Year Code", "Area"], right_on=["Year", "Country"])

# Store Merged Data in SQLite
print("Storing Merged Data to SQLite Database...")
conn = sqlite3.connect(db_path)
merged_data.to_sql("MergedData", conn, if_exists="replace", index=False)
conn.commit()
conn.close()
print("Data pipeline completed. Data stored in project_data.db.")
