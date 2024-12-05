import pandas as pd
import sqlite3
import os
import requests
import matplotlib.pyplot as plt

# Data directory
base_dir = os.path.dirname(os.path.abspath(__file__))  
data_dir = os.path.join(base_dir, "../data")
climate_csv_path = os.path.join(data_dir, "GlobalLandTTemperaturesByCity.csv")
agri_csv_path = os.path.join(data_dir, "FAOSTAT_data_en_11-28-2024.csv")  
db_path = os.path.join(data_dir, "project_data.db")

# Check data directory exists
os.makedirs(data_dir, exist_ok=True)

# Download Climate Data if not already downloaded
if not os.path.exists(climate_csv_path):
    print("Downloading Climate Data...")
    climate_url = "https://sourceforge.net/projects/faostat-data-en-11-13-2024-csv/files/GlobalLandTTemperaturesByCity.csv/download"
    
    response = requests.get(climate_url)
    with open(climate_csv_path, "wb") as file:
        file.write(response.content)

# Download Agricultural Data if not already downloaded
if not os.path.exists(agri_csv_path):
    print("Downloading Agricultural Data...")
    agri_url = "https://sourceforge.net/projects/faostat-data-en-11-13-2024-csv/files/FAOSTAT_data_en_11-28-2024.csv/download"
    
    response = requests.get(agri_url, allow_redirects=True)
    with open(agri_csv_path, "wb") as file:
        file.write(response.content)

# Load Datasets
print("Loading Climate Data...")
climate_data = pd.read_csv(climate_csv_path)

# Load Agricultural Data
print("Loading Agricultural Data...")
agri_data = pd.read_csv(agri_csv_path)

# Data Cleaning and Transformation for Climate Data
print("Processing Climate Data...")
climate_data = climate_data[["dt", "AverageTemperature", "City", "Country"]]
climate_data.dropna(inplace=True)
climate_data["dt"] = pd.to_datetime(climate_data["dt"])
climate_data["Year"] = climate_data["dt"].dt.year  

# Create a mapping for country names (standardizing country names for consistency)
country_mapping = {
    "United States of America": "United States",
    "United States": "United States",  
    "Côte D'Ivoire": "Ivory Coast",  
    "United Kingdom": "UK",  
    
}

# Apply the country mapping 
climate_data["Country"] = climate_data["Country"].replace(country_mapping)
agri_data["Area"] = agri_data["Area"].replace(country_mapping)

# Filter Climate Data
print("Filtering Climate Data for North and South America...")
american_countries = [
    "United States", "Canada", "Mexico", "Brazil", "Argentina", "Colombia", "Chile", "Peru",
    "Venezuela", "Bolivia", "Paraguay", "Uruguay", "Ecuador", "Guyana", "Suriname", "Belize",
    "Costa Rica", "Cuba", "Dominican Republic", "El Salvador", "Guatemala", "Honduras",
    "Nicaragua", "Panama", "Trinidad and Tobago", "Jamaica", "Bahamas", "Barbados"
]
climate_data = climate_data[climate_data["Country"].isin(american_countries)]
climate_data = climate_data[(climate_data["Year"] >= 1961) & (climate_data["Year"] <= 2013)]  

# Data Cleaning and Transformation for Agricultural Data
print("Processing Agricultural Data...")
agri_data = agri_data[["Year", "Area", "Item", "Value", "Unit"]]  
agri_data.dropna(inplace=True)

# Filter Agricultural Data
print("Filtering Agricultural Data for North and South America...")
agri_data = agri_data[agri_data["Area"].isin(american_countries)]

# Merge Climate and Agricultural Data on 'Country' and 'Year'
print("Merging Climate and Agricultural Data...")
merged_data = pd.merge(climate_data, agri_data, left_on=["Year", "Country"], right_on=["Year", "Area"])

# Aggregate Temperature by Year (to average out the temperature data)
temperature_by_year = merged_data.groupby("Year")["AverageTemperature"].mean()

print("Merged Data Preview:")
print(merged_data.head())  

# Check for any missing values
print("Checking for missing values in merged data...")
print(merged_data[["AverageTemperature", "Value"]].isnull().sum())

# Store Merged Data in SQLite
print("Storing Merged Data to SQLite Database...")
conn = sqlite3.connect(db_path)
merged_data.to_sql("MergedData", conn, if_exists="replace", index=False)
conn.close()
print("Data pipeline completed. Data stored in project_data.db.")
