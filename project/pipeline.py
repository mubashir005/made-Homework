import pandas as pd
import sqlite3
import os
import requests

# data directory
base_dir = os.path.dirname(os.path.abspath(__file__))  
data_dir = os.path.join(base_dir, "../data")
climate_csv_path = os.path.join(data_dir, "GlobalLandTemperaturesByCity.csv")
agri_csv_path = os.path.join(data_dir, "FAOSTAT_data_en_11-13-2024.csv")
db_path = os.path.join(data_dir, "project_data.db")

# check data directory exists
os.makedirs(data_dir, exist_ok=True)

# Download Climate Data if not already downloaded
if not os.path.exists(climate_csv_path):
    print("Downloading Climate Data...")
    climate_url = "https://query.data.world/s/bxhv23ezqupfoqjoi67cl55i732zbz?dws=00000"
    response = requests.get(climate_url)
    with open(climate_csv_path, "wb") as file:
        file.write(response.content)

# Download Agricultural Data if not already downloaded
if not os.path.exists(agri_csv_path):
    print("Downloading Agricultural Data...")
    agri_url = "https://sourceforge.net/projects/faostat-data-en-11-13-2024-csv/files/FAOSTAT_data_en_11-13-2024.csv/download"
    response = requests.get(agri_url, allow_redirects=True)
    with open(agri_csv_path, "wb") as file:
        file.write(response.content)


# Load Datasets
print("Loading Climate Data...")
climate_data = pd.read_csv(climate_csv_path)

# Load Agricultural Data from uploaded file
print("Loading Agricultural Data...")
agri_data = pd.read_csv(agri_csv_path)

# Data Cleaning and Transformation
print("Processing Climate Data...")
climate_data = climate_data[["dt", "AverageTemperature", "City", "Country"]]
climate_data.dropna(inplace=True)
climate_data["dt"] = pd.to_datetime(climate_data["dt"])

# Filter Climate Data for North and South America
print("Filtering Climate Data for North and South America...")
american_countries = [
    "United States of America", "Canada", "Mexico", "Brazil", "Argentina", "Colombia", "Chile", "Peru",
    "Venezuela", "Bolivia", "Paraguay", "Uruguay", "Ecuador", "Guyana", "Suriname", "Belize",
    "Costa Rica", "Cuba", "Dominican Republic", "El Salvador", "Guatemala", "Honduras",
    "Nicaragua", "Panama", "Trinidad and Tobago", "Jamaica", "Bahamas", "Barbados"
]
climate_data = climate_data[climate_data["Country"].isin(american_countries)]

# Filter Agricultural Data
print("Processing Agricultural Data...")
agri_data = agri_data[["Year", "Area", "Item", "Value", "Unit"]]

# Filter for North and South America
print("Filtering Agricultural Data for North and South America...")
agri_data = agri_data[agri_data["Area"].isin(american_countries)]
agri_data.dropna(inplace=True)

# Storing Data in SQLite
print("Storing Data to SQLite Database...")
conn = sqlite3.connect(db_path)
climate_data.to_sql("Climate", conn, if_exists="replace", index=False)
agri_data.to_sql("Agriculture", conn, if_exists="replace", index=False)
conn.close()
print("Data pipeline completed. Data stored in project_data.db.")
