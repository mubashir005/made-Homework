#!/bin/bash

# Set up the data directory
DATA_DIR="../data"
DB_PATH="$DATA_DIR/project_data.db"
CLIMATE_CSV="$DATA_DIR/GlobalLandTTemperaturesByCity.csv"
AGRI_CSV="$DATA_DIR/FAOSTAT_data_en_11-28-2024.csv"

# Test 1: Run the data pipeline
echo "Running the data pipeline..."
python3 pipeline.py
echo "Pipeline executed successfully..."
# Test 2: Check if the database file was created
echo "Test 2: Check if the database file was created"
if [ -f "$DB_PATH" ]; then
    echo "Test Passed: Database file exists."
else
    echo "Test Failed: Database file not created."
    exit 1
fi

# Test 3: Check if climate data CSV was downloaded
if [ -f "$CLIMATE_CSV" ]; then
    echo "Test Passed: Climate data CSV exists."
else
    echo "Test Failed: Climate data CSV not found."
    exit 1
fi

# Test 4: Check if agricultural data CSV was downloaded
if [ -f "$AGRI_CSV" ]; then
    echo "Test Passed: Agricultural data CSV exists."
else
    echo "Test Failed: Agricultural data CSV not found."
    exit 1
fi

# Test 5: Validate SQLite database content
echo "Validating database content..."
sqlite3 $DB_PATH "SELECT COUNT(*) FROM MergedData;" | grep -q '[1-9][0-9]*'
if [ $? -eq 0 ]; then
    echo "Test Passed: Database contains data."
else
    echo "Test Failed: Database is empty."
    exit 1
fi

echo "All tests passed successfully!"
exit 0
