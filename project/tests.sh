#!/bin/bash

# Enable debugging
set -x

# Initialize a flag to track test success
all_tests_passed=true

# Function to check if a file exists
Files() {
  if [ -f "$1" ]; then
    echo "Test passed: $1 exists."
  else
    echo "Test failed: $1 does not exist."
    all_tests_passed=false
  fi
}

# Execute your data pipelines
echo "Running dataset1 pipeline..."
jv ./dataset1.jv -d
if [ $? -ne 0 ]; then
    echo "Error: dataset1 pipeline failed."
    all_tests_passed=false
fi

echo "Running dataset2 pipeline..."
jv ./dataset2.jv -d
if [ $? -ne 0 ]; then
    echo "Error: dataset2 pipeline failed."
    all_tests_passed=false
fi

# files to check
DATA_DIR="../data"
Pipeline1="$DATA_DIR/dataset1.sqlite"
Pipeline2="$DATA_DIR/dataset2.sqlite"

# function execution for each file
Files "$Pipeline1"
Files "$Pipeline2"

# Final status
if [ "$all_tests_passed" = true ]; then
  echo "All tests passed."
  exit 0
else
  echo "Some tests failed."
  exit 1
