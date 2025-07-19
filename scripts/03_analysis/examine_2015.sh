#!/bin/bash
# examine_2015.sh

FILE_PATH="/Users/laurentstaub4/documents/00_open_medic/data/processed/csv_data/OPEN_MEDIC_2015.csv"

echo "File size:"
du -h "$FILE_PATH"

echo -e "\nHeader line:"
head -n 1 "$FILE_PATH"

echo -e "\nFirst data row:"
head -n 2 "$FILE_PATH" | tail -n 1

echo -e "\nCount of columns in header:"
head -n 1 "$FILE_PATH" | tr ';' '\n' | wc -l

echo -e "\nCount of columns in first data row:"
head -n 2 "$FILE_PATH" | tail -n 1 | tr ';' '\n' | wc -l

echo -e "\nSample of first 3 rows:"
head -n 4 "$FILE_PATH"

# Check if any values in the TOP_GEN, AGE, or sexe columns are non-numeric
echo -e "\nChecking for non-numeric values in potentially problematic columns:"
head -n 1000 "$FILE_PATH" | cut -d';' -f7,9,10 | grep -v "^[0-9]\+;[0-9]\+;[0-9]\+$" | head -n 5

# Check if any decimal numbers use commas instead of periods
echo -e "\nChecking for French number format (comma as decimal separator):"
head -n 1000 "$FILE_PATH" | cut -d';' -f14,15 | grep "," | head -n 5
