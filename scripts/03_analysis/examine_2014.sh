#!/bin/bash
# examine_2014.sh

FILE_PATH="/Users/laurentstaub4/documents/00_open_medic/data/processed/csv_data/OPEN_MEDIC_2014.csv"

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
