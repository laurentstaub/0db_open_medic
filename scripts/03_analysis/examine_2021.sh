#!/bin/bash
# examine_2021.sh

FILE_PATH="/Users/laurentstaub4/documents/00_open_medic/data/processed/csv_data/OPEN_MEDIC_2021.csv"

echo "File size:"
du -h "$FILE_PATH"

echo -e "\nHeader line:"
head -n 1 "$FILE_PATH"

echo -e "\nFirst data row:"
head -n 2 "$FILE_PATH" | tail -n 1

echo -e "\nCount of columns in header:"
head -n 1 "$FILE_PATH" | tr ';' '\n' | wc -l

echo -e "\nSample of first 3 rows:"
head -n 4 "$FILE_PATH"

echo -e "\nChecking maximum text column lengths (first 1000 rows):"
python3 -c '
import csv
import sys

with open("'"$FILE_PATH"'", "r", encoding="latin1") as f:
    reader = csv.reader(f, delimiter=";")
    header = next(reader)
    
    # Initialize max lengths dictionary
    max_lengths = {col: 0 for col in header}
    
    # Check first 1000 rows
    for i, row in enumerate(reader):
        if i >= 1000:
            break
            
        for col_idx, value in enumerate(row):
            if col_idx < len(header):  # Avoid index errors
                col_name = header[col_idx]
                max_lengths[col_name] = max(max_lengths[col_name], len(value))
    
    # Print results for text columns
    text_columns = ["l_ATC1", "L_ATC2", "L_ATC3", "L_ATC4", "L_ATC5", "l_cip13"]
    for col in text_columns:
        if col in max_lengths:
            print(f"{col}: {max_lengths[col]}")
'
