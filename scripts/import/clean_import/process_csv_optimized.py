#!/usr/bin/env python3
"""
process_csv_optimized.py - Optimized CSV processor for Open Medic data
Handles French number format conversion and data type cleaning for direct import
"""
import pandas as pd
import sys
import os

# Check arguments
if len(sys.argv) != 4:
    print("Usage: python3 process_csv_optimized.py <input_file> <output_file> <year>")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]
year = int(sys.argv[3])
chunk_size = 100000  # Process in chunks

# Column data types based on the schema in import_2021.sh
column_types = {
    'ATC5': str,
    'L_ATC5': str,
    'CIP13': str,
    'l_cip13': str,
    'TOP_GEN': 'Int64',  # Nullable integer
    'GEN_NUM': float,
    'age': 'Int64',      # Nullable integer
    'sexe': 'Int64',     # Nullable integer
    'BEN_REG': 'Int64',  # Nullable integer
    'PSP_SPE': 'Int64',  # Nullable integer
    'BOITES': 'Int64',   # Nullable integer
    'REM': float,        # French monetary format
    'BSE': float         # French monetary format
}

# Create an empty output file
with open(output_file, 'w', encoding='utf-8') as f:
    f.write('')  # Initialize empty file

print(f"Processing {input_file}...")
print(f"Converting French number format and cleaning data types...")

# Process the file in chunks
chunk_num = 0
for chunk in pd.read_csv(input_file, sep=';', chunksize=chunk_size, 
                         encoding='latin1', low_memory=False,
                         quotechar='"', escapechar='\\', 
                         on_bad_lines='warn',
                         decimal=','):  # Handle French decimal format (comma)

    # Add year column
    chunk['year'] = year

    # Convert data types according to the schema
    for col in chunk.columns:
        if col in column_types:
            # Handle numeric columns with French format
            if column_types[col] == float:
                # Convert string numbers with commas to float
                if chunk[col].dtype == 'object':
                    chunk[col] = pd.to_numeric(chunk[col].astype(str).str.replace(',', '.'), errors='coerce')
                else:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

            # Handle integer columns
            elif column_types[col] == 'Int64':
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('Int64')

            # Handle string columns
            else:
                chunk[col] = chunk[col].astype(str)
                # Clean text fields (remove newlines, etc.)
                chunk[col] = chunk[col].str.replace('\n', ' ').str.replace('\r', ' ')

    # Ensure CIP13 is properly formatted (13 digits)
    if 'CIP13' in chunk.columns:
        # Remove non-numeric characters and ensure 13 digits
        chunk['CIP13'] = chunk['CIP13'].astype(str).str.replace(r'\D', '', regex=True)
        # Pad with zeros if needed (but only if not empty)
        chunk['CIP13'] = chunk['CIP13'].apply(
            lambda x: x.zfill(13) if x and len(x) <= 13 else x
        )
        # Replace empty strings with a default value to avoid NOT NULL constraint
        chunk['CIP13'] = chunk['CIP13'].replace('', '0000000000000')

    # Select only the columns needed for the database table
    columns_to_keep = ['ATC5', 'L_ATC5', 'CIP13', 'l_cip13', 'TOP_GEN', 'GEN_NUM', 
                      'age', 'sexe', 'BEN_REG', 'PSP_SPE', 'BOITES', 'REM', 'BSE', 'year']

    # Filter columns that exist in the dataframe
    existing_columns = [col for col in columns_to_keep if col in chunk.columns]

    # Replace empty strings and NaN with default values in numeric columns
    numeric_columns = ['TOP_GEN', 'GEN_NUM', 'age', 'BEN_REG', 'PSP_SPE', 'REM', 'BSE']
    for col in numeric_columns:
        if col in chunk.columns:
            # Replace empty strings with 0
            chunk[col] = chunk[col].replace('', 0)
            # Replace NaN with 0
            chunk[col] = chunk[col].fillna(0)

    # Special handling for BOITES field - must be a positive integer
    if 'BOITES' in chunk.columns:
        # Replace empty strings with default value 1
        chunk['BOITES'] = chunk['BOITES'].replace('', 1)
        # Replace NaN with default value 1
        chunk['BOITES'] = chunk['BOITES'].fillna(1)
        # Replace zero or negative values with default value 1
        chunk['BOITES'] = chunk['BOITES'].apply(lambda x: 1 if x <= 0 else x)

    # Special handling for sexe field - must be 1 or 2 according to database constraint
    if 'sexe' in chunk.columns:
        # Replace empty strings with default value 1
        chunk['sexe'] = chunk['sexe'].replace('', 1)
        # Replace NaN with default value 1
        chunk['sexe'] = chunk['sexe'].fillna(1)
        # Replace invalid values (not 1 or 2) with default value 1
        chunk['sexe'] = chunk['sexe'].apply(lambda x: x if x in [1, 2] else 1)

    # Write to CSV (append mode after first chunk)
    mode = 'a' if chunk_num > 0 else 'w'
    chunk[existing_columns].to_csv(output_file, sep=';', index=False, mode=mode, 
                header=(chunk_num==0), encoding='utf-8',
                quoting=1)  # QUOTE_ALL

    chunk_num += 1
    print(f"Processed chunk {chunk_num} ({chunk_size * chunk_num} rows)")

print(f"Processing complete. Output written to {output_file}")
print(f"Data is ready for direct import with correct French number format handling.")
