import pandas as pd
import numpy as np
import os
import chardet

def analyze_csv_data_types(file_path, sample_size=10000):
    # First, detect the file encoding
    with open(file_path, 'rb') as raw_file:
        # Read a sample to detect encoding
        sample = raw_file.read(min(10000, os.path.getsize(file_path)))
        detected = chardet.detect(sample)
        encoding = detected['encoding']
        print(f"Detected file encoding: {encoding} (confidence: {detected['confidence']:.2f})")
    
    # Read CSV with the detected encoding
    try:
        df = pd.read_csv(file_path, sep=';', nrows=sample_size, encoding=encoding)
    except Exception as e:
        print(f"Error with detected encoding, trying Latin-1: {e}")
        df = pd.read_csv(file_path, sep=';', nrows=sample_size, encoding='latin1')
    
    # Count total lines without loading the whole file
    with open(file_path, 'rb') as f:
        line_count = sum(1 for _ in f)
    
    print(f"File size: {os.path.getsize(file_path) / (1024 * 1024):.2f} MB")
    print(f"Analyzing {min(sample_size, len(df))} rows out of {line_count - 1} total")
    print("\nCOLUMN ANALYSIS:\n" + "-"*80)
    
    for column in df.columns:
        # Count null values
        null_count = df[column].isna().sum()
        null_percent = null_count / len(df) * 100
        
        # Analyze unique values
        unique_count = df[column].nunique()
        unique_percent = unique_count / len(df) * 100
        
        # Determine if numeric
        numeric = pd.api.types.is_numeric_dtype(df[column])
        
        # Analyze column values
        if numeric:
            min_val = df[column].min()
            max_val = df[column].max()
            avg_val = df[column].mean()
            print(f"{column}:")
            print(f"  Type: Numeric")
            print(f"  Range: {min_val} to {max_val}")
            print(f"  Average: {avg_val:.2f}")
            
            # Recommend PostgreSQL type
            if df[column].dropna().apply(lambda x: x.is_integer() if not pd.isna(x) else True).all():
                # Integer checks
                if min_val >= -32768 and max_val <= 32767:
                    pg_type = "SMALLINT"
                elif min_val >= -2147483648 and max_val <= 2147483647:
                    pg_type = "INTEGER"
                else:
                    pg_type = "BIGINT"
            else:
                # Float checks
                pg_type = "NUMERIC(12,2)"
        else:
            # String 03_analysis
            max_len = df[column].astype(str).str.len().max()
            print(f"{column}:")
            print(f"  Type: String")
            print(f"  Max Length: {max_len}")
            
            # Recommend PostgreSQL type
            if max_len <= 1:
                pg_type = "CHAR(1)"
            elif max_len <= 50:
                pg_type = f"VARCHAR({max_len + 10})"
            else:
                pg_type = f"VARCHAR({max_len + 20})"
                
            # Special case for age column if it exists but is string type
            if column.lower() == 'age':
                # Check if it's numeric without using str method that caused the error
                if df[column].dropna().apply(lambda x: str(x).isdigit()).all():
                    pg_type = "INTEGER"
        
        print(f"  Null Values: {null_count} ({null_percent:.2f}%)")
        print(f"  Unique Values: {unique_count} ({unique_percent:.2f}%)")
        print(f"  Recommended PostgreSQL Type: {pg_type}")
        print("-"*80)
    
    # Generate PostgreSQL CREATE TABLE statement
    print("\nSUGGESTED POSTGRESQL TABLE DEFINITION:")
    print("CREATE TABLE medicine_sales (")
    for i, column in enumerate(df.columns):
        comma = "," if i < len(df.columns) - 1 else ""
        
        if pd.api.types.is_numeric_dtype(df[column]):
            if column.lower() == 'age' or (df[column].dropna().apply(lambda x: x.is_integer() if not pd.isna(x) else True).all()):
                if df[column].min() >= -32768 and df[column].max() <= 32767:
                    print(f"    {column} SMALLINT{comma}")
                elif df[column].min() >= -2147483648 and df[column].max() <= 2147483647:
                    print(f"    {column} INTEGER{comma}")
                else:
                    print(f"    {column} BIGINT{comma}")
            else:
                print(f"    {column} NUMERIC(12,2){comma}")
        else:
            max_len = df[column].astype(str).str.len().max()
            if max_len <= 1:
                print(f"    {column} CHAR(1){comma}")
            elif max_len <= 50:
                print(f"    {column} VARCHAR({max_len + 10}){comma}")
            else:
                print(f"    {column} VARCHAR({max_len + 20}){comma}")
    print(");")

# Replace with your file path
analyze_csv_data_types('OPEN_MEDIC_2024.CSV')
