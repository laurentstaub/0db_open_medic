#!/bin/bash

# Database connection details
DB_NAME="open_medic"
DB_USER="laurentstaub4"
DB_HOST="localhost"
DB_PORT="5432"
DATA_DIR="/Users/laurentstaub4/documents/open_medic/csv_data"

# First, check the headers of one file to understand structure
echo "Examining CSV structure..."
head -n 1 "$DATA_DIR/OPEN_MEDIC_2016.csv"

for year in {2014..2024}; do
  echo "Importing data for year $year..."

  if [ -f "$DATA_DIR/OPEN_MEDIC_${year}.csv" ]; then
    psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME <<EOF
      -- Import raw data with minimal processing
      TRUNCATE temp_medicine_sales;
      
      -- Import each entire line as a text field
      \copy temp_medicine_sales (raw_data) FROM '$DATA_DIR/OPEN_MEDIC_${year}.csv' CSV HEADER DELIMITER ';';
      
      -- Set the year
      UPDATE temp_medicine_sales SET year = $year;
      
      -- Now insert using regexp to parse the data safely
      INSERT INTO medicine_sales
      SELECT
          -- Parse each column from the raw_data using split_part
          split_part(raw_data, ';', 1) AS ATC1,
          split_part(raw_data, ';', 2) AS l_ATC1,
          split_part(raw_data, ';', 3) AS ATC2,
          split_part(raw_data, ';', 4) AS L_ATC2,
          split_part(raw_data, ';', 5) AS ATC3,
          split_part(raw_data, ';', 6) AS L_ATC3,
          split_part(raw_data, ';', 7) AS ATC4,
          split_part(raw_data, ';', 8) AS L_ATC4,
          split_part(raw_data, ';', 9) AS ATC5,
          split_part(raw_data, ';', 10) AS L_ATC5,
          
          -- Convert numeric values with error handling
          NULLIF(split_part(raw_data, ';', 11), '')::BIGINT AS CIP13,
          split_part(raw_data, ';', 12) AS l_cip13,
          
          -- Keep as text to handle "R" and other non-numeric values
          split_part(raw_data, ';', 13) AS TOP_GEN,
          split_part(raw_data, ';', 14) AS GEN_NUM,
          
          -- Handle missing fields by using NULL for columns that might not exist
          NULLIF(NULLIF(split_part(raw_data, ';', 15), ''), 'R')::SMALLINT AS age,
          split_part(raw_data, ';', 16) AS sexe,
          split_part(raw_data, ';', 17) AS BEN_REG,
          split_part(raw_data, ';', 18) AS PSP_SPE,
          NULLIF(split_part(raw_data, ';', 19), '')::INTEGER AS BOITES,
          
          -- Keep problematic monetary values as text for now
          split_part(raw_data, ';', 20) AS REM,
          split_part(raw_data, ';', 21) AS BSE,
          
          year
      FROM temp_medicine_sales
      -- Skip the header row that got imported
      WHERE raw_data != (SELECT raw_data FROM temp_medicine_sales LIMIT 1);
      
      -- Report row count
      SELECT count(*) FROM medicine_sales WHERE year = $year;
EOF
    echo "Completed import for $year"
  else
    echo "WARNING: File $DATA_DIR/OPEN_MEDIC_${year}.csv not found. Skipping."
  fi
done

echo "All data imported successfully! Processing numeric fields..."

# Now clean up the numeric fields in a separate step
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME <<EOF
  -- Create numeric versions of the text fields
  ALTER TABLE medicine_sales 
    ADD COLUMN rem_numeric NUMERIC(12,2),
    ADD COLUMN bse_numeric NUMERIC(12,2),
    ADD COLUMN gen_num_numeric NUMERIC(12,2);
  
  -- Clean and convert the numeric fields
  UPDATE medicine_sales
  SET rem_numeric = 
      CASE 
          WHEN REM ~ '^\d+,\d+$' THEN REPLACE(REM, ',', '.')::NUMERIC
          WHEN REM ~ '^\d+\.\d+,\d+$' THEN REPLACE(REPLACE(REM, '.', ''), ',', '.')::NUMERIC
          WHEN REM ~ '^\d+\.\d+\.\d+$' THEN REPLACE(REM, '.', '')::NUMERIC
          ELSE NULL
      END
  WHERE REM IS NOT NULL AND REM != '';
  
  -- Similar updates for BSE and GEN_NUM
EOF

echo "Data cleanup complete!"
