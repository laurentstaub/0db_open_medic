#!/bin/bash
# import_2015.sh

DB_NAME="open_medic"
DB_USER="laurentstaub4"
DB_HOST="localhost"
DB_PORT="5432"
FILE_PATH="/Users/laurentstaub4/documents/open_medic/csv_data/OPEN_MEDIC_2015.csv"
YEAR=2015

# First, check if our Python script exists
if [ ! -f "clean_csv.py" ]; then
  cat >clean_csv.py <<'EOF'
import csv
import sys

input_file = sys.argv[1]
output_file = sys.argv[2]
year = sys.argv[3]

# Use Python's built-in CSV module with strict handling
with open(input_file, 'r', encoding='latin1') as infile, \
     open(output_file, 'w', encoding='utf8') as outfile:
    
    # Read header
    header = infile.readline().strip()
    outfile.write(header + ";year\n")
    
    # Process data with minimal processing
    for line in infile:
        fields = line.strip().split(';')
        if len(fields) == 15:  # Expected columns for 2015
            outfile.write(line.strip() + f";{year}\n")
EOF
fi

# Clean CSV file
echo "Preprocessing CSV file..."
python3 clean_csv.py "$FILE_PATH" "clean_2015.csv" "$YEAR"

# Create a temporary staging table
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DROP TABLE IF EXISTS temp_medicine_2015;
CREATE TABLE temp_medicine_2015 (
    ATC1 TEXT,
    ATC2 TEXT,
    ATC3 TEXT,
    ATC4 TEXT,
    ATC5 TEXT,
    CIP13 TEXT,
    TOP_GEN TEXT,
    GEN_NUM TEXT,
    AGE TEXT,
    sexe TEXT,
    BEN_REG TEXT,
    PSP_SPE TEXT,
    BOITES TEXT,
    REM TEXT,
    BSE TEXT,
    year TEXT
);
"

# Import to temporary table
echo "Importing file to staging table..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "\
COPY temp_medicine_2015 FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true);" <clean_2015.csv

# Create partition for 2015 if needed
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
CREATE TABLE IF NOT EXISTS medicine_sales_2015 PARTITION OF medicine_sales
    FOR VALUES FROM (2015) TO (2016);
"

# Insert into partitioned table
echo "Inserting into partitioned table..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
INSERT INTO medicine_sales (
    ATC1, ATC2, ATC3, ATC4, ATC5, 
    CIP13, TOP_GEN, GEN_NUM, AGE, sexe, 
    BEN_REG, PSP_SPE, BOITES, REM, BSE, year
)
SELECT 
    ATC1, ATC2, ATC3, ATC4, ATC5,
    CASE WHEN CIP13 ~ '^[0-9]+$' THEN CIP13::BIGINT ELSE NULL END,
    TOP_GEN,
    GEN_NUM,
    AGE, 
    sexe,
    BEN_REG, 
    PSP_SPE, 
    CASE WHEN BOITES ~ '^[0-9]+$' THEN BOITES::INTEGER ELSE NULL END,
    REM, BSE, year::INTEGER
FROM temp_medicine_2015
WHERE ATC1 IS NOT NULL;  -- Skip header row if it got imported as data

-- Update numeric columns
UPDATE medicine_sales 
SET 
    rem_numeric = CASE WHEN REM ~ '^[0-9,]+$' THEN REPLACE(REM, ',', '.')::NUMERIC ELSE NULL END,
    bse_numeric = CASE WHEN BSE ~ '^[0-9,]+$' THEN REPLACE(BSE, ',', '.')::NUMERIC ELSE NULL END,
    gen_num_numeric = CASE WHEN GEN_NUM ~ '^[0-9,]+$' THEN REPLACE(GEN_NUM, ',', '.')::NUMERIC ELSE NULL END,
    top_gen_numeric = CASE WHEN TOP_GEN ~ '^[0-9]+$' THEN TOP_GEN::SMALLINT ELSE NULL END,
    age_numeric = CASE WHEN AGE ~ '^[0-9]+$' THEN AGE::SMALLINT ELSE NULL END,
    sexe_numeric = CASE WHEN sexe ~ '^[0-9]+$' THEN sexe::SMALLINT ELSE NULL END,
    ben_reg_numeric = CASE WHEN BEN_REG ~ '^[0-9]+$' THEN BEN_REG::SMALLINT ELSE NULL END,
    psp_spe_numeric = CASE WHEN PSP_SPE ~ '^[0-9]+$' THEN PSP_SPE::SMALLINT ELSE NULL END
WHERE year = $YEAR;
"

# Report results
echo "Data imported. Verifying count:"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT COUNT(*) FROM medicine_sales WHERE year = $YEAR;"

# Show sample data
echo "Sample data from 2015:"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT ATC1, ATC5, CIP13, 
       AGE, age_numeric, 
       sexe, sexe_numeric, 
       BOITES, 
       REM, rem_numeric, 
       BSE, bse_numeric 
FROM medicine_sales WHERE year = $YEAR LIMIT 5;
"

# Clean up
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "DROP TABLE temp_medicine_2015;"
rm clean_2015.csv

echo "Import of 2015 data completed successfully!"
