#!/bin/bash
# import_2014_robust.sh

DB_NAME="open_medic"
DB_USER="laurentstaub4"
DB_HOST="localhost"
DB_PORT="5432"
FILE_PATH="/Users/laurentstaub4/documents/open_medic/csv_data/OPEN_MEDIC_2014.csv"
YEAR=2014

# First, let's update the main table schema to handle text values
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
-- Modify problematic columns to TEXT
ALTER TABLE medicine_sales 
    ALTER COLUMN TOP_GEN TYPE TEXT,
    ALTER COLUMN AGE TYPE TEXT,
    ALTER COLUMN sexe TYPE TEXT,
    ALTER COLUMN BEN_REG TYPE TEXT,
    ALTER COLUMN PSP_SPE TYPE TEXT;

-- Add numeric versions for analysis
ALTER TABLE medicine_sales 
    ADD COLUMN IF NOT EXISTS top_gen_numeric SMALLINT,
    ADD COLUMN IF NOT EXISTS age_numeric SMALLINT,
    ADD COLUMN IF NOT EXISTS sexe_numeric SMALLINT,
    ADD COLUMN IF NOT EXISTS ben_reg_numeric SMALLINT,
    ADD COLUMN IF NOT EXISTS psp_spe_numeric SMALLINT;
"

# Clean CSV file
echo "Preprocessing CSV file..."
python3 clean_csv.py "$FILE_PATH" "clean_2014.csv" "$YEAR"

# Create a temporary staging table
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DROP TABLE IF EXISTS temp_medicine_2014;
CREATE TABLE temp_medicine_2014 (
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
COPY temp_medicine_2014 FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER true);" <clean_2014.csv

# Insert into partitioned table with proper handling
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
FROM temp_medicine_2014;

-- Update numeric columns where possible
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
echo "Sample data (including any non-numeric values):"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT ATC1, ATC5, CIP13, 
       AGE, age_numeric, 
       sexe, sexe_numeric, 
       TOP_GEN, top_gen_numeric,
       BOITES, 
       REM, rem_numeric, 
       BSE, bse_numeric 
FROM medicine_sales WHERE year = $YEAR LIMIT 10;
"

# Clean up
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "DROP TABLE temp_medicine_2014;"
rm clean_2014.csv

echo "Import completed successfully!"
