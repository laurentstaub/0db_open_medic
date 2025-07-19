#!/bin/bash
# import_2022.sh

DB_NAME="open_medic"
DB_USER="laurentstaub4"
DB_HOST="localhost"
DB_PORT="5432"
FILE_PATH="/Users/laurentstaub4/documents/open_medic/csv_data/OPEN_MEDIC_2022.csv"
YEAR=2022

# Process the file with pandas
echo "Processing CSV file..."
python3 process_csv_pandas.py "$FILE_PATH" "clean_2022.csv" "$YEAR"

# Create a temporary staging table
echo "Creating staging table..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DROP TABLE IF EXISTS temp_medicine_2022;
CREATE TABLE temp_medicine_2022 (
    ATC1 TEXT,
    l_ATC1 TEXT,
    ATC2 TEXT,
    L_ATC2 TEXT,
    ATC3 TEXT,
    L_ATC3 TEXT,
    ATC4 TEXT,
    L_ATC4 TEXT,
    ATC5 TEXT,
    L_ATC5 TEXT,
    CIP13 TEXT,
    l_cip13 TEXT,
    TOP_GEN TEXT,
    GEN_NUM TEXT,
    age TEXT,
    sexe TEXT,
    BEN_REG TEXT,
    PSP_SPE TEXT,
    BOITES TEXT,
    REM TEXT,
    BSE TEXT,
    year TEXT
);
"

# Create the partition
echo "Creating partition..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
CREATE TABLE IF NOT EXISTS medicine_sales_2022 PARTITION OF medicine_sales
    FOR VALUES FROM (2022) TO (2023);
"

# Import to temporary table with \copy
echo "Importing with \\copy..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME <<EOF
\copy temp_medicine_2022 FROM 'clean_2022.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true, ENCODING 'UTF8');
EOF

# Insert into the main table
echo "Inserting into main table..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
INSERT INTO medicine_sales (
    ATC1, l_ATC1, ATC2, L_ATC2, ATC3, L_ATC3, ATC4, L_ATC4, ATC5, L_ATC5,
    CIP13, l_cip13, TOP_GEN, GEN_NUM, AGE, sexe, BEN_REG, PSP_SPE, BOITES, REM, BSE, year
)
SELECT 
    ATC1, l_ATC1, ATC2, L_ATC2, ATC3, L_ATC3, ATC4, L_ATC4, ATC5, L_ATC5,
    CASE WHEN CIP13 ~ '^[0-9]+$' THEN CIP13::BIGINT ELSE NULL END,
    l_cip13,
    TOP_GEN,
    GEN_NUM,
    age, 
    sexe,
    BEN_REG, 
    PSP_SPE, 
    CASE WHEN BOITES ~ '^[0-9]+$' THEN BOITES::INTEGER ELSE NULL END,
    REM, BSE, year::INTEGER
FROM temp_medicine_2022
WHERE ATC1 IS NOT NULL;
"

# Update numeric columns
echo "Updating numeric columns..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
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

# Sample data
echo "Sample data from 2022:"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT ATC1, SUBSTRING(l_ATC1, 1, 30) as l_ATC1, ATC5, 
       SUBSTRING(L_ATC5, 1, 30) as L_ATC5, 
       CIP13, SUBSTRING(l_cip13, 1, 30) as l_cip13,
       AGE, sexe, BOITES, REM, rem_numeric
FROM medicine_sales WHERE year = $YEAR LIMIT 5;
"

# Clean up
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "DROP TABLE temp_medicine_2022;"
rm clean_2022.csv

echo "Import of 2022 data completed successfully!"
