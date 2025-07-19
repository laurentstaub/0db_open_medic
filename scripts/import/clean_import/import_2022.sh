#!/bin/bash
# import_2022.sh

DB_NAME="open_medic_clean"
DB_USER="laurentstaub4"
DB_HOST="localhost"
DB_PORT="5432"
FILE_PATH="/Users/laurentstaub4/documents/00_open_medic/data/OPEN_MEDIC_2022.csv"
YEAR="2022"

# Process the file with pandas
echo "Processing CSV file..."
python3 process_csv_pandas.py "$FILE_PATH" "clean_2022.csv" "$YEAR"

# Create a temporary staging table
echo "Creating staging table..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
DROP TABLE IF EXISTS temp_medicine_2022;
CREATE TABLE temp_medicine_2022 (
    ATC5 TEXT,
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
    year INT
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
    ATC5, CIP13, l_cip13, TOP_GEN, GEN_NUM, AGE, sexe, BEN_REG, PSP_SPE, BOITES, REM, BSE, year
)
SELECT
    ATC5,
    CIP13,
    l_cip13,
    TOP_GEN,
    GEN_NUM,
    age,
    sexe,
    BEN_REG,
    PSP_SPE,
    BOITES,
    REM,
    BSE,
    year
FROM temp_medicine_2022;
"

# Report results
echo "Data imported. Verifying count:"
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT COUNT(*) FROM medicine_sales WHERE year = $YEAR;"

# Clean up
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "DROP TABLE temp_medicine_2022;"
rm clean_2022.csv

echo "Import of 2022 data completed successfully!"
