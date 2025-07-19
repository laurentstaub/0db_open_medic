#!/bin/bash
# create_clean_database.sh
# Creates optimized open_medic_clean database with proper structure

DB_NAME="open_medic_clean"
DB_USER="laurentstaub4"
DB_HOST="localhost"
DB_PORT="5432"

echo "🗄️ Creating clean Open Medic database..."

# Create the database
echo "📋 Creating database: $DB_NAME"
createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME

if [ $? -ne 0 ]; then
    echo "❌ Failed to create database. It might already exist."
    echo "Do you want to drop and recreate it? (y/N)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        dropdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
        createdb -h $DB_HOST -p $DB_PORT -U $DB_USER $DB_NAME
        echo "✅ Database recreated"
    else
        echo "Using existing database"
    fi
fi

# Create the database structure
echo "🏗️ Creating optimal table structure..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME << 'EOF'

-- Create main medicine_sales table with optimal structure
CREATE TABLE medicine_sales (
    -- Unique identifier
    id SERIAL,                                   -- Auto-incrementing ID

    -- ATC Classification (only level 5 - chemical substance)
    ATC5 VARCHAR(7),                             -- Level 5: Chemical substance
    L_ATC5 TEXT,                                 -- Level 5: Description

    -- Drug identification
    CIP13 VARCHAR(13),                           -- 13-digit drug presentation code
    l_cip13 TEXT,                                -- Drug presentation name

    -- Drug properties (converted to proper numeric types)
    TOP_GEN SMALLINT,                            -- Generic top indicator
    GEN_NUM NUMERIC(10,2),                       -- Generic number

    -- Demographics (converted to proper numeric types)
    age SMALLINT,                                -- Age (numeric)
    sexe SMALLINT,                               -- Sex (1=M, 2=F)
    BEN_REG SMALLINT,                            -- Beneficiary region code
    PSP_SPE SMALLINT,                            -- Prescriber specialty code

    -- Consumption data (converted to proper numeric types)
    BOITES INTEGER,                              -- Number of boxes (cleaned)
    REM NUMERIC(12,2),                          -- Reimbursement amount (French format converted)
    BSE NUMERIC(12,2),                          -- Base reimbursement (French format converted)

    -- Metadata
    year INTEGER,                                -- Year (numeric)
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Import tracking

    -- Constraints
    -- No primary key constraint as there can be multiple sales for the same product/region
    CONSTRAINT chk_year CHECK (year >= 2014 AND year <= 2060)
    -- Removed other constraints to simplify import process

) PARTITION BY RANGE (year);

-- Create partitions for each year (2021-2024 + future years)
CREATE TABLE medicine_sales_2021 PARTITION OF medicine_sales
    FOR VALUES FROM (2021) TO (2022);

CREATE TABLE medicine_sales_2022 PARTITION OF medicine_sales
    FOR VALUES FROM (2022) TO (2023);

CREATE TABLE medicine_sales_2023 PARTITION OF medicine_sales
    FOR VALUES FROM (2023) TO (2024);

CREATE TABLE medicine_sales_2024 PARTITION OF medicine_sales
    FOR VALUES FROM (2024) TO (2025);

-- Create partition for future data
CREATE TABLE medicine_sales_2025 PARTITION OF medicine_sales
    FOR VALUES FROM (2025) TO (2026);

-- Create indexes for optimal performance
CREATE INDEX idx_medicine_sales_cip13 ON medicine_sales(CIP13);
CREATE INDEX idx_medicine_sales_year ON medicine_sales(year);
CREATE INDEX idx_medicine_sales_atc5 ON medicine_sales(ATC5);
CREATE INDEX idx_medicine_sales_boites ON medicine_sales(BOITES);
CREATE INDEX idx_medicine_sales_rem ON medicine_sales(REM);

-- Composite indexes for common queries
CREATE INDEX idx_medicine_sales_cip13_year ON medicine_sales(CIP13, year);
CREATE INDEX idx_medicine_sales_atc5_year ON medicine_sales(ATC5, year);
CREATE INDEX idx_medicine_sales_year_boites ON medicine_sales(year, BOITES DESC);

-- Create optimization schema for BDPM analysis
CREATE SCHEMA open_medic_opt;

-- Create aggregated consumption table (ready for BDPM integration)
CREATE TABLE open_medic_opt.consumption (
    id SERIAL PRIMARY KEY,
    cip13 VARCHAR(13),
    annee INTEGER,
    total_boites BIGINT DEFAULT 0,
    total_remboursement NUMERIC(15,2) DEFAULT 0,
    total_base_remboursement NUMERIC(15,2) DEFAULT 0,
    nb_lignes_origine INTEGER DEFAULT 0,
    nb_ages_differents SMALLINT DEFAULT 0,
    nb_sexes_differents SMALLINT DEFAULT 0,
    nb_regions_differentes SMALLINT DEFAULT 0,
    prix_moyen_par_boite NUMERIC(8,4) GENERATED ALWAYS AS
        (CASE WHEN total_boites > 0 THEN total_remboursement / total_boites ELSE NULL END) STORED,
    taux_remboursement NUMERIC(6,4) GENERATED ALWAYS AS
        (CASE WHEN total_base_remboursement > 0 THEN total_remboursement / total_base_remboursement ELSE NULL END) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_consumption_year CHECK (annee >= 2021)
    -- Removed other constraints to simplify import process
);

-- Indexes for optimization schema
CREATE INDEX idx_opt_consumption_cip13 ON open_medic_opt.consumption(cip13);
CREATE INDEX idx_opt_consumption_annee ON open_medic_opt.consumption(annee);
CREATE INDEX idx_opt_consumption_boites ON open_medic_opt.consumption(total_boites DESC);
CREATE INDEX idx_opt_consumption_prix ON open_medic_opt.consumption(prix_moyen_par_boite);

-- Create function to rebuild optimized data
CREATE OR REPLACE FUNCTION open_medic_opt.rebuild_consumption_data()
RETURNS void AS $$
BEGIN
    -- Clear existing data
    DELETE FROM open_medic_opt.consumption;

    -- Rebuild from main table
    INSERT INTO open_medic_opt.consumption (
        cip13, annee, total_boites, total_remboursement, total_base_remboursement,
        nb_lignes_origine, nb_ages_differents, nb_sexes_differents, nb_regions_differentes
    )
    SELECT
        CIP13,
        year,
        SUM(BOITES) as total_boites,
        SUM(REM) as total_remboursement,
        SUM(BSE) as total_base_remboursement,
        COUNT(*) as nb_lignes_origine,
        COUNT(DISTINCT age) as nb_ages_differents,
        COUNT(DISTINCT sexe) as nb_sexes_differents,
        COUNT(DISTINCT BEN_REG) as nb_regions_differentes
    FROM medicine_sales
    WHERE year >= 2021
    GROUP BY CIP13, year
    HAVING SUM(BOITES) > 0;

    -- Update statistics
    ANALYZE open_medic_opt.consumption;

    RAISE NOTICE 'Consumption data rebuilt: % records', (SELECT COUNT(*) FROM open_medic_opt.consumption);
END;
$$ LANGUAGE plpgsql;

-- Create function to add new year data
CREATE OR REPLACE FUNCTION open_medic_opt.add_year_to_consumption(target_year INTEGER)
RETURNS void AS $$
BEGIN
    -- Remove existing data for this year
    DELETE FROM open_medic_opt.consumption WHERE annee = target_year;

    -- Add new year data
    INSERT INTO open_medic_opt.consumption (
        cip13, annee, total_boites, total_remboursement, total_base_remboursement,
        nb_lignes_origine, nb_ages_differents, nb_sexes_differents, nb_regions_differentes
    )
    SELECT
        CIP13,
        year,
        SUM(BOITES) as total_boites,
        SUM(REM) as total_remboursement,
        SUM(BSE) as total_base_remboursement,
        COUNT(*) as nb_lignes_origine,
        COUNT(DISTINCT age) as nb_ages_differents,
        COUNT(DISTINCT sexe) as nb_sexes_differents,
        COUNT(DISTINCT BEN_REG) as nb_regions_differentes
    FROM medicine_sales
    WHERE year = target_year
    GROUP BY CIP13, year
    HAVING SUM(BOITES) > 0;

    ANALYZE open_medic_opt.consumption;

    RAISE NOTICE 'Added year % to consumption data: % records',
                 target_year,
                 (SELECT COUNT(*) FROM open_medic_opt.consumption WHERE annee = target_year);
END;
$$ LANGUAGE plpgsql;

-- Create export function for BDPM integration
CREATE OR REPLACE FUNCTION open_medic_opt.export_for_incidents_json()
RETURNS void AS $$
DECLARE
    export_file TEXT;
    record_count INTEGER;
BEGIN
    export_file := '/tmp/open_medic_for_bdpm_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';

    EXECUTE format('
        COPY (
            SELECT
                cip13,
                annee,
                total_boites,
                total_remboursement,
                total_base_remboursement,
                nb_lignes_origine,
                prix_moyen_par_boite,
                taux_remboursement
            FROM open_medic_opt.consumption
            WHERE total_boites >= 1
            ORDER BY total_boites DESC
        ) TO %L WITH CSV HEADER
    ', export_file);

    SELECT COUNT(*) INTO record_count
    FROM open_medic_opt.consumption
    WHERE total_boites >= 1;

    RAISE NOTICE 'Exported % records to %', record_count, export_file;
    RAISE NOTICE 'Import this file to incidents_json open_medic schema';
END;
$$ LANGUAGE plpgsql;

-- Create monitoring views
CREATE VIEW open_medic_opt.v_data_summary AS
SELECT
    year as annee,
    COUNT(*) as nb_lignes_raw,
    COUNT(DISTINCT CIP13) as nb_cip13_uniques,
    SUM(BOITES) as total_boites,
    SUM(REM) as total_remboursement,
    ROUND(AVG(REM / NULLIF(BOITES, 0)), 2) as prix_moyen_par_boite,
    pg_size_pretty(pg_total_relation_size('medicine_sales_' || year)) as taille_partition
FROM medicine_sales
GROUP BY year
ORDER BY year;

-- Create data quality view
CREATE VIEW open_medic_opt.v_data_quality AS
SELECT
    year as annee,
    COUNT(*) as total_lignes,
    COUNT(CASE WHEN CIP13 IS NOT NULL THEN 1 END) as cip13_non_null,
    COUNT(CASE WHEN BOITES IS NOT NULL THEN 1 END) as boites_non_null,
    COUNT(CASE WHEN REM IS NOT NULL THEN 1 END) as rem_non_null,
    COUNT(CASE WHEN BSE IS NOT NULL THEN 1 END) as bse_non_null,

    -- Quality percentages
    ROUND(COUNT(CASE WHEN CIP13 IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as cip13_quality_pct,
    ROUND(COUNT(CASE WHEN BOITES IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as boites_quality_pct,
    ROUND(COUNT(CASE WHEN REM IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as rem_quality_pct,
    ROUND(COUNT(CASE WHEN BSE IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as bse_quality_pct
FROM medicine_sales
GROUP BY year
ORDER BY year;

EOF

echo "✅ Database structure created successfully!"

# Verify the structure
echo "🔍 Verifying database structure..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT
    'Table Structure Verification' as status,
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname IN ('public', 'open_medic_opt')
ORDER BY schemaname, tablename;
"

echo ""
echo "📊 Checking column structure..."
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "
SELECT
    column_name,
    data_type,
    is_nullable,
    CASE
        WHEN data_type IN ('integer', 'smallint', 'numeric', 'bigint') THEN '🔢 NUMERIC'
        WHEN data_type IN ('character varying', 'text') THEN '📝 TEXT'
        ELSE '❓ OTHER'
    END as type_category
FROM information_schema.columns
WHERE table_name = 'medicine_sales'
  AND column_name IN ('CIP13', 'BOITES', 'REM', 'BSE', 'age', 'sexe', 'year', 'ATC5')
ORDER BY ordinal_position;
"

echo ""
echo "🎉 Clean database 'open_medic_clean' is ready!"
echo ""