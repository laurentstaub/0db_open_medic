-- CREATE medicine_sales TABLE IN TEXT FORMAT
-- (EXCEPT FOR id, year and imported_at)
DROP TABLE IF EXISTS medicine_sales CASCADE;
CREATE TABLE medicine_sales (
    id SERIAL,
    ATC5 TEXT,
    CIP13 TEXT,                           -- 13-digit drug presentation code
    l_cip13 TEXT,                       -- Drug presentation name
    TOP_GEN TEXT,                            -- Generic top indicator
    GEN_NUM TEXT,                       -- Generic number
    age TEXT,                                -- Age (numeric)
    sexe TEXT,                               -- Sex (1=M, 2=F)
    BEN_REG TEXT,                            -- Beneficiary region code
    PSP_SPE TEXT,                            -- Prescriber specialty code
    BOITES TEXT,                              -- Number of boxes (cleaned)
    REM TEXT,                          -- Reimbursement amount (French format converted)
    BSE TEXT,
    year INT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (year);

-- Create partitions for each year (2021-2024 + future years)
CREATE TABLE medicine_sales_2021 PARTITION OF medicine_sales FOR VALUES FROM (2021) TO (2022);
CREATE TABLE medicine_sales_2022 PARTITION OF medicine_sales FOR VALUES FROM (2022) TO (2023);
CREATE TABLE medicine_sales_2023 PARTITION OF medicine_sales FOR VALUES FROM (2023) TO (2024);
CREATE TABLE medicine_sales_2024 PARTITION OF medicine_sales FOR VALUES FROM (2024) TO (2025);
CREATE TABLE medicine_sales_2025 PARTITION OF medicine_sales FOR VALUES FROM (2025) TO (2026);

-- Create indexes for optimal performance
CREATE INDEX idx_medicine_sales_cip13 ON medicine_sales(CIP13);
CREATE INDEX idx_medicine_sales_year ON medicine_sales(year);
CREATE INDEX idx_medicine_sales_atc5 ON medicine_sales(ATC5);
CREATE INDEX idx_medicine_sales_boites ON medicine_sales(BOITES);
CREATE INDEX idx_medicine_sales_rem ON medicine_sales(REM);


-- CREATE sales TABLE WITH NUMERIC FORMAT
DROP TABLE IF EXISTS sales CASCADE;
CREATE TABLE sales (
    id SERIAL,
    ATC5 VARCHAR(7),
    CIP13 VARCHAR(13),                           -- 13-digit drug presentation code
    l_cip13 TEXT,                                -- Drug presentation name
    TOP_GEN VARCHAR(1),                            -- Generic top indicator
    GEN_NUM SMALLINT,                       -- Generic number
    age SMALLINT,                                -- Age (numeric)
    sexe SMALLINT,                               -- Sex (1=M, 2=F)
    BEN_REG SMALLINT,                            -- Beneficiary region code
    PSP_SPE SMALLINT,                            -- Prescriber specialty code
    BOITES INT,                              -- Number of boxes (cleaned)
    REM NUMERIC(12,2),                          -- Reimbursement amount (French format converted)
    BSE NUMERIC(12,2),
    year INT,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (year);

-- Create partitions for each year (2021-2024 + future years)
CREATE TABLE sales_2021 PARTITION OF sales FOR VALUES FROM (2021) TO (2022);
CREATE TABLE sales_2022 PARTITION OF sales FOR VALUES FROM (2022) TO (2023);
CREATE TABLE sales_2023 PARTITION OF sales FOR VALUES FROM (2023) TO (2024);
CREATE TABLE sales_2024 PARTITION OF sales FOR VALUES FROM (2024) TO (2025);
CREATE TABLE sales_2025 PARTITION OF sales FOR VALUES FROM (2025) TO (2026);

-- Create indexes for optimal performance
CREATE INDEX idx_sales_cip13 ON sales(CIP13);
CREATE INDEX idx_sales_year ON sales(year);
CREATE INDEX idx_sales_atc5 ON sales(ATC5);
CREATE INDEX idx_sales_boites ON sales(BOITES);
CREATE INDEX idx_sales_rem ON sales(REM);


-- TRANSFER DATA FROM medicine_sales TO sales
-- Create a reusable function for French number conversion
CREATE OR REPLACE FUNCTION convert_french_decimal(input_text TEXT)
RETURNS NUMERIC AS $$
BEGIN
    IF input_text IS NULL OR TRIM(input_text) = '' THEN
        RETURN NULL;
    END IF;

    RETURN REPLACE(REPLACE(TRIM(input_text), '.', ''), ',', '.')::NUMERIC;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- INSERT data to small table (cleaner version)
INSERT INTO sales (ATC5, CIP13, l_cip13, TOP_GEN, GEN_NUM, age, sexe, BEN_REG, PSP_SPE, BOITES, REM, BSE, year)
SELECT
    LEFT(ATC5, 7),
    LEFT(CIP13, 13),
    l_cip13,
    LEFT(TOP_GEN, 1),
    NULLIF(TRIM(GEN_NUM), '')::SMALLINT,
    NULLIF(TRIM(age), '')::SMALLINT,
    NULLIF(TRIM(sexe), '')::SMALLINT,
    NULLIF(TRIM(BEN_REG), '')::SMALLINT,
    NULLIF(TRIM(PSP_SPE), '')::SMALLINT,
    NULLIF(TRIM(BOITES), '')::INT,
    convert_french_decimal(REM),
    convert_french_decimal(BSE),
    year
FROM medicine_sales;


-- CREATE A SUMMARY TABLE NO AGE NO SEX
CREATE TABLE sales_no_sex_age AS
SELECT  ATC5, CIP13, l_cip13, TOP_GEN, GEN_NUM, BEN_REG, PSP_SPE, year,
    -- Numeric columns to sum
    SUM(BOITES) as boites,
    SUM(REM) as rem,
    SUM(BSE) as bse
FROM sales
GROUP BY ATC5, CIP13, l_cip13, TOP_GEN, GEN_NUM, BEN_REG, PSP_SPE, year;

CREATE INDEX idx_sales_summary_atc5 ON sales_no_sex_age (ATC5);
CREATE INDEX idx_sales_summary_cip13 ON sales_no_sex_age (CIP13);
CREATE INDEX idx_sales_summary_region ON sales_no_sex_age (BEN_REG);
CREATE INDEX idx_sales_summary_year ON sales_no_sex_age (year);
CREATE INDEX idx_sales_summary_combo ON sales_no_sex_age (ATC5, BEN_REG, year);


-- Check the results
SELECT
    SUM(boites) as original_boites,
    SUM(REM) as original_rem
FROM sales;

SELECT
    SUM(boites) as total_boxes_all,
    SUM(rem) as total_reimbursement_all
FROM sales_no_sex_age;

-- Sample the summarized data
SELECT * FROM sales_no_sex_age
ORDER BY rem DESC
LIMIT 10;