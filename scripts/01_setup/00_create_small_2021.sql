DROP TABLE IF EXISTS medicine_sales CASCADE;

CREATE TABLE medicine_sales (
    id SERIAL,
    ATC5 TEXT,
    CIP13 TEXT,                           -- 13-digit drug presentation code
    l_cip13 TEXT,                                -- Drug presentation name
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