CREATE TABLE medicine_sales (
    -- ATC classification codes and descriptions
    ATC1 CHAR(1),
    l_ATC1 VARCHAR(41),
    ATC2 VARCHAR(13),
    L_ATC2 VARCHAR(38),
    ATC3 VARCHAR(14),
    L_ATC3 VARCHAR(38),
    ATC4 VARCHAR(15),
    L_ATC4 VARCHAR(51),
    ATC5 VARCHAR(17),
    L_ATC5 VARCHAR(74),
    
    -- Medicine identifiers and names
    CIP13 BIGINT,
    l_cip13 VARCHAR(48),
    
    -- Generic medicine info
    TOP_GEN SMALLINT,  -- Binary flag (0 or 1)
    GEN_NUM SMALLINT,  -- Generic group code
    
    -- Patient demographics
    age SMALLINT,      -- Patient age (0-99)
    sexe SMALLINT,     -- Patient gender
    BEN_REG SMALLINT,  -- Patient region code
    
    -- Prescriber info
    PSP_SPE SMALLINT,  -- Prescriber specialty code
    
    -- Quantity and financial data
    BOITES INTEGER,    -- Number of boxes/units
    REM NUMERIC(12,2), -- Reimbursement amount (converting from string)
    BSE NUMERIC(12,2), -- Base amount (converting from string)
    
    -- Add year column for partitioning
    year SMALLINT
) PARTITION BY RANGE (year);

-- Create a partition for each year
DO $$
BEGIN
    FOR y IN 2014..2024 LOOP
        EXECUTE format('
            CREATE TABLE medicine_sales_%s PARTITION OF medicine_sales
            FOR VALUES FROM (%s) TO (%s)', 
            y, y, y+1
        );
    END LOOP;
END $$;

-- Create the function in the database
CREATE OR REPLACE FUNCTION import_medicine_data(file_path text, year_val int)
RETURNS void AS $$
BEGIN
    EXECUTE FORMAT('
        COPY medicine_sales(
            ATC1, l_ATC1, ATC2, L_ATC2, ATC3, L_ATC3, ATC4, L_ATC4, 
            ATC5, L_ATC5, CIP13, l_cip13, TOP_GEN, GEN_NUM, 
            age, sexe, BEN_REG, PSP_SPE, BOITES, REM, BSE
        ) 
        FROM ''%s''
        WITH (FORMAT csv, DELIMITER '';'', HEADER true, ENCODING ''Latin1'');
    ', file_path);
    
    -- Update the year column
    EXECUTE FORMAT('
        UPDATE medicine_sales 
        SET year = %s 
        WHERE year IS NULL;
    ', year_val);
    
    -- Convert string columns to numeric
    EXECUTE '
        UPDATE medicine_sales 
        SET 
            REM = REPLACE(REPLACE(REM, '' '', ''''), '','', ''.'')::NUMERIC,
            BSE = REPLACE(REPLACE(BSE, '' '', ''''), '','', ''.'')::NUMERIC
        WHERE year = ' || year_val;
END;
$$ LANGUAGE plpgsql;
