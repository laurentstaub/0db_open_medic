/*
 * Complete Numeric Column Audit Script
 *
 * Goals:
 * 1. Check if existing numeric columns are 100% accurate
 * 2. Compare text vs numeric values for discrepancies
 * 3. Decide cleanup strategy: keep numeric OR convert text and remove text
 * 4. Implement the optimal solution
 *
 * Database: Your original Open Medic database
 */

-- =====================================================
-- STEP 1: IDENTIFY ALL NUMERIC AND TEXT COLUMN PAIRS
-- =====================================================
-- First, let's see what columns we actually have
SELECT
    column_name,
    data_type,
    is_nullable,
    CASE
        WHEN column_name LIKE '%_numeric' THEN 'NUMERIC_VERSION'
        WHEN column_name IN ('boites', 'rem', 'bse', 'gen_num') THEN 'TEXT_VERSION'
        ELSE 'OTHER'
    END as column_category
FROM information_schema.columns
WHERE table_name = 'medicine_sales'
ORDER BY column_name;

-- =====================================================
-- STEP 2: ANALYZE COMPLETENESS OF KEY COLUMNS
-- =====================================================
SELECT
    'Key Columns Completeness Analysis' as analysis_type,
    COUNT(*) as total_rows,

    -- BOITES - already integer, should be 100%
    COUNT(boites) as boites,
    ROUND(COUNT(boites) * 100.0 / COUNT(*), 2) as boites_coverage,

    -- BSE columns
    COUNT(bse) as bse_text,
    COUNT(bse_numeric) as bse_numeric,
    ROUND(COUNT(bse_numeric) * 100.0 / COUNT(bse), 2) as bse_coverage,

  -- GEN_NUM columns
    COUNT(gen_num) as gen_num_text,
    COUNT(gen_num_numeric) as gen_num_numeric,
    ROUND(COUNT(gen_num_numeric) * 100.0 / COUNT(gen_num), 2) as gen_num_coverage,

    -- REM columns
    COUNT(rem) as rem_text,
    COUNT(rem_numeric) as rem_numeric,
    ROUND(COUNT(rem_numeric) * 100.0 / COUNT(rem), 2) as rem_coverage,

    -- BEN_REG columns
    COUNT(ben_reg) as ben_reg_text,
    COUNT(ben_reg_numeric) as ben_reg_numeric,
    ROUND(COUNT(ben_reg_numeric) * 100.0 / COUNT(ben_reg), 2) as ben_reg_coverage,

    -- PSP_REG columns
    COUNT(psp_spe) as psp_spe_text,
    COUNT(psp_spe_numeric) as psp_spe_numeric,
    ROUND(COUNT(psp_spe_numeric) * 100.0 / COUNT(psp_spe), 2) as psp_spe_coverage,

    -- SEXE columns
    COUNT(sexe) as sexe_text,
    COUNT(sexe_numeric) as sexe_numeric,
    ROUND(COUNT(sexe_numeric) * 100.0 / COUNT(sexe), 2) as sexe_coverage,

    -- TOP_GEN columns
    COUNT(top_gen) as top_gen_text,
    COUNT(top_gen_numeric) as top_gen_numeric,
    ROUND(COUNT(top_gen_numeric) * 100.0 / COUNT(top_gen), 2) as top_gen_coverage

FROM medicine_sales;

-- Check if there are any mismatches (should be 0)
SELECT
    'Checking for any mismatches in 100% columns' as check_type,
    COUNT(CASE WHEN gen_num IS NOT NULL AND gen_num_numeric IS NULL THEN 1 END) as gen_num_failures,
    COUNT(CASE WHEN ben_reg IS NOT NULL AND ben_reg_numeric IS NULL THEN 1 END) as ben_reg_failures,
    COUNT(CASE WHEN psp_spe IS NOT NULL AND psp_spe_numeric IS NULL THEN 1 END) as psp_spe_failures,
    COUNT(CASE WHEN sexe IS NOT NULL AND sexe_numeric IS NULL THEN 1 END) as sexe_failures
FROM medicine_sales;

-- EXECUTE CLEANUP
BEGIN;

-- Drop the text columns that have 100% numeric success
ALTER TABLE medicine_sales DROP COLUMN gen_num;
ALTER TABLE medicine_sales DROP COLUMN ben_reg;
ALTER TABLE medicine_sales DROP COLUMN psp_spe;
ALTER TABLE medicine_sales DROP COLUMN sexe;

-- Rename numeric columns to clean names
ALTER TABLE medicine_sales RENAME COLUMN gen_num_numeric TO gen_num;
ALTER TABLE medicine_sales RENAME COLUMN ben_reg_numeric TO ben_reg;
ALTER TABLE medicine_sales RENAME COLUMN psp_spe_numeric TO psp_spe;
ALTER TABLE medicine_sales RENAME COLUMN sexe_numeric TO sexe;

COMMIT;

SELECT
    'Key Columns Completeness Analysis' as analysis_type,
    COUNT(*) as total_rows,

    -- BSE columns
    COUNT(age) as bse_text,
    COUNT(age_numeric) as bse_numeric,
    ROUND(COUNT(age_numeric) * 100.0 / COUNT(age), 2) as age_coverage

FROM medicine_sales;

-- EXECUTE CLEANUP
BEGIN;

-- Drop the text columns that have 100% numeric success
ALTER TABLE medicine_sales DROP COLUMN age;
-- Rename numeric columns to clean names
ALTER TABLE medicine_sales RENAME COLUMN age_numeric TO age;

COMMIT;