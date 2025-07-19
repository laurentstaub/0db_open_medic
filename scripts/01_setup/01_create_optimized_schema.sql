/*
 * Core Open Medic Database Optimization
 *
 * Step 1: Run this in your ORIGINAL Open Medic database
 * Creates optimized schema and exports data for incidents_json
 *
 * Database: Your original Open Medic database
 * Target: Create lean data for BDPM 03_analysis
 */

-- =====================================================
-- STEP 1: ANALYZE CURRENT DATA
-- =====================================================
-- Quick check of what we're working with
SELECT
    'Current database size' as metric,
    pg_size_pretty(pg_database_size(current_database())) as value
UNION ALL
SELECT
    'medicine_sales table size',
    pg_size_pretty(pg_total_relation_size('medicine_sales'))
UNION ALL
SELECT
    'Total rows',
    COUNT(*)::text
FROM medicine_sales
UNION ALL
SELECT
    'Rows 2021+',
    COUNT(*)::text
FROM medicine_sales WHERE year >= 2021
UNION ALL
SELECT
    'Unique CIP13 codes',
    COUNT(DISTINCT cip13)::text
FROM medicine_sales WHERE year >= 2021;

-- =====================================================
-- STEP 2: CREATE OPTIMIZATION SCHEMA
-- =====================================================
CREATE SCHEMA IF NOT EXISTS open_medic_opt;

-- =====================================================
-- STEP 3: CREATE OPTIMIZED CONSUMPTION TABLE
-- =====================================================
-- This is the core optimization - aggregate all demographics
CREATE TABLE open_medic_opt.consumption AS
SELECT
    cip13,
    year,
    -- Core metrics
    SUM(COALESCE(boites, 0)) as total_boites,
    SUM(COALESCE(rem_numeric, 0)) as total_remboursement,
    SUM(COALESCE(base_remboursement, 0)) as total_base_remboursement,

    -- Useful metadata
    COUNT(*) as nb_lignes_origine,
    AVG(COALESCE(montant_rembourse, 0)) as montant_moyen_par_ligne,

    -- Geographic diversity
    COUNT(DISTINCT COALESCE(region, 'Unknown')) as nb_regions,

    -- Demographic diversity
    COUNT(DISTINCT COALESCE(sexe, 'Unknown')) as nb_sexes,
    COUNT(DISTINCT COALESCE(age_range, 'Unknown')) as nb_tranches_age

FROM medicine_sales
WHERE annee >= 2021  -- Only recent years
  AND cip13 IS NOT NULL
  AND cip13 != ''
  AND nombre_boites > 0  -- Only actual consumption
GROUP BY cip13, annee;