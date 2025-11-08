-- ============================================================================
-- Biscuit Index Comprehensive Benchmark Script
-- Compares: Sequential Scan, B-Tree, pg_trgm, and Biscuit indexes
-- Dataset: 10,000 UUID records
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Starting Biscuit Index Benchmark'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Clean up any existing objects
DROP TABLE IF EXISTS benchmark_data CASCADE;
DROP EXTENSION IF EXISTS pg_trgm CASCADE;
DROP EXTENSION IF EXISTS pg_biscuit CASCADE;

-- Create extensions
DO $$ BEGIN RAISE NOTICE 'Creating extensions...'; END $$;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_biscuit;

-- Create benchmark table
DO $$ BEGIN RAISE NOTICE 'Creating benchmark table...'; END $$;
CREATE TABLE benchmark_data (
    id SERIAL PRIMARY KEY,
    uuid_str TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Populate with 10,000 UUID records
DO $$ BEGIN RAISE NOTICE 'Populating 10,000 UUID records...'; END $$;
INSERT INTO benchmark_data (uuid_str)
SELECT gen_random_uuid()::TEXT
FROM generate_series(1, 1000000);

-- Analyze table
ANALYZE benchmark_data;

DO $$ 
DECLARE
    row_count INTEGER;
BEGIN 
    SELECT COUNT(*) INTO row_count FROM benchmark_data;
    RAISE NOTICE 'Total records inserted: %', row_count;
END $$;

-- Create indexes
DO $$ BEGIN RAISE NOTICE '----------------------------------------'; END $$;
DO $$ BEGIN RAISE NOTICE 'Creating indexes...'; END $$;

-- B-Tree index
DO $$ BEGIN RAISE NOTICE 'Creating B-Tree index...'; END $$;
CREATE INDEX idx_btree ON benchmark_data(uuid_str);

-- pg_trgm GIN index
DO $$ BEGIN RAISE NOTICE 'Creating pg_trgm GIN index...'; END $$;
CREATE INDEX idx_trgm ON benchmark_data USING GIN(uuid_str gin_trgm_ops);

-- Biscuit index
DO $$ BEGIN RAISE NOTICE 'Creating Biscuit index...'; END $$;
SET client_min_messages = WARNING;
CREATE INDEX idx_biscuit ON benchmark_data USING biscuit(uuid_str);
SET client_min_messages = NOTICE;

DO $$ BEGIN RAISE NOTICE 'All indexes created successfully'; END $$;
DO $$ BEGIN RAISE NOTICE '----------------------------------------'; END $$;

-- Vacuum and analyze
VACUUM ANALYZE benchmark_data;

-- ============================================================================
-- Test Cases
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'BENCHMARK TEST CASES'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Create table to store results
DROP TABLE IF EXISTS benchmark_results;
CREATE TABLE benchmark_results (
    test_id SERIAL PRIMARY KEY,
    test_name TEXT,
    pattern TEXT,
    index_type TEXT,
    result_count INTEGER,
    execution_time_ms NUMERIC(10,3),
    planning_time_ms NUMERIC(10,3)
);

-- ============================================================================
-- Test 1: Exact match (should favor B-Tree)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 1: Exact Match'; END $$;

DO $$
DECLARE
    test_pattern TEXT;
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
BEGIN
    -- Get a sample UUID
    SELECT uuid_str INTO test_pattern FROM benchmark_data LIMIT 1;
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Exact Match', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- B-Tree
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str = test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Exact Match', test_pattern, 'B-Tree (=)', rec.count, exec_time);
    RAISE NOTICE 'B-Tree (=): % matches, %.3f ms', rec.count, exec_time;
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Exact Match', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit (force index scan)
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Exact Match', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
END $$;

-- ============================================================================
-- Test 2: Prefix match (UUID starts with specific characters)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 2: Prefix Match (e.g., "abc%")'; END $$;

DO $$
DECLARE
    test_pattern TEXT := 'a%';
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
    expected_count INTEGER;
BEGIN
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Get expected count
    SELECT COUNT(*) INTO expected_count FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Prefix Match', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- B-Tree (can use for prefix)
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Prefix Match', test_pattern, 'B-Tree', rec.count, exec_time);
    RAISE NOTICE 'B-Tree: % matches, %.3f ms', rec.count, exec_time;
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Prefix Match', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Prefix Match', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
    
    -- Verify accuracy
    IF rec.count != expected_count THEN
        RAISE WARNING 'Accuracy check failed! Expected %, got %', expected_count, rec.count;
    END IF;
END $$;

-- ============================================================================
-- Test 3: Suffix match (UUID ends with specific characters)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 3: Suffix Match (e.g., "%xyz")'; END $$;

DO $$
DECLARE
    test_pattern TEXT := '%a';
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
    expected_count INTEGER;
BEGIN
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Get expected count
    SELECT COUNT(*) INTO expected_count FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Suffix Match', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- B-Tree (cannot use for suffix)
    RAISE NOTICE 'B-Tree: Skipped (cannot optimize suffix matches)';
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Suffix Match', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Suffix Match', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
    
    -- Verify accuracy
    IF rec.count != expected_count THEN
        RAISE WARNING 'Accuracy check failed! Expected %, got %', expected_count, rec.count;
    END IF;
END $$;

-- ============================================================================
-- Test 4: Contains match (substring anywhere)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 4: Contains Match (e.g., "%abc%")'; END $$;

DO $$
DECLARE
    test_pattern TEXT := '%4a%';
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
    expected_count INTEGER;
BEGIN
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Get expected count
    SELECT COUNT(*) INTO expected_count FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Contains Match', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- B-Tree (cannot use)
    RAISE NOTICE 'B-Tree: Skipped (cannot optimize contains matches)';
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Contains Match', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Contains Match', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
    
    -- Verify accuracy
    IF rec.count != expected_count THEN
        RAISE WARNING 'Accuracy check failed! Expected %, got %', expected_count, rec.count;
    END IF;
END $$;

-- ============================================================================
-- Test 5: Complex pattern with multiple wildcards
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 5: Complex Pattern (e.g., "%a%b%c%")'; END $$;

DO $$
DECLARE
    test_pattern TEXT := '%a%4%b%';
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
    expected_count INTEGER;
BEGIN
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Get expected count
    SELECT COUNT(*) INTO expected_count FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Complex Pattern', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- B-Tree (cannot use)
    RAISE NOTICE 'B-Tree: Skipped (cannot optimize complex patterns)';
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Complex Pattern', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Complex Pattern', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
    
    -- Verify accuracy
    IF rec.count != expected_count THEN
        RAISE WARNING 'Accuracy check failed! Expected %, got %', expected_count, rec.count;
    END IF;
END $$;

-- ============================================================================
-- Test 6: Very selective pattern (few matches)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 6: Very Selective Pattern (rare substring)'; END $$;

DO $$
DECLARE
    test_pattern TEXT := '%zzz%';
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
    expected_count INTEGER;
BEGIN
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Get expected count
    SELECT COUNT(*) INTO expected_count FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Very Selective', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Very Selective', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Very Selective', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
    
    -- Verify accuracy
    IF rec.count != expected_count THEN
        RAISE WARNING 'Accuracy check failed! Expected %, got %', expected_count, rec.count;
    END IF;
END $$;

-- ============================================================================
-- Test 7: Broad match (many results)
-- ============================================================================
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Test 7: Broad Match (common character)'; END $$;

DO $$
DECLARE
    test_pattern TEXT := '%a%';
    rec RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    exec_time NUMERIC;
    expected_count INTEGER;
BEGIN
    RAISE NOTICE 'Pattern: %', test_pattern;
    
    -- Get expected count
    SELECT COUNT(*) INTO expected_count FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    
    -- Sequential Scan
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Broad Match', test_pattern, 'Sequential Scan', rec.count, exec_time);
    RAISE NOTICE 'Sequential Scan: % matches, %.3f ms', rec.count, exec_time;
    
    -- pg_trgm
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = ON;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Broad Match', test_pattern, 'pg_trgm', rec.count, exec_time);
    RAISE NOTICE 'pg_trgm: % matches, %.3f ms', rec.count, exec_time;
    
    -- Biscuit
    SET enable_indexscan = ON;
    SET enable_bitmapscan = OFF;
    SET enable_seqscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO rec FROM benchmark_data WHERE uuid_str LIKE test_pattern;
    end_time := clock_timestamp();
    exec_time := EXTRACT(EPOCH FROM (end_time - start_time)) * 1000;
    
    INSERT INTO benchmark_results (test_name, pattern, index_type, result_count, execution_time_ms)
    VALUES ('Broad Match', test_pattern, 'Biscuit', rec.count, exec_time);
    RAISE NOTICE 'Biscuit: % matches, %.3f ms', rec.count, exec_time;
    
    -- Verify accuracy
    IF rec.count != expected_count THEN
        RAISE WARNING 'Accuracy check failed! Expected %, got %', expected_count, rec.count;
    END IF;
END $$;

-- ============================================================================
-- Results Summary
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'BENCHMARK RESULTS SUMMARY'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Display all results
SELECT 
    test_name,
    pattern,
    index_type,
    result_count,
    ROUND(execution_time_ms::numeric, 3) as exec_time_ms
FROM benchmark_results
ORDER BY test_name, execution_time_ms;

-- Best performer per test
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Best Performer Per Test:'; END $$;

SELECT 
    test_name,
    pattern,
    index_type as winner,
    result_count,
    ROUND(execution_time_ms::numeric, 3) as exec_time_ms
FROM benchmark_results br1
WHERE execution_time_ms = (
    SELECT MIN(execution_time_ms) 
    FROM benchmark_results br2 
    WHERE br2.test_name = br1.test_name
)
ORDER BY test_name;

-- Overall statistics
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Overall Statistics:'; END $$;

SELECT 
    index_type,
    COUNT(*) as tests_run,
    ROUND(AVG(execution_time_ms)::numeric, 3) as avg_time_ms,
    ROUND(MIN(execution_time_ms)::numeric, 3) as min_time_ms,
    ROUND(MAX(execution_time_ms)::numeric, 3) as max_time_ms,
    COUNT(CASE WHEN rk = 1 THEN 1 END) as times_fastest
FROM (
    SELECT 
        *,
        RANK() OVER (PARTITION BY test_name ORDER BY execution_time_ms) as rk
    FROM benchmark_results
) ranked
GROUP BY index_type
ORDER BY avg_time_ms;

-- Reset settings
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;
SET enable_seqscan = ON;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Benchmark Complete!'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;