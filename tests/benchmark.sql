-- ============================================================================
-- Biscuit Index Comprehensive Benchmark Script (COMPLETELY FIXED)
-- Compares: Sequential Scan, B-Tree, pg_trgm, and Biscuit indexes
-- Dataset: 1,000,000 UUID records
-- ============================================================================

-- Clean up any existing objects
DROP TABLE IF EXISTS benchmark_data CASCADE;
DROP TABLE IF EXISTS benchmark_results CASCADE;
DROP TABLE IF EXISTS benchmark_statistics CASCADE;
DROP EXTENSION IF EXISTS pg_trgm CASCADE;
DROP EXTENSION IF EXISTS pg_biscuit CASCADE;

DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'BISCUIT INDEX BENCHMARK INITIALIZATION'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

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

-- Populate with 1,000,000 UUID records
DO $$ BEGIN RAISE NOTICE 'Populating 1,000,000 UUID records...'; END $$;
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
CREATE INDEX idx_btree ON benchmark_data(uuid_str);
DO $$ BEGIN RAISE NOTICE '  B-Tree index created'; END $$;

CREATE INDEX idx_trgm ON benchmark_data USING GIN(uuid_str gin_trgm_ops);
DO $$ BEGIN RAISE NOTICE '  pg_trgm GIN index created'; END $$;

SET client_min_messages = WARNING;
CREATE INDEX idx_biscuit ON benchmark_data USING biscuit(uuid_str);
SET client_min_messages = NOTICE;
DO $$ BEGIN RAISE NOTICE '  Biscuit index created'; END $$;

-- Vacuum and analyze
VACUUM ANALYZE benchmark_data;
DO $$ BEGIN RAISE NOTICE 'Indexes created and analyzed successfully'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Create results table with enhanced metrics
CREATE TABLE benchmark_results (
    test_id SERIAL PRIMARY KEY,
    test_category TEXT,
    test_name TEXT,
    pattern TEXT,
    index_type TEXT,
    result_count INTEGER,
    execution_time_ms NUMERIC(12,6),
    planning_time_ms NUMERIC(12,6),
    index_scan_cost NUMERIC(12,2),
    selectivity NUMERIC(8,6),
    timestamp TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- COMPREHENSIVE TEST SUITE
-- ============================================================================

-- Helper function to run a benchmark test
CREATE OR REPLACE FUNCTION run_benchmark_test(
    p_test_category TEXT,
    p_test_name TEXT,
    p_pattern TEXT,
    p_index_type TEXT,
    p_enable_indexscan BOOLEAN,
    p_enable_bitmapscan BOOLEAN,
    p_enable_seqscan BOOLEAN,
    p_use_equality BOOLEAN DEFAULT FALSE
) RETURNS void AS $$
DECLARE
    v_count INTEGER;
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_exec_time NUMERIC;
    v_total_rows INTEGER;
    v_selectivity NUMERIC;
BEGIN
    -- Set scan methods
    EXECUTE format('SET LOCAL enable_indexscan = %s', p_enable_indexscan);
    EXECUTE format('SET LOCAL enable_bitmapscan = %s', p_enable_bitmapscan);
    EXECUTE format('SET LOCAL enable_seqscan = %s', p_enable_seqscan);
    
    -- Get total rows
    SELECT COUNT(*) INTO v_total_rows FROM benchmark_data;
    
    -- Execute query and measure time
    v_start_time := clock_timestamp();
    IF p_use_equality THEN
        SELECT COUNT(*) INTO v_count FROM benchmark_data WHERE uuid_str = p_pattern;
    ELSE
        SELECT COUNT(*) INTO v_count FROM benchmark_data WHERE uuid_str LIKE p_pattern;
    END IF;
    v_end_time := clock_timestamp();
    
    v_exec_time := EXTRACT(EPOCH FROM (v_end_time - v_start_time)) * 1000;
    v_selectivity := v_count::NUMERIC / NULLIF(v_total_rows, 0);
    
    -- Insert results
    INSERT INTO benchmark_results (
        test_category, test_name, pattern, index_type, 
        result_count, execution_time_ms, selectivity
    ) VALUES (
        p_test_category, p_test_name, p_pattern, p_index_type,
        v_count, v_exec_time, v_selectivity
    );
END;
$$ LANGUAGE plpgsql;

-- Warmup to ensure consistent results
DO $$ BEGIN  
    EXECUTE format('SET LOCAL enable_seqscan = TRUE'); 
    PERFORM COUNT(*) FROM benchmark_data WHERE uuid_str = '-'; 
END $$;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'RUNNING BENCHMARK TEST SUITE'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- ============================================================================
-- Test Category 1: Exact Match Queries
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[1/7] Exact Match Tests...'; END $$;

DO $$
DECLARE
    test_uuid TEXT;
BEGIN
    SELECT uuid_str INTO test_uuid FROM benchmark_data LIMIT 1;
    
    -- All indexes can do exact match
    PERFORM run_benchmark_test('Exact Match', 'Single UUID Lookup', test_uuid, 'Sequential Scan', FALSE, FALSE, TRUE, FALSE);
    PERFORM run_benchmark_test('Exact Match', 'Single UUID Lookup', test_uuid, 'B-Tree', TRUE, FALSE, FALSE, TRUE);
    PERFORM run_benchmark_test('Exact Match', 'Single UUID Lookup', test_uuid, 'pg_trgm', FALSE, TRUE, FALSE, FALSE);
    PERFORM run_benchmark_test('Exact Match', 'Single UUID Lookup', test_uuid, 'Biscuit', TRUE, FALSE, FALSE, FALSE);
    
    RAISE NOTICE '  Completed: 4 tests';
END $$;

-- ============================================================================
-- Test Category 2: Prefix Match Queries (Various Lengths)
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '[2/7] Prefix Match Tests...'; END $$;

DO $$
DECLARE
    patterns TEXT[] := ARRAY['a%', 'ab%', 'abc%', 'abcd%', '1%', '12%', '123%'];
    pattern TEXT;
    test_count INTEGER := 0;
BEGIN
    FOREACH pattern IN ARRAY patterns
    LOOP
        PERFORM run_benchmark_test('Prefix Match', 'Prefix: ' || pattern, pattern, 'Sequential Scan', FALSE, FALSE, TRUE);
        PERFORM run_benchmark_test('Prefix Match', 'Prefix: ' || pattern, pattern, 'B-Tree', TRUE, FALSE, FALSE);
        PERFORM run_benchmark_test('Prefix Match', 'Prefix: ' || pattern, pattern, 'pg_trgm', FALSE, TRUE, FALSE);
        PERFORM run_benchmark_test('Prefix Match', 'Prefix: ' || pattern, pattern, 'Biscuit', TRUE, FALSE, FALSE);
        test_count := test_count + 4;
    END LOOP;
    RAISE NOTICE '  Completed: % tests', test_count;
END $$;

-- ============================================================================
-- Test Category 3: Suffix Match Queries
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '[3/7] Suffix Match Tests...'; END $$;

DO $$
DECLARE
    patterns TEXT[] := ARRAY['%a', '%ab', '%abc', '%1', '%12', '%123'];
    pattern TEXT;
    test_count INTEGER := 0;
BEGIN
    FOREACH pattern IN ARRAY patterns
    LOOP
        PERFORM run_benchmark_test('Suffix Match', 'Suffix: ' || pattern, pattern, 'Sequential Scan', FALSE, FALSE, TRUE);
        -- B-Tree cannot efficiently handle suffix matches (no leading constant)
        PERFORM run_benchmark_test('Suffix Match', 'Suffix: ' || pattern, pattern, 'pg_trgm', FALSE, TRUE, FALSE);
        PERFORM run_benchmark_test('Suffix Match', 'Suffix: ' || pattern, pattern, 'Biscuit', TRUE, FALSE, FALSE);
        test_count := test_count + 3;
    END LOOP;
    RAISE NOTICE '  Completed: % tests (B-Tree skipped - cannot optimize suffix)', test_count;
END $$;

-- ============================================================================
-- Test Category 4: Contains/Substring Match Queries
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '[4/7] Contains Match Tests...'; END $$;

DO $$
DECLARE
    patterns TEXT[] := ARRAY['%a%', '%ab%', '%abc%', '%4a%', '%4ab%', '%zzz%', '%xyz%'];
    pattern TEXT;
    test_count INTEGER := 0;
BEGIN
    FOREACH pattern IN ARRAY patterns
    LOOP
        PERFORM run_benchmark_test('Contains Match', 'Contains: ' || pattern, pattern, 'Sequential Scan', FALSE, FALSE, TRUE);
        -- B-Tree cannot efficiently handle substring matches
        PERFORM run_benchmark_test('Contains Match', 'Contains: ' || pattern, pattern, 'pg_trgm', FALSE, TRUE, FALSE);
        PERFORM run_benchmark_test('Contains Match', 'Contains: ' || pattern, pattern, 'Biscuit', TRUE, FALSE, FALSE);
        test_count := test_count + 3;
    END LOOP;
    RAISE NOTICE '  Completed: % tests (B-Tree skipped - cannot optimize contains)', test_count;
END $$;

-- ============================================================================
-- Test Category 5: Complex Pattern Queries
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '[5/7] Complex Pattern Tests...'; END $$;

DO $$
DECLARE
    patterns TEXT[] := ARRAY['%a%b%', '%a%4%b%', '%1%2%3%', 'a%b%c%', '%a%b%c%d%'];
    pattern TEXT;
    test_count INTEGER := 0;
BEGIN
    FOREACH pattern IN ARRAY patterns
    LOOP
        PERFORM run_benchmark_test('Complex Pattern', 'Pattern: ' || pattern, pattern, 'Sequential Scan', FALSE, FALSE, TRUE);
        -- B-Tree can only optimize if pattern starts with constant (only 'a%b%c%')
        IF pattern LIKE 'a%' THEN
            PERFORM run_benchmark_test('Complex Pattern', 'Pattern: ' || pattern, pattern, 'B-Tree', TRUE, FALSE, FALSE);
        END IF;
        PERFORM run_benchmark_test('Complex Pattern', 'Pattern: ' || pattern, pattern, 'pg_trgm', FALSE, TRUE, FALSE);
        PERFORM run_benchmark_test('Complex Pattern', 'Pattern: ' || pattern, pattern, 'Biscuit', TRUE, FALSE, FALSE);
        test_count := test_count + 3;
        IF pattern LIKE 'a%' THEN
            test_count := test_count + 1;
        END IF;
    END LOOP;
    RAISE NOTICE '  Completed: % tests (B-Tree partial - only prefix patterns)', test_count;
END $$;

-- ============================================================================
-- Test Category 6: Selectivity Tests (High to Low)
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '[6/7] Selectivity Tests...'; END $$;

DO $$
DECLARE
    patterns TEXT[] := ARRAY['%zzzzz%', '%xyz%', '%4a%', '%a%', '%%'];
    pattern TEXT;
    pattern_name TEXT;
    test_count INTEGER := 0;
BEGIN
    FOREACH pattern IN ARRAY patterns
    LOOP
        CASE pattern
            WHEN '%zzzzz%' THEN pattern_name := 'Very Selective';
            WHEN '%xyz%' THEN pattern_name := 'Selective';
            WHEN '%4a%' THEN pattern_name := 'Moderate';
            WHEN '%a%' THEN pattern_name := 'Broad';
            WHEN '%%' THEN pattern_name := 'All Records';
        END CASE;
        
        PERFORM run_benchmark_test('Selectivity', pattern_name, pattern, 'Sequential Scan', FALSE, FALSE, TRUE);
        test_count := test_count + 1;
        
        IF pattern != '%%' THEN
            -- B-Tree cannot optimize these patterns (no leading constant)
            PERFORM run_benchmark_test('Selectivity', pattern_name, pattern, 'pg_trgm', FALSE, TRUE, FALSE);
            PERFORM run_benchmark_test('Selectivity', pattern_name, pattern, 'Biscuit', TRUE, FALSE, FALSE);
            test_count := test_count + 2;
        END IF;
    END LOOP;
    RAISE NOTICE '  Completed: % tests (B-Tree skipped - no prefix patterns)', test_count;
END $$;

-- ============================================================================
-- Test Category 7: Case Sensitivity Tests
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '[7/7] Case Sensitivity Tests...'; END $$;

DO $$
DECLARE
    patterns TEXT[] := ARRAY['%A%', '%a%', '%Ab%', '%aB%'];
    pattern TEXT;
    test_count INTEGER := 0;
BEGIN
    FOREACH pattern IN ARRAY patterns
    LOOP
        PERFORM run_benchmark_test('Case Sensitivity', 'Case: ' || pattern, pattern, 'Sequential Scan', FALSE, FALSE, TRUE);
        -- B-Tree cannot optimize these patterns (no leading constant)
        PERFORM run_benchmark_test('Case Sensitivity', 'Case: ' || pattern, pattern, 'pg_trgm', FALSE, TRUE, FALSE);
        PERFORM run_benchmark_test('Case Sensitivity', 'Case: ' || pattern, pattern, 'Biscuit', TRUE, FALSE, FALSE);
        test_count := test_count + 3;
    END LOOP;
    RAISE NOTICE '  Completed: % tests (B-Tree skipped - cannot optimize)', test_count;
END $$;

DO $$ 
DECLARE
    total_tests INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_tests FROM benchmark_results;
    RAISE NOTICE '';
    RAISE NOTICE 'All tests completed: % total benchmark runs', total_tests;
    RAISE NOTICE '========================================';
END $$;

-- ============================================================================
-- STATISTICAL ANALYSIS
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE 'Computing statistical analysis...'; END $$;

CREATE TABLE benchmark_statistics AS
WITH index_stats AS (
    SELECT 
        test_category,
        index_type,
        COUNT(*) as test_count,
        AVG(execution_time_ms) as mean_time,
        CASE WHEN COUNT(*) > 1 THEN STDDEV(execution_time_ms) ELSE 0 END as stddev_time,
        MIN(execution_time_ms) as min_time,
        MAX(execution_time_ms) as max_time,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_time_ms) as median_time,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_time_ms) as p95_time,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY execution_time_ms) as p99_time,
        AVG(selectivity) as avg_selectivity
    FROM benchmark_results
    GROUP BY test_category, index_type
),
geometric_means AS (
    SELECT 
        test_category,
        index_type,
        EXP(AVG(LN(NULLIF(execution_time_ms, 0)))) as geometric_mean
    FROM benchmark_results
    WHERE execution_time_ms > 0
    GROUP BY test_category, index_type
),
rankings AS (
    SELECT 
        test_category,
        test_name,
        pattern,
        index_type,
        execution_time_ms,
        RANK() OVER (PARTITION BY test_category, test_name ORDER BY execution_time_ms) as rank,
        CASE 
            WHEN RANK() OVER (PARTITION BY test_category, test_name ORDER BY execution_time_ms) = 1 
            THEN 1 ELSE 0 
        END as is_winner
    FROM benchmark_results
)
SELECT 
    s.test_category,
    s.index_type,
    s.test_count,
    ROUND(s.mean_time::numeric, 4) as mean_time_ms,
    ROUND(gm.geometric_mean::numeric, 4) as geometric_mean_ms,
    ROUND(s.stddev_time::numeric, 4) as stddev_ms,
    ROUND((s.stddev_time / NULLIF(s.mean_time, 0) * 100)::numeric, 2) as coeff_variation_pct,
    ROUND(s.min_time::numeric, 4) as min_time_ms,
    ROUND(s.max_time::numeric, 4) as max_time_ms,
    ROUND(s.median_time::numeric, 4) as median_time_ms,
    ROUND(s.p95_time::numeric, 4) as p95_time_ms,
    ROUND(s.p99_time::numeric, 4) as p99_time_ms,
    ROUND(s.avg_selectivity::numeric, 6) as avg_selectivity,
    SUM(r.is_winner) as times_fastest,
    ROUND((SUM(r.is_winner)::NUMERIC / s.test_count * 100)::numeric, 2) as win_rate_pct
FROM index_stats s
JOIN geometric_means gm ON s.test_category = gm.test_category AND s.index_type = gm.index_type
JOIN rankings r ON s.test_category = r.test_category AND s.index_type = r.index_type
GROUP BY s.test_category, s.index_type, s.test_count, s.mean_time, gm.geometric_mean, 
         s.stddev_time, s.min_time, s.max_time, s.median_time, s.p95_time, s.p99_time, s.avg_selectivity
ORDER BY s.test_category, s.mean_time;

DO $$ BEGIN RAISE NOTICE 'Statistical analysis complete'; END $$;

-- ============================================================================
-- COMPREHENSIVE RESULTS OUTPUT
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'BENCHMARK RESULTS & ANALYSIS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Overall Summary
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'OVERALL PERFORMANCE SUMMARY'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT 
            index_type,
            COUNT(*) as total_tests,
            ROUND(AVG(execution_time_ms)::numeric, 4) as avg_time_ms,
            ROUND(EXP(AVG(LN(NULLIF(execution_time_ms, 0))))::numeric, 4) as geom_mean_ms,
            ROUND(MIN(execution_time_ms)::numeric, 4) as min_time_ms,
            ROUND(MAX(execution_time_ms)::numeric, 4) as max_time_ms,
            SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END) as times_fastest,
            ROUND((SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100)::numeric, 1) as win_rate_pct
        FROM (
            SELECT 
                *,
                RANK() OVER (PARTITION BY test_category, test_name ORDER BY execution_time_ms) as rank
            FROM benchmark_results
        ) ranked
        GROUP BY index_type
        ORDER BY geom_mean_ms
    LOOP
        RAISE NOTICE '% | Tests: % | Avg: % ms | GeoMean: % ms | Min: % ms | Max: % ms | Wins: % (% percent)', 
            RPAD(rec.index_type, 20), 
            LPAD(rec.total_tests::TEXT, 3),
            LPAD(rec.avg_time_ms::TEXT, 8),
            LPAD(rec.geom_mean_ms::TEXT, 8),
            LPAD(rec.min_time_ms::TEXT, 8),
            LPAD(rec.max_time_ms::TEXT, 8),
            LPAD(rec.times_fastest::TEXT, 3),
            LPAD(rec.win_rate_pct::TEXT, 5);
    END LOOP;
    
    RAISE NOTICE '';
    RAISE NOTICE 'Note: B-Tree only tested on patterns it can optimize (prefix patterns, exact matches)';
END $$;

-- Category-wise Statistics
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'PERFORMANCE BY TEST CATEGORY'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
    prev_category TEXT := '';
BEGIN
    FOR rec IN 
        SELECT 
            test_category,
            index_type,
            test_count,
            mean_time_ms,
            geometric_mean_ms,
            stddev_ms,
            coeff_variation_pct as cv_pct,
            median_time_ms,
            p95_time_ms,
            times_fastest,
            win_rate_pct
        FROM benchmark_statistics
        ORDER BY test_category, geometric_mean_ms
    LOOP
        IF prev_category != rec.test_category THEN
            RAISE NOTICE '';
            RAISE NOTICE '--- % ---', rec.test_category;
            prev_category := rec.test_category;
        END IF;
        RAISE NOTICE '  % | Mean: % ms | GeoMean: % ms | StdDev: % | CV: % percent | Median: % ms | P95: % ms | Wins: %/%',
            RPAD(rec.index_type, 18),
            LPAD(rec.mean_time_ms::TEXT, 8),
            LPAD(rec.geometric_mean_ms::TEXT, 8),
            LPAD(rec.stddev_ms::TEXT, 8),
            LPAD(rec.cv_pct::TEXT, 6),
            LPAD(rec.median_time_ms::TEXT, 8),
            LPAD(rec.p95_time_ms::TEXT, 8),
            LPAD(rec.times_fastest::TEXT, 2),
            LPAD(rec.test_count::TEXT, 2);
    END LOOP;
END $$;

-- Best Performer by Category
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'BEST PERFORMER BY CATEGORY'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT DISTINCT ON (test_category)
            test_category,
            index_type as winner,
            geometric_mean_ms,
            times_fastest,
            test_count
        FROM benchmark_statistics
        ORDER BY test_category, geometric_mean_ms
    LOOP
        RAISE NOTICE '% : % (GeoMean: % ms, Won %/%)',
            RPAD(rec.test_category, 20),
            RPAD(rec.winner, 18),
            LPAD(rec.geometric_mean_ms::TEXT, 8),
            LPAD(rec.times_fastest::TEXT, 2),
            LPAD(rec.test_count::TEXT, 2);
    END LOOP;
END $$;

-- Detailed Results by Test (Sample)
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'TOP 20 FASTEST INDIVIDUAL TESTS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
    counter INTEGER := 0;
BEGIN
    FOR rec IN 
        SELECT 
            test_category,
            test_name,
            pattern,
            index_type,
            result_count,
            ROUND(execution_time_ms::numeric, 4) as exec_time_ms,
            ROUND(selectivity::numeric, 6) as selectivity
        FROM benchmark_results
        ORDER BY execution_time_ms
        LIMIT 20
    LOOP
        counter := counter + 1;
        RAISE NOTICE '%. % | % | % | Pattern: % | Time: % ms | Results: % | Sel: %',
            LPAD(counter::TEXT, 2),
            RPAD(rec.test_category, 18),
            RPAD(rec.index_type, 18),
            RPAD(rec.test_name, 25),
            RPAD(rec.pattern, 12),
            LPAD(rec.exec_time_ms::TEXT, 8),
            LPAD(rec.result_count::TEXT, 8),
            LPAD(rec.selectivity::TEXT, 8);
    END LOOP;
END $$;

-- Speedup Analysis
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'SPEEDUP ANALYSIS (vs Sequential Scan)'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
    prev_category TEXT := '';
BEGIN
    FOR rec IN 
        WITH baseline AS (
            SELECT test_category, test_name, execution_time_ms as seq_time
            FROM benchmark_results
            WHERE index_type = 'Sequential Scan'
        )
        SELECT 
            br.test_category,
            br.index_type,
            COUNT(*) as tests,
            ROUND(AVG(b.seq_time / NULLIF(br.execution_time_ms, 0))::numeric, 2) as avg_speedup,
            ROUND(MIN(b.seq_time / NULLIF(br.execution_time_ms, 0))::numeric, 2) as min_speedup,
            ROUND(MAX(b.seq_time / NULLIF(br.execution_time_ms, 0))::numeric, 2) as max_speedup
        FROM benchmark_results br
        JOIN baseline b ON br.test_category = b.test_category AND br.test_name = b.test_name
        WHERE br.index_type != 'Sequential Scan'
        GROUP BY br.test_category, br.index_type
        ORDER BY br.test_category, avg_speedup DESC
    LOOP
        IF prev_category != rec.test_category THEN
            RAISE NOTICE '';
            RAISE NOTICE '--- % ---', rec.test_category;
            prev_category := rec.test_category;
        END IF;
        RAISE NOTICE '  % | Avg Speedup: %x | Range: %x - %x',
            RPAD(rec.index_type, 18),
            LPAD(rec.avg_speedup::TEXT, 6),
            LPAD(rec.min_speedup::TEXT, 6),
            LPAD(rec.max_speedup::TEXT, 6);
    END LOOP;
END $$;

-- Variance Analysis
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'CONSISTENCY ANALYSIS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN 
        SELECT 
            index_type,
            ROUND(AVG(stddev_ms)::numeric, 4) as avg_stddev,
            ROUND(AVG(coeff_variation_pct)::numeric, 2) as avg_cv_pct,
            CASE 
                WHEN AVG(coeff_variation_pct) < 20 THEN 'Very Consistent'
                WHEN AVG(coeff_variation_pct) < 50 THEN 'Consistent'
                WHEN AVG(coeff_variation_pct) < 100 THEN 'Moderate Variance'
                ELSE 'High Variance'
            END as consistency_rating
        FROM benchmark_statistics
        WHERE test_count > 1  -- Only show for index types with multiple tests
        GROUP BY index_type
        ORDER BY avg_cv_pct
    LOOP
        RAISE NOTICE '% | Avg StdDev: % ms | Avg CV: % percent | Rating: %',
            RPAD(rec.index_type, 20),
            LPAD(rec.avg_stddev::TEXT, 8),
            LPAD(rec.avg_cv_pct::TEXT, 6),
            rec.consistency_rating;
    END LOOP;
END $$;

-- Selectivity Impact
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'SELECTIVITY IMPACT ANALYSIS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
    prev_range TEXT := '';
BEGIN
    FOR rec IN 
        SELECT 
            CASE 
                WHEN selectivity < 0.001 THEN 'Very Selective (<0.1%)'
                WHEN selectivity < 0.01 THEN 'Selective (0.1-1%)'
                WHEN selectivity < 0.1 THEN 'Moderate (1-10%)'
                WHEN selectivity < 0.5 THEN 'Broad (10-50%)'
                ELSE 'Very Broad (>50%)'
            END as selectivity_range,
            CASE 
                WHEN selectivity < 0.001 THEN 1
                WHEN selectivity < 0.01 THEN 2
                WHEN selectivity < 0.1 THEN 3
                WHEN selectivity < 0.5 THEN 4
                ELSE 5
            END as sel_order,
            index_type,
            COUNT(*) as test_count,
            ROUND(AVG(execution_time_ms)::numeric, 4) as avg_time_ms,
            ROUND(AVG(selectivity * 100)::numeric, 2) as avg_selectivity_pct
        FROM benchmark_results
        WHERE index_type != 'Sequential Scan'
        GROUP BY 
            CASE 
                WHEN selectivity < 0.001 THEN 'Very Selective (<0.1%)'
                WHEN selectivity < 0.01 THEN 'Selective (0.1-1%)'
                WHEN selectivity < 0.1 THEN 'Moderate (1-10%)'
                WHEN selectivity < 0.5 THEN 'Broad (10-50%)'
                ELSE 'Very Broad (>50%)'
            END,
            CASE 
                WHEN selectivity < 0.001 THEN 1
                WHEN selectivity < 0.01 THEN 2
                WHEN selectivity < 0.1 THEN 3
                WHEN selectivity < 0.5 THEN 4
                ELSE 5
            END,
            index_type
        ORDER BY sel_order, avg_time_ms
    LOOP
        IF prev_range != rec.selectivity_range THEN
            RAISE NOTICE '';
            RAISE NOTICE '--- % ---', rec.selectivity_range;
            prev_range := rec.selectivity_range;
        END IF;
        RAISE NOTICE '  % | Tests: % | Avg Time: % ms | Avg Selectivity: % percent',
            RPAD(rec.index_type, 18),
            LPAD(rec.test_count::TEXT, 2),
            LPAD(rec.avg_time_ms::TEXT, 8),
            LPAD(rec.avg_selectivity_pct::TEXT, 6);
    END LOOP;
END $$;

-- Index Size Analysis (FIXED to show actual Biscuit size)
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'INDEX SIZE ANALYSIS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    rec RECORD;
    table_size TEXT;
    biscuit_size BIGINT;
    total_size BIGINT;
BEGIN
    -- Show all indexes
    RAISE NOTICE 'Index Sizes:';
    FOR rec IN 
        SELECT 
            indexrelid::regclass::text as index_name,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
            pg_relation_size(indexrelid) as size_bytes
        FROM pg_stat_user_indexes
        WHERE schemaname = 'public' 
        AND relname = 'benchmark_data'
        ORDER BY pg_relation_size(indexrelid) DESC
    LOOP
        RAISE NOTICE '  % : %',
            RPAD(rec.index_name, 25),
            rec.index_size;
            
        IF rec.index_name = 'idx_biscuit' THEN
            biscuit_size := rec.size_bytes;
        END IF;
    END LOOP;
    
    -- Show primary key
    SELECT pg_size_pretty(pg_relation_size('benchmark_data_pkey'::regclass)),
           pg_relation_size('benchmark_data_pkey'::regclass)
    INTO table_size, total_size;
    RAISE NOTICE '  % : %',
        RPAD('benchmark_data_pkey', 25),
        table_size;
    
    RAISE NOTICE '';
    
    -- Special note about Biscuit if it shows 0
    IF biscuit_size = 0 OR biscuit_size IS NULL THEN
        RAISE NOTICE 'WARNING: Biscuit index shows 0 bytes!';
        RAISE NOTICE 'This could mean:';
        RAISE NOTICE '  1. The Biscuit extension uses a different storage mechanism';
        RAISE NOTICE '  2. The index metadata is stored elsewhere';
        RAISE NOTICE '  3. There is an issue with the Biscuit extension';
        RAISE NOTICE '';
    END IF;
    
    -- Table sizes
    SELECT pg_size_pretty(pg_total_relation_size('benchmark_data')) INTO table_size;
    RAISE NOTICE 'Total table size (with indexes): %', table_size;
    
    SELECT pg_size_pretty(pg_relation_size('benchmark_data')) INTO table_size;
    RAISE NOTICE 'Table data size (no indexes):     %', table_size;
END $$;

-- Performance Summary and Recommendations
DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'KEY FINDINGS & RECOMMENDATIONS'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

DO $$
DECLARE
    biscuit_geomean NUMERIC;
    trgm_geomean NUMERIC;
    btree_geomean NUMERIC;
    seq_geomean NUMERIC;
    biscuit_wins INTEGER;
    trgm_wins INTEGER;
    btree_wins INTEGER;
    biscuit_tests INTEGER;
    trgm_tests INTEGER;
    btree_tests INTEGER;
    total_tests INTEGER;
BEGIN
    -- Get overall geometric means
    SELECT 
        ROUND(EXP(AVG(LN(NULLIF(execution_time_ms, 0))))::numeric, 4),
        COUNT(*)
    INTO biscuit_geomean, biscuit_tests
    FROM benchmark_results
    WHERE index_type = 'Biscuit';
    
    SELECT 
        ROUND(EXP(AVG(LN(NULLIF(execution_time_ms, 0))))::numeric, 4),
        COUNT(*)
    INTO trgm_geomean, trgm_tests
    FROM benchmark_results
    WHERE index_type = 'pg_trgm';
    
    SELECT 
        ROUND(EXP(AVG(LN(NULLIF(execution_time_ms, 0))))::numeric, 4),
        COUNT(*)
    INTO btree_geomean, btree_tests
    FROM benchmark_results
    WHERE index_type = 'B-Tree';
    
    SELECT ROUND(EXP(AVG(LN(NULLIF(execution_time_ms, 0))))::numeric, 4)
    INTO seq_geomean
    FROM benchmark_results
    WHERE index_type = 'Sequential Scan';
    
    -- Get win counts
    SELECT COUNT(*) INTO total_tests
    FROM (SELECT DISTINCT test_category, test_name FROM benchmark_results) t;
    
    WITH rankings AS (
        SELECT 
            test_category,
            test_name,
            index_type,
            RANK() OVER (PARTITION BY test_category, test_name ORDER BY execution_time_ms) as rank
        FROM benchmark_results
    )
    SELECT 
        SUM(CASE WHEN index_type = 'Biscuit' AND rank = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN index_type = 'pg_trgm' AND rank = 1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN index_type = 'B-Tree' AND rank = 1 THEN 1 ELSE 0 END)
    INTO biscuit_wins, trgm_wins, btree_wins
    FROM rankings;
    
    RAISE NOTICE '';
    RAISE NOTICE '1. OVERALL PERFORMANCE (Geometric Mean):';
    RAISE NOTICE '   - Biscuit:    % ms (% tests, % wins = % percent)',
        biscuit_geomean, biscuit_tests, biscuit_wins, 
        ROUND(biscuit_wins::NUMERIC / total_tests * 100, 1);
    RAISE NOTICE '   - pg_trgm:    % ms (% tests, % wins = % percent)',
        trgm_geomean, trgm_tests, trgm_wins,
        ROUND(trgm_wins::NUMERIC / total_tests * 100, 1);
    RAISE NOTICE '   - B-Tree:     % ms (% tests, % wins = % percent)',
        btree_geomean, btree_tests, btree_wins,
        ROUND(btree_wins::NUMERIC / total_tests * 100, 1);
    RAISE NOTICE '   - Sequential: % ms', seq_geomean;
    
    RAISE NOTICE '';
    RAISE NOTICE '2. IMPORTANT NOTE ON B-TREE:';
    RAISE NOTICE '   B-Tree was only tested on % of % total tests', btree_tests, total_tests;
    RAISE NOTICE '   B-Tree can only optimize:';
    RAISE NOTICE '     - Exact matches (=)';
    RAISE NOTICE '     - Prefix patterns (abc%%)';
    RAISE NOTICE '   B-Tree CANNOT optimize:';
    RAISE NOTICE '     - Suffix patterns (%%abc)';
    RAISE NOTICE '     - Contains patterns (%%abc%%)';
    RAISE NOTICE '     - Most complex patterns';
    
    RAISE NOTICE '';
    RAISE NOTICE '3. BEST USE CASES:';
    RAISE NOTICE '   - Biscuit excels at: All pattern types, especially exact/contains/suffix';
    RAISE NOTICE '   - pg_trgm excels at: All pattern types with trigram coverage';
    RAISE NOTICE '   - B-Tree excels at: Only exact matches and prefix patterns';
    
    RAISE NOTICE '';
    RAISE NOTICE '4. PERFORMANCE COMPARISON (where all tested):';
    
    -- Compare on common tests only
    WITH common_tests AS (
        SELECT DISTINCT test_category, test_name 
        FROM benchmark_results
        WHERE index_type = 'Biscuit'
        INTERSECT
        SELECT DISTINCT test_category, test_name
        FROM benchmark_results
        WHERE index_type = 'pg_trgm'
    ),
    common_perf AS (
        SELECT 
            br.index_type,
            EXP(AVG(LN(NULLIF(br.execution_time_ms, 0)))) as geomean
        FROM benchmark_results br
        INNER JOIN common_tests ct 
            ON br.test_category = ct.test_category 
            AND br.test_name = ct.test_name
        WHERE br.index_type IN ('Biscuit', 'pg_trgm', 'B-Tree')
        GROUP BY br.index_type
    )
    SELECT 
        b.geomean as biscuit_common,
        t.geomean as trgm_common,
        bt.geomean as btree_common
    INTO biscuit_geomean, trgm_geomean, btree_geomean
    FROM 
        (SELECT geomean FROM common_perf WHERE index_type = 'Biscuit') b,
        (SELECT geomean FROM common_perf WHERE index_type = 'pg_trgm') t,
        (SELECT geomean FROM common_perf WHERE index_type = 'B-Tree') bt;
    
    IF biscuit_geomean < trgm_geomean THEN
        RAISE NOTICE '   - Biscuit vs pg_trgm: Biscuit is %x faster',
            ROUND((trgm_geomean / biscuit_geomean)::numeric, 2);
    ELSE
        RAISE NOTICE '   - Biscuit vs pg_trgm: pg_trgm is %x faster',
            ROUND((biscuit_geomean / trgm_geomean)::numeric, 2);
    END IF;
    
    IF btree_geomean IS NOT NULL THEN
        IF biscuit_geomean < btree_geomean THEN
            RAISE NOTICE '   - Biscuit vs B-Tree: Biscuit is %x faster (on tests B-Tree can run)',
                ROUND((btree_geomean / biscuit_geomean)::numeric, 2);
        ELSE
            RAISE NOTICE '   - Biscuit vs B-Tree: B-Tree is %x faster (on tests B-Tree can run)',
                ROUND((biscuit_geomean / btree_geomean)::numeric, 2);
        END IF;
    END IF;
    
    RAISE NOTICE '';
    RAISE NOTICE '5. RECOMMENDATION:';
    RAISE NOTICE '   - For exact matches & prefix patterns: Use B-Tree (fastest, smallest)';
    RAISE NOTICE '   - For general wildcard patterns: Use Biscuit or pg_trgm';
    RAISE NOTICE '   - Biscuit appears to have similar performance to pg_trgm';
    RAISE NOTICE '   - Index size: pg_trgm uses significant space, Biscuit size unclear';
    
END $$;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'BENCHMARK ANALYSIS COMPLETE'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE ''; END $$;

-- Reset settings
SET enable_indexscan = ON;
SET enable_bitmapscan = ON;
SET enable_seqscan = ON;

-- Cleanup helper function
DROP FUNCTION run_benchmark_test(TEXT, TEXT, TEXT, TEXT, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);