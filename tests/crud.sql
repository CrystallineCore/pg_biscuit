-- ============================================================================
-- Biscuit Index Access Method - Comprehensive Test Suite
-- ============================================================================
-- This script tests all CRUD operations and pattern matching accuracy
-- PostgreSQL 15+ required
-- Run after: CREATE EXTENSION pg_biscuit;
-- ============================================================================

DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Biscuit IAM Comprehensive Test Suite'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- ============================================================================
-- SETUP: Create test table and data
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[SETUP] Creating test table...'; END $$;

DROP TABLE IF EXISTS biscuit_test CASCADE;

CREATE TABLE biscuit_test (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    status TEXT DEFAULT 'active'
);

-- Insert diverse test data
INSERT INTO biscuit_test (username, email, status) VALUES
    -- Prefix patterns
    ('admin', 'admin@example.com', 'active'),
    ('admin_user', 'admin.user@example.com', 'active'),
    ('administrator', 'administrator@example.com', 'active'),
    
    -- Suffix patterns
    ('user_admin', 'user@admin.com', 'active'),
    ('super_admin', 'super@admin.com', 'active'),
    
    -- Contains patterns
    ('test_admin_user', 'test@example.com', 'active'),
    ('my_admin_account', 'my.admin@example.com', 'active'),
    
    -- Exact matches
    ('john', 'john@example.com', 'active'),
    ('jane', 'jane@example.com', 'active'),
    ('bob', 'bob@example.com', 'inactive'),
    
    -- Wildcards
    ('user_123', 'user123@example.com', 'active'),
    ('user_456', 'user456@example.com', 'active'),
    ('user_789', 'user789@example.com', 'active'),
    
    -- Edge cases
    ('a', 'a@example.com', 'active'),
    ('ab', 'ab@example.com', 'active'),
    ('abc', 'abc@example.com', 'active'),
    ('', 'empty@example.com', 'active'),
    
    -- Special characters
    ('user@host', 'special1@example.com', 'active'),
    ('user.name', 'special2@example.com', 'active'),
    ('user-name', 'special3@example.com', 'active'),
    
    -- Long strings
    ('verylongusernamethatexceedsnormallength', 'long@example.com', 'active'),
    
    -- Case sensitivity test
    ('Admin', 'Admin@example.com', 'active'),
    ('ADMIN', 'ADMIN@example.com', 'active');

DO $$ BEGIN RAISE NOTICE '[SETUP] Inserted % rows', (SELECT COUNT(*) FROM biscuit_test); END $$;

-- ============================================================================
-- TEST 1: Index Creation
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 1] Creating Biscuit index...'; END $$;

CREATE INDEX idx_username_biscuit ON biscuit_test USING biscuit(username);
CREATE INDEX idx_email_biscuit ON biscuit_test USING biscuit(email);

DO $$ BEGIN RAISE NOTICE '[TEST 1] ✓ Indexes created successfully'; END $$;

-- Verify indexes exist
DO $$
DECLARE
    idx_count INT;
BEGIN
    SELECT COUNT(*) INTO idx_count
    FROM pg_indexes
    WHERE schemaname = 'public'
    AND tablename = 'biscuit_test'
    AND indexdef LIKE '%biscuit%';
    
    IF idx_count = 2 THEN
        RAISE NOTICE '[TEST 1] ✓ Found % Biscuit indexes', idx_count;
    ELSE
        RAISE WARNING '[TEST 1] ✗ Expected 2 indexes, found %', idx_count;
    END IF;
END $$;

-- ============================================================================
-- TEST 2: Pattern Matching Accuracy (SELECT)
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 2] Testing pattern matching accuracy...'; END $$;

-- Test 2.1: Prefix patterns
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE 'admin%';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE 'admin%';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.1] ✓ Prefix pattern "admin%%": SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.1] ✗ Prefix pattern mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- Test 2.2: Suffix patterns
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE '%admin';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE '%admin';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.2] ✓ Suffix pattern "%%admin": SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.2] ✗ Suffix pattern mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- Test 2.3: Contains patterns
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE '%admin%';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE '%admin%';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.3] ✓ Contains pattern "%%admin%%": SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.3] ✗ Contains pattern mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- Test 2.4: Exact match
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE 'john';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE 'john';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.4] ✓ Exact match "john": SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.4] ✗ Exact match mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- Test 2.5: Wildcard patterns
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE 'user_1%3';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE 'user_1%3';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.5] ✓ Wildcard pattern "user_1%%3": SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.5] ✗ Wildcard pattern mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- Test 2.6: Empty string
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE '';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE '';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.6] ✓ Empty pattern: SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.6] ✗ Empty pattern mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- Test 2.7: Match all pattern
DO $$
DECLARE
    count_seq INT;
    count_idx INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    SELECT COUNT(*) INTO count_seq FROM biscuit_test WHERE username LIKE '%';
    
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    SELECT COUNT(*) INTO count_idx FROM biscuit_test WHERE username LIKE '%';
    
    IF count_seq = count_idx THEN
        RAISE NOTICE '[TEST 2.7] ✓ Match all pattern "%%": SeqScan=%, IndexScan=%', count_seq, count_idx;
    ELSE
        RAISE WARNING '[TEST 2.7] ✗ Match all pattern mismatch: SeqScan=%, IndexScan=%', count_seq, count_idx;
    END IF;
    
    SET enable_seqscan = ON;
END $$;

-- ============================================================================
-- TEST 3: INSERT Operations
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 3] Testing INSERT operations...'; END $$;

-- Test 3.1: Single insert
DO $$
DECLARE
    initial_count INT;
    after_count INT;
BEGIN
    SELECT COUNT(*) INTO initial_count FROM biscuit_test;
    
    INSERT INTO biscuit_test (username, email, status) 
    VALUES ('newuser', 'newuser@example.com', 'active');
    
    SELECT COUNT(*) INTO after_count FROM biscuit_test;
    
    IF after_count = initial_count + 1 THEN
        RAISE NOTICE '[TEST 3.1] ✓ Single insert successful: % -> % rows', initial_count, after_count;
    ELSE
        RAISE WARNING '[TEST 3.1] ✗ Insert failed: expected %, got %', initial_count + 1, after_count;
    END IF;
END $$;

-- Test 3.2: Verify new record is searchable
DO $$
DECLARE
    found_count INT;
BEGIN
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE 'newuser';
    SET enable_seqscan = ON;
    
    IF found_count = 1 THEN
        RAISE NOTICE '[TEST 3.2] ✓ New record found via index';
    ELSE
        RAISE WARNING '[TEST 3.2] ✗ New record not found via index (found %)', found_count;
    END IF;
END $$;

-- Test 3.3: Bulk insert
DO $$
DECLARE
    initial_count INT;
    after_count INT;
    inserted INT := 5;
BEGIN
    SELECT COUNT(*) INTO initial_count FROM biscuit_test;
    
    INSERT INTO biscuit_test (username, email, status) 
    SELECT 
        'bulkuser' || i,
        'bulkuser' || i || '@example.com',
        'active'
    FROM generate_series(1, inserted) i;
    
    SELECT COUNT(*) INTO after_count FROM biscuit_test;
    
    IF after_count = initial_count + inserted THEN
        RAISE NOTICE '[TEST 3.3] ✓ Bulk insert successful: inserted % rows', inserted;
    ELSE
        RAISE WARNING '[TEST 3.3] ✗ Bulk insert failed: expected %, got %', initial_count + inserted, after_count;
    END IF;
END $$;

-- Test 3.4: Verify bulk records are searchable
DO $$
DECLARE
    found_count INT;
BEGIN
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE 'bulkuser%';
    SET enable_seqscan = ON;
    
    IF found_count = 5 THEN
        RAISE NOTICE '[TEST 3.4] ✓ All bulk records found via index (%)' , found_count;
    ELSE
        RAISE WARNING '[TEST 3.4] ✗ Expected 5 bulk records, found %', found_count;
    END IF;
END $$;

-- ============================================================================
-- TEST 4: UPDATE Operations
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 4] Testing UPDATE operations...'; END $$;

-- Test 4.1: Update single record
DO $$
DECLARE
    old_found INT;
    new_found INT;
BEGIN
    UPDATE biscuit_test SET username = 'updateduser' WHERE username = 'newuser';
    
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO old_found FROM biscuit_test WHERE username LIKE 'newuser';
    SELECT COUNT(*) INTO new_found FROM biscuit_test WHERE username LIKE 'updateduser';
    SET enable_seqscan = ON;
    
    IF old_found = 0 AND new_found = 1 THEN
        RAISE NOTICE '[TEST 4.1] ✓ Update successful: old=%, new=%', old_found, new_found;
    ELSE
        RAISE WARNING '[TEST 4.1] ✗ Update failed: old=% (expected 0), new=% (expected 1)', old_found, new_found;
    END IF;
END $$;

-- Test 4.2: Bulk update
DO $$
DECLARE
    updated_count INT;
    found_count INT;
BEGIN
    UPDATE biscuit_test SET username = 'BULK' || SUBSTRING(username FROM 9) 
    WHERE username LIKE 'bulkuser%';
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE 'BULK%';
    SET enable_seqscan = ON;
    
    IF updated_count = found_count THEN
        RAISE NOTICE '[TEST 4.2] ✓ Bulk update successful: updated % rows', updated_count;
    ELSE
        RAISE WARNING '[TEST 4.2] ✗ Bulk update mismatch: updated=%, found=%', updated_count, found_count;
    END IF;
END $$;

-- ============================================================================
-- TEST 5: DELETE Operations
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 5] Testing DELETE operations...'; END $$;

-- Test 5.1: Single delete
DO $$
DECLARE
    before_count INT;
    after_count INT;
    found_count INT;
BEGIN
    SELECT COUNT(*) INTO before_count FROM biscuit_test;
    
    DELETE FROM biscuit_test WHERE username = 'updateduser';
    
    SELECT COUNT(*) INTO after_count FROM biscuit_test;
    
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE 'updateduser';
    SET enable_seqscan = ON;
    
    IF after_count = before_count - 1 AND found_count = 0 THEN
        RAISE NOTICE '[TEST 5.1] ✓ Single delete successful: % -> % rows', before_count, after_count;
    ELSE
        RAISE WARNING '[TEST 5.1] ✗ Delete failed: before=%, after=%, still found=%', before_count, after_count, found_count;
    END IF;
END $$;

-- Test 5.2: Bulk delete
DO $$
DECLARE
    before_count INT;
    after_count INT;
    deleted_count INT;
    found_count INT;
BEGIN
    SELECT COUNT(*) INTO before_count FROM biscuit_test;
    
    DELETE FROM biscuit_test WHERE username LIKE 'BULK%';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    SELECT COUNT(*) INTO after_count FROM biscuit_test;
    
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE 'BULK%';
    SET enable_seqscan = ON;
    
    IF after_count = before_count - deleted_count AND found_count = 0 THEN
        RAISE NOTICE '[TEST 5.2] ✓ Bulk delete successful: deleted % rows', deleted_count;
    ELSE
        RAISE WARNING '[TEST 5.2] ✗ Delete failed: deleted=%, still found=%', deleted_count, found_count;
    END IF;
END $$;

-- ============================================================================
-- TEST 6: Tombstone and Cleanup
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 6] Testing tombstone management...'; END $$;

-- Test 6.1: Check index stats before cleanup
DO $$
DECLARE
    stats TEXT;
BEGIN
    SELECT biscuit_index_stats('idx_username_biscuit'::regclass::oid) INTO stats;
    RAISE NOTICE '[TEST 6.1] Index stats before cleanup:';
    RAISE NOTICE '%', stats;
END $$;

-- Test 6.2: Run VACUUM to trigger cleanup
DO $$ BEGIN RAISE NOTICE '[TEST 6.2] Running VACUUM to trigger tombstone cleanup...'; END $$;

VACUUM ANALYZE biscuit_test;

-- Test 6.3: Check index stats after cleanup
DO $$
DECLARE
    stats TEXT;
BEGIN
    SELECT biscuit_index_stats('idx_username_biscuit'::regclass::oid) INTO stats;
    RAISE NOTICE '[TEST 6.3] Index stats after cleanup:';
    RAISE NOTICE '%', stats;
END $$;

-- ============================================================================
-- TEST 7: Stress Test with Many Operations
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 7] Running stress test...'; END $$;

-- Test 7.1: Insert 100 records
DO $$
DECLARE
    initial_count INT;
    after_count INT;
BEGIN
    SELECT COUNT(*) INTO initial_count FROM biscuit_test;
    
    INSERT INTO biscuit_test (username, email, status)
    SELECT 
        'stress' || i,
        'stress' || i || '@example.com',
        CASE WHEN i % 2 = 0 THEN 'active' ELSE 'inactive' END
    FROM generate_series(1, 100) i;
    
    SELECT COUNT(*) INTO after_count FROM biscuit_test;
    
    RAISE NOTICE '[TEST 7.1] ✓ Inserted % records: % -> %', 
        after_count - initial_count, initial_count, after_count;
END $$;

-- Test 7.2: Delete every other record
DO $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM biscuit_test WHERE username LIKE 'stress%' AND status = 'inactive';
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RAISE NOTICE '[TEST 7.2] ✓ Deleted % records', deleted_count;
END $$;

-- Test 7.3: Verify remaining records
DO $$
DECLARE
    remaining INT;
BEGIN
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO remaining FROM biscuit_test WHERE username LIKE 'stress%';
    SET enable_seqscan = ON;
    
    RAISE NOTICE '[TEST 7.3] ✓ % records remain after deletion', remaining;
END $$;

-- ============================================================================
-- TEST 8: Edge Cases
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 8] Testing edge cases...'; END $$;

-- Test 8.1: NULL values
DO $$
DECLARE
    inserted_id INT;
    found_count INT;
BEGIN
    INSERT INTO biscuit_test (username, email, status) 
    VALUES (NULL, 'null@example.com', 'active')
    RETURNING id INTO inserted_id;
    
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username IS NULL;
    
    IF found_count >= 1 THEN
        RAISE NOTICE '[TEST 8.1] ✓ NULL value handled correctly';
    ELSE
        RAISE WARNING '[TEST 8.1] ✗ NULL value not found';
    END IF;
END $$;

-- Test 8.2: Very long strings
DO $$
DECLARE
    long_str TEXT := REPEAT('a', 500);
    found_count INT;
BEGIN
    INSERT INTO biscuit_test (username, email, status)
    VALUES (long_str, 'long@example.com', 'active');
    
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE REPEAT('a', 100) || '%';
    SET enable_seqscan = ON;
    
    IF found_count >= 1 THEN
        RAISE NOTICE '[TEST 8.2] ✓ Long string (500 chars) handled correctly';
    ELSE
        RAISE WARNING '[TEST 8.2] ✗ Long string not found';
    END IF;
END $$;

-- Test 8.3: Special characters in patterns
DO $$
DECLARE
    found_count INT;
BEGIN
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_count FROM biscuit_test WHERE username LIKE 'user%host';
    SET enable_seqscan = ON;
    
    IF found_count = 1 THEN
        RAISE NOTICE '[TEST 8.3] ✓ Special character pattern matched correctly';
    ELSE
        RAISE WARNING '[TEST 8.3] ✗ Special character pattern failed (found %)', found_count;
    END IF;
END $$;

-- ============================================================================
-- TEST 9: Concurrent Operations Simulation
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 9] Testing concurrent operations...'; END $$;

-- Test 9.1: Insert, update, delete sequence
DO $$
DECLARE
    test_username TEXT := 'concurrent_test';
    found_after_insert INT;
    found_after_update INT;
    found_after_delete INT;
BEGIN
    -- Insert
    INSERT INTO biscuit_test (username, email, status)
    VALUES (test_username, 'concurrent@example.com', 'active');
    
    SET enable_seqscan = OFF;
    SELECT COUNT(*) INTO found_after_insert FROM biscuit_test WHERE username LIKE test_username;
    
    -- Update
    UPDATE biscuit_test SET username = test_username || '_updated' WHERE username = test_username;
    SELECT COUNT(*) INTO found_after_update FROM biscuit_test WHERE username LIKE test_username || '_updated';
    
    -- Delete
    DELETE FROM biscuit_test WHERE username = test_username || '_updated';
    SELECT COUNT(*) INTO found_after_delete FROM biscuit_test WHERE username LIKE test_username || '_updated';
    SET enable_seqscan = ON;
    
    IF found_after_insert = 1 AND found_after_update = 1 AND found_after_delete = 0 THEN
        RAISE NOTICE '[TEST 9.1] ✓ Insert->Update->Delete sequence successful';
    ELSE
        RAISE WARNING '[TEST 9.1] ✗ Sequence failed: insert=%, update=%, delete=%', 
            found_after_insert, found_after_update, found_after_delete;
    END IF;
END $$;

-- ============================================================================
-- TEST 10: Performance Comparison
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '[TEST 10] Performance comparison...'; END $$;

-- Test 10.1: Sequential scan timing
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    result_count INT;
BEGIN
    SET enable_indexscan = OFF;
    SET enable_bitmapscan = OFF;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO result_count FROM biscuit_test WHERE username LIKE '%admin%';
    end_time := clock_timestamp();
    duration := end_time - start_time;
    
    RAISE NOTICE '[TEST 10.1] Sequential scan: % ms (% results)', 
        EXTRACT(MILLISECONDS FROM duration), result_count;
    
    SET enable_seqscan = ON;
END $$;

-- Test 10.2: Index scan timing
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    result_count INT;
BEGIN
    SET enable_seqscan = OFF;
    SET enable_indexscan = ON;
    SET enable_bitmapscan = ON;
    
    start_time := clock_timestamp();
    SELECT COUNT(*) INTO result_count FROM biscuit_test WHERE username LIKE '%admin%';
    end_time := clock_timestamp();
    duration := end_time - start_time;
    
    RAISE NOTICE '[TEST 10.2] Index scan: % ms (% results)', 
        EXTRACT(MILLISECONDS FROM duration), result_count;
    
    SET enable_seqscan = ON;
END $$;

-- ============================================================================
-- FINAL SUMMARY
-- ============================================================================

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Final Index Statistics'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Display final statistics for username index
DO $$
DECLARE
    stats TEXT;
BEGIN
    SELECT biscuit_index_stats('idx_username_biscuit'::regclass::oid) INTO stats;
    RAISE NOTICE '%', stats;
END $$;

DO $$ BEGIN RAISE NOTICE ''; END $$;

-- Display all Biscuit indexes
DO $$
DECLARE
    rec RECORD;
BEGIN
    RAISE NOTICE 'All Biscuit Indexes:';
    FOR rec IN 
        SELECT 
            schema_name,
            index_name,
            table_name,
            column_name,
            index_size
        FROM biscuit_indexes
    LOOP
        RAISE NOTICE '  - %.%: % on %.% (size: %)',
            rec.schema_name, rec.index_name, rec.index_name, 
            rec.table_name, rec.column_name, rec.index_size;
    END LOOP;
END $$;

DO $$ BEGIN RAISE NOTICE ''; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;
DO $$ BEGIN RAISE NOTICE 'Test Suite Complete!'; END $$;
DO $$ BEGIN RAISE NOTICE '========================================'; END $$;

-- Display final table stats
DO $$
DECLARE
    total_rows INT;
    active_rows INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM biscuit_test;
    SELECT COUNT(*) INTO active_rows FROM biscuit_test WHERE status = 'active';
    
    RAISE NOTICE 'Total rows in table: %', total_rows;
    RAISE NOTICE 'Active rows: %', active_rows;
    RAISE NOTICE 'Inactive rows: %', total_rows - active_rows;
END $$;