-- pg_biscuit--1.0.sql
-- SQL installation script for Biscuit Index Access Method
-- PostgreSQL 15+ compatible with full CRUD support
--
-- Features:
-- - O(1) lazy deletion with tombstones
-- - Incremental insert/update
-- - Automatic slot reuse
-- - Full VACUUM integration
-- - Optimized pattern matching for LIKE queries

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_biscuit" to load this file. \quit

-- ==================== CORE INDEX ACCESS METHOD ====================

-- Create the index access method handler function
CREATE FUNCTION biscuit_handler(internal)
RETURNS index_am_handler
AS 'MODULE_PATHNAME', 'biscuit_handler'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_handler(internal) IS 
'Index access method handler for Biscuit indexes - provides callbacks for index operations';

-- Create the Biscuit index access method
CREATE ACCESS METHOD biscuit TYPE INDEX HANDLER biscuit_handler;

COMMENT ON ACCESS METHOD biscuit IS 
'Biscuit index access method: High-performance pattern matching for LIKE queries with O(1) deletion';

-- ==================== OPERATOR SUPPORT ====================

-- Support function for LIKE operator optimization
CREATE FUNCTION biscuit_like_support(internal)
RETURNS bool
AS 'MODULE_PATHNAME', 'biscuit_like_support'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_like_support(internal) IS
'Support function that tells the planner Biscuit can handle LIKE pattern matching';

-- ==================== DIAGNOSTIC FUNCTIONS ====================

-- Function to get index statistics and health information
CREATE FUNCTION biscuit_index_stats(oid)
RETURNS text
AS 'MODULE_PATHNAME', 'biscuit_index_stats'
LANGUAGE C STRICT;

COMMENT ON FUNCTION biscuit_index_stats(oid) IS
'Returns detailed statistics for a Biscuit index including CRUD counts, tombstones, and memory usage.
Usage: SELECT biscuit_index_stats(''index_name''::regclass::oid);';

-- ==================== OPERATOR CLASSES ====================

-- Default operator class for text types (text, varchar, bpchar)
CREATE OPERATOR CLASS biscuit_text_ops
DEFAULT FOR TYPE text USING biscuit AS
    OPERATOR 1 ~~ (text, text),          -- LIKE operator
    OPERATOR 2 ~~* (text, text),         -- ILIKE operator (case-insensitive)
    FUNCTION 1 biscuit_like_support(internal);

COMMENT ON OPERATOR CLASS biscuit_text_ops USING biscuit IS
'Default operator class for Biscuit indexes on text columns - supports LIKE and ILIKE queries';

-- ==================== HELPER VIEWS ====================

-- View to show all Biscuit indexes in the database
CREATE VIEW biscuit_indexes AS
SELECT
    n.nspname AS schema_name,
    c.relname AS index_name,
    t.relname AS table_name,
    a.attname AS column_name,
    pg_size_pretty(pg_relation_size(c.oid)) AS index_size,
    c.oid AS index_oid
FROM
    pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    JOIN pg_index i ON i.indexrelid = c.oid
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
WHERE
    am.amname = 'biscuit'
    AND c.relkind = 'i'
ORDER BY
    n.nspname, c.relname;

COMMENT ON VIEW biscuit_indexes IS
'Shows all Biscuit indexes in the current database with their tables, columns, and sizes';

-- ==================== USAGE EXAMPLES ====================

-- Example queries (commented out - for documentation)
/*

-- Basic index creation
CREATE INDEX idx_username ON users USING biscuit(username);
CREATE INDEX idx_email ON users USING biscuit(email);

-- Case-insensitive index (use LOWER())
CREATE INDEX idx_username_lower ON users USING biscuit(LOWER(username));

-- Partial index (only active users)
CREATE INDEX idx_active_users ON users USING biscuit(username)
WHERE status = 'active';

-- Query examples that use the index
SELECT * FROM users WHERE username LIKE 'john%';        -- Prefix
SELECT * FROM users WHERE email LIKE '%@gmail.com';     -- Suffix
SELECT * FROM users WHERE username LIKE '%admin%';      -- Contains
SELECT * FROM users WHERE username LIKE 'user_1%5';     -- Complex

-- Case-insensitive query (requires lowercase index)
SELECT * FROM users WHERE LOWER(username) LIKE '%admin%';

-- Get index statistics
SELECT biscuit_index_stats('idx_username'::regclass::oid);

-- View all Biscuit indexes
SELECT * FROM biscuit_indexes;

-- List indexes on a specific table
SELECT * FROM biscuit_indexes WHERE table_name = 'users';

-- Force index usage for testing
SET enable_seqscan = off;
EXPLAIN ANALYZE SELECT * FROM users WHERE username LIKE '%test%';
SET enable_seqscan = on;

-- Maintenance
VACUUM ANALYZE users;           -- Clean up tombstones
REINDEX INDEX idx_username;     -- Rebuild if needed

*/

-- ==================== GRANT PERMISSIONS ====================

-- Grant execute on functions to public (read-only diagnostic function)
GRANT EXECUTE ON FUNCTION biscuit_index_stats(oid) TO PUBLIC;

-- ==================== VERSION INFO ====================

-- Store extension version information
CREATE TABLE IF NOT EXISTS biscuit_version (
    version text PRIMARY KEY,
    installed_at timestamptz DEFAULT now(),
    description text
);

INSERT INTO biscuit_version (version, description) VALUES
    ('1.0', 'Initial release with full CRUD support, O(1) deletion, and automatic cleanup');

COMMENT ON TABLE biscuit_version IS
'Version history for the Biscuit IAM extension';