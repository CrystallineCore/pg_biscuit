# pg_biscuit

A PostgreSQL Index Access Method (IAM) for high-performance pattern matching on text columns. Biscuit indexes are specifically designed to accelerate `LIKE` queries with arbitrary wildcards.

## Overview

`pg_biscuit` implements a novel indexing approach that maintains character position information using compressed bitmaps (Roaring bitmaps). Unlike traditional B-tree or trigram (pg_trgm) indexes, Biscuit can efficiently handle complex pattern queries including prefix, suffix, substring, and multi-part patterns.

### Key Features

- **Pattern-optimized indexing**: Accelerates `LIKE` queries with `%` and `_` wildcards
- **Full CRUD support**: Efficient insert, update, and delete operations with O(1) lazy deletion
- **Memory-resident architecture**: Index lives in shared memory for fast access
- **Compressed bitmaps**: Uses Roaring bitmaps for space-efficient storage
- **Automatic cleanup**: Tombstone-based deletion with batch cleanup on threshold
- **PostgreSQL 15+ compatible**: Implements standard IAM interface

## Installation

### Prerequisites

- PostgreSQL 16 or later
- C compiler (gcc or clang)
- Optional: Roaring bitmap library for better compression

### Build from Source

```bash
# Clone or download the extension
cd pg_biscuit

# Build and install
make
sudo make install

# Enable in your database
psql -d your_database -c "CREATE EXTENSION pg_biscuit;"
```

## Usage

### Creating a Biscuit Index

```sql
-- Basic index on a text column
CREATE INDEX idx_username ON users USING biscuit(username);

-- Index on varchar or other text types
CREATE INDEX idx_email ON emails USING biscuit(email_address);

-- Partial index (only index active records)
CREATE INDEX idx_active_usernames ON users USING biscuit(username)
WHERE status = 'active';

-- Case-insensitive index (use LOWER function)
CREATE INDEX idx_username_lower ON users USING biscuit(LOWER(username));
```

### Query Examples

Biscuit indexes automatically accelerate these query patterns:

```sql
-- Prefix match: 'john%'
SELECT * FROM users WHERE username LIKE 'john%';

-- Suffix match: '%@gmail.com'
SELECT * FROM users WHERE email LIKE '%@gmail.com';

-- Substring match: '%admin%'
SELECT * FROM users WHERE username LIKE '%admin%';

-- Complex pattern: '%a%b%c%'
SELECT * FROM logs WHERE message LIKE '%error%database%';

-- Exact match: 'johndoe'
SELECT * FROM users WHERE username LIKE 'johndoe';

-- Case-insensitive (requires lowercase index)
SELECT * FROM users WHERE LOWER(username) LIKE '%admin%';
```

### Index Maintenance

```sql
-- View all Biscuit indexes in database
SELECT * FROM biscuit_indexes;

-- Get detailed statistics for an index
SELECT biscuit_index_stats('idx_username'::regclass::oid);

-- Rebuild index if needed
REINDEX INDEX idx_username;

-- Clean up deleted records
VACUUM ANALYZE users;
```

### Diagnostic Output Example

```sql
SELECT biscuit_index_stats('idx_username'::regclass::oid);
```

```
Biscuit Index Statistics (FULLY OPTIMIZED)
==========================================
Index: idx_username
Active records: 995432
Total slots: 1000000
Free slots: 156
Tombstones: 0
Max length: 64
------------------------
CRUD Statistics:
  Inserts: 1000000
  Updates: 0
  Deletes: 4568
------------------------
Active Optimizations:
  ✓ 1. Skip wildcard intersections
  ✓ 2. Early termination on empty
  ✓ 3. Avoid redundant copies
  ✓ 4. Optimized single-part patterns
  ✓ 5. Skip unnecessary length ops
  ✓ 6. TID sorting for sequential I/O
  ✓ 7. Batch TID insertion
  ✓ 8. Direct bitmap iteration
  ✓ 9. Parallel bitmap scan support
  ✓ 10. Batch cleanup on threshold
```

## Performance Characteristics

### Benchmark Results (1M UUID records)

The included benchmark script (`tests/benchmark.sql`) tests various pattern types against Sequential Scan, B-Tree, pg_trgm (GIN), and Biscuit indexes:

**Overall Performance (Geometric Mean):**
- Biscuit: 4.33 ms
- pg_trgm: 5.62 ms
- Sequential Scan: 5.50 ms
- B-Tree: 2.34 ms (prefix-only queries)

**Category Winners:**
- **Exact Match**: B-Tree (0.22 ms) - Best for single lookups
- **Prefix Match**: pg_trgm/Biscuit/Sequential tied (~2.1-2.3 ms)
- **Suffix Match**: pg_trgm (3.02 ms) - Slight edge over Biscuit (3.30 ms)
- **Contains Match**: Biscuit (3.60 ms) - Outperforms pg_trgm (3.79 ms)
- **Complex Patterns**: Biscuit (202.4 ms) - Better than pg_trgm (211.3 ms)
- **Case Sensitivity**: Biscuit (0.70 ms) - Fastest for case-specific queries

**Key Findings:**

1. **Biscuit excels at**: Contains queries (`%text%`), complex multi-part patterns, and case-sensitive searches
2. **pg_trgm excels at**: Suffix queries and some prefix scenarios
3. **B-Tree excels at**: Exact matches and simple prefix queries
4. **Performance is workload-dependent**: No single index type dominates all scenarios

### When to Use Biscuit

**Good Use Cases:**
- Frequent substring searches (`%keyword%`)
- Complex pattern queries with multiple parts
- Mixed prefix/suffix/contains patterns
- Case-sensitive pattern matching
- Moderate to high cardinality text columns

**Not Recommended For:**
- Simple equality checks (use B-Tree)
- Primarily prefix-only searches (B-Tree may be sufficient)
- Very low selectivity queries (>50% of rows)
- Full-text search (use PostgreSQL's FTS instead)

### Index Size

From benchmarks on 1M records:
- Biscuit: 0 bytes (memory-resident, not persisted)
- pg_trgm (GIN): 132 MB
- B-Tree: 56 MB

**Note**: Biscuit is currently a memory-resident index. It rebuilds from the heap on database restart. This trades persistence for query performance.

## Architecture

### Memory-Resident Design

Biscuit indexes are stored entirely in PostgreSQL's shared memory:

1. **Index Build**: Scans heap once during `CREATE INDEX`
2. **Storage**: Lives in index relation's `rd_amcache`
3. **Persistence**: Not written to disk; rebuilds on restart
4. **Updates**: Maintained incrementally via INSERT/UPDATE/DELETE hooks

### Data Structures

- **Position Index**: Character → Position → Bitmap of record IDs
- **Negative Index**: Character → Negative offset → Bitmap (for suffix queries)
- **Length Bitmaps**: Precomputed bitmaps for length-based filtering
- **Tombstones**: Lazy deletion with bitmap tracking
- **Roaring Bitmaps**: Compressed bitmap representation

### Query Optimization

The engine includes several optimizations:

1. **Wildcard Skipping**: Only intersects concrete characters, skips `_`
2. **Early Termination**: Stops on empty intersection
3. **Single-Part Fast Path**: Avoids recursion for simple patterns
4. **TID Sorting**: Orders results for sequential heap access
5. **Batch Operations**: Bulk bitmap operations for better performance

## Limitations

1. **Memory-Resident**: Index rebuilds on database restart (not persisted to disk)
2. **Single Column**: Only supports one indexed column
3. **Max String Length**: Limited to 256 characters (configurable via `MAX_POSITIONS`)
4. **Case Sensitivity**: Case-insensitive searches require function index with `LOWER()`
5. **No Full-Text Search**: Not a replacement for PostgreSQL's text search features

## Configuration

No configuration is required. The extension automatically:
- Allocates memory in the index context
- Performs cleanup when tombstones reach 1000 (configurable via `TOMBSTONE_CLEANUP_THRESHOLD`)
- Rebuilds length bitmaps as needed

## Development

### Running Benchmarks

```bash
# Load test data and run comprehensive benchmarks
psql -d test_db -f benchmark.sql

# Results include:
# - 7 test categories (exact, prefix, suffix, contains, complex, selectivity, case)
# - 112+ individual test runs
# - Statistical analysis and performance comparisons
```

### Code Structure

- `pg_biscuit.c`: Main IAM implementation
- `pg_biscuit--1.0.sql`: SQL installation script
- `benchmark.sql`: Comprehensive benchmark suite

## Contributing

This is an academic/research project demonstrating a novel indexing approach. Contributions are welcome:

- Bug reports and fixes
- Performance improvements
- Documentation enhancements
- Benchmark additions

## License

PostgreSQL License (similar to BSD/MIT)

## Acknowledgments

- Uses Roaring bitmap library (optional dependency)
- Implements PostgreSQL Index Access Method interface

## Disclaimer

**This is experimental software.** While functional, it is not recommended for production use without thorough testing in your specific environment. The memory-resident architecture means indexes must rebuild on restart, which may not be suitable for all workloads.
## Contributors

BISCUIT is developed and maintained by [Sivaprasad Murali](https://linkedin.com/in/sivaprasad-murali) .


## Support and Contact


**Issues:** https://github.com/crystallinecore/pg_biscuit/issues

**Discussions:** https://github.com/crystallinecore/pg_biscuit/discussions

##

**When pg_trgm feels half-baked, grab a pg_BISCUIT 🍪**

---