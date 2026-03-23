# ClawDB — MySQL Vector Storage Engine

ClawDB is a **plugin storage engine for MySQL** that adds native vector
embedding support.  It lets you store high-dimensional float vectors in BLOB
columns, compute L2 / cosine distances via SQL functions, and perform
approximate K-nearest-neighbor (KNN) search using an in-memory HNSW index —
all without any modification to the MySQL server source code.

---

## Verified MySQL Versions

| MySQL Version | C++ Standard | Build Status |
|---|---|---|
| **5.7.44** | C++14 | ✅ Pass |
| **8.0.20** | C++14 | ✅ Pass |
| **8.0.45** | C++17 | ✅ Pass |
| **8.4.7**  | C++20 | ✅ Pass |

> **Note:** A single copy of the ClawDB source code compiles and runs on all
> four versions above.  Version-specific differences are handled by the
> compatibility layer in `clawdb_compat.h`.

---

## Features

| Feature | Details |
|---|---|
| Vector storage | BLOB column with binary encoding: `uint32 dim` + `float32[dim]` |
| Max dimensions | 16 000 (aligned with PGVector `VECTOR_MAX_DIM`) |
| Distance metrics | L2 squared distance, Cosine distance |
| KNN search | Brute-force full-scan via `ORDER BY vector_distance() LIMIT N` |
| HNSW index | In-memory HNSW with customizable parameters via `CREATE INDEX ... COMMENT` |
| Vector index creation | `CREATE INDEX ... COMMENT 'HNSW(metric=cosine, m=32, ...)'` syntax |
| SQL functions | `vector_distance()`, `clawdb_to_vector()`, `clawdb_from_vector()` |
| Multi-version support | Single codebase for MySQL 5.7 / 8.0 / 8.4 |
| Zero server changes | Pure plugin — `INSTALL PLUGIN` is all you need |

---

## Quick Start

### 1. Build ClawDB

Copy the `storage/clawdb/` directory into the MySQL source tree, then build:

```bash
# Step 1: Copy ClawDB source into the MySQL source tree
cp -r /path/to/clawdb  /path/to/mysql-src/storage/clawdb

# Step 2: Run cmake (first time only, or after adding new source files)
cd /path/to/mysql-build
cmake /path/to/mysql-src \
      -DCMAKE_BUILD_TYPE=Debug \
      -DWITH_BOOST=/path/to/boost \
      -DDOWNLOAD_BOOST=1 \
      -DWITH_SSL=system \
      -DWITH_UNIT_TESTS=OFF

# Step 3: Build the clawdb plugin only (fast incremental build)
make -j$(nproc) clawdb
```

#### Build Notes

- **First build** requires a full `cmake` run so that the build system
  discovers the new `storage/clawdb/` directory and generates its Makefile.
- **Subsequent builds** after editing `.cc`/`.h` files only need `make clawdb`.
- The C++ standard is selected automatically by `CMakeLists.txt` based on
  `MYSQL_VERSION_ID`:

  | MySQL Version | C++ Standard |
  |---|---|
  | 5.7.x | C++14 (overrides default gnu++03) |
  | 8.0.0 – 8.0.24 | C++14 |
  | 8.0.25 – 8.0.x | C++17 |
  | 8.4+ | C++20 |

- On MySQL 5.7, the build also adds `-fabi-version=0` to avoid ABI
  compatibility warnings with the older default ABI.

### 2. Install the Plugin

```sql
-- After starting mysqld, install the plugin once:
INSTALL PLUGIN clawdb SONAME 'ha_clawdb.so';

-- Verify it is loaded:
SHOW PLUGINS;
-- You should see: clawdb | ACTIVE | STORAGE ENGINE | ha_clawdb.so | GPL
```

#### MySQL 5.7: Manual UDF Registration

On MySQL 5.7, the component service registry is not available, so UDFs must
be registered manually after loading the plugin:

```sql
CREATE FUNCTION vector_distance    RETURNS REAL   SONAME 'ha_clawdb.so';
CREATE FUNCTION clawdb_to_vector   RETURNS STRING SONAME 'ha_clawdb.so';
CREATE FUNCTION clawdb_from_vector RETURNS STRING SONAME 'ha_clawdb.so';
```

On MySQL 8.0+, UDFs are registered automatically during plugin init.

### 3. Create a Vector Table

```sql
CREATE TABLE vec_tbl (
  id        INT          NOT NULL PRIMARY KEY,
  label     VARCHAR(128) NOT NULL,
  embedding BLOB         COMMENT 'VECTOR(128)'   -- 128-dimensional float32 vector
) ENGINE=CLAWDB;
```

### 4. Create a Vector Index (Optional)

ClawDB supports creating an HNSW (Hierarchical Navigable Small World) index
on the vector column to customize the approximate nearest-neighbor search
behavior.  The index parameters are passed via the standard `COMMENT` clause:

```sql
-- Create an HNSW index with custom parameters
CREATE INDEX vec_idx ON vec_tbl (embedding(768))
  COMMENT 'HNSW(metric=cosine, m=32, ef_construction=200, ef_search=100)';
```

**Syntax:** `HNSW([key=value, ...])`

| Parameter | Default | Range | Description |
|---|---|---|---|
| `metric` | `l2` | `l2`, `euclidean`, `cosine` | Distance metric |
| `m` | 16 | [2, 100] | Max connections per node |
| `ef_construction` | 64 | [4, 1000] | Build-time search width |
| `ef_search` | 40 | [1, 1000] | Query-time search width |

All parameters are optional.  If no `COMMENT` is specified, HNSW defaults
are used.  The prefix length (e.g. `768`) is required by MySQL for BLOB
indexes but is ignored by ClawDB — the full vector is always indexed.

```sql
-- Minimal form: HNSW index with all defaults (L2 metric)
CREATE INDEX vec_idx ON vec_tbl (embedding(768));

-- Inline index in CREATE TABLE
CREATE TABLE vec_tbl (
  id        INT          NOT NULL PRIMARY KEY,
  embedding BLOB         NOT NULL COMMENT 'VECTOR(128)',
  INDEX vec_idx (embedding(512)) COMMENT 'HNSW(metric=cosine)'
) ENGINE=CLAWDB;
```

> **Note:** Only one vector index per table is supported.  The index is
> persisted to a `.hnsw` binary file and loaded on table open (rebuilt from
> the data file if the index file is missing or stale).

### 5. Insert Vectors

```sql
-- Use clawdb_to_vector() to convert the string representation to binary BLOB
INSERT INTO vec_tbl VALUES
  (1, 'cat',  clawdb_to_vector('[0.1, 0.2, 0.3, ...]')),
  (2, 'dog',  clawdb_to_vector('[0.4, 0.5, 0.6, ...]')),
  (3, 'bird', clawdb_to_vector('[0.7, 0.8, 0.9, ...]'));
```

### 6. KNN Search

```sql
-- Find the 5 nearest neighbors by L2 distance (default)
SELECT id, label,
       vector_distance(embedding, '[0.15, 0.25, 0.35, ...]') AS dist
FROM vec_tbl
ORDER BY dist
LIMIT 5;

-- Find the 5 nearest neighbors by cosine distance
SELECT id, label,
       vector_distance(embedding, '[0.15, 0.25, 0.35, ...]', 'cosine') AS dist
FROM vec_tbl
ORDER BY dist
LIMIT 5;
```

### 7. Read Back Vectors as Strings

```sql
SELECT id, clawdb_from_vector(embedding) AS vec_str FROM vec_tbl;
```

---

## SQL Function Reference

### `vector_distance(vec_col, query_str [, metric])`

Computes the distance between a stored vector BLOB and a query vector string.

| Parameter | Type | Description |
|---|---|---|
| `vec_col` | BLOB | Column containing a ClawDB binary vector |
| `query_str` | VARCHAR | Vector string, e.g. `'[0.1, 0.2, 0.3]'` |
| `metric` | VARCHAR | Optional: `'l2'` (default) or `'cosine'` |

Returns `DOUBLE`.  Returns `NULL` if either argument is `NULL` or on dimension
mismatch.

**L2 distance** returns the *squared* Euclidean distance (no `sqrt`), which is
monotone with true L2 and sufficient for ranking.

**Cosine distance** = `1 - cosine_similarity`, in the range `[0, 2]`.

---

### `clawdb_to_vector(vector_string)`

Converts a vector string `'[f0, f1, ..., fn]'` to the binary BLOB format used
by ClawDB for storage.  Use this when inserting vector data.

Returns `BLOB`.

---

### `clawdb_from_vector(blob_column)`

Converts a ClawDB binary vector BLOB back to the human-readable string
`'[f0, f1, ..., fn]'`.

Returns `VARCHAR`.

---

## Architecture

```
MySQL SQL Layer
      │
      ▼
ha_clawdb (handler)          ← storage/clawdb/ha_clawdb.{h,cc}
      │                        Handler lifecycle, DML, scan, KNN dispatch
      │
      ├── ClawdbShare        ← clawdb_share.{h,cc}
      │     Global share cache (one per table path)
      │     HNSW COMMENT parser (metric, m, ef_construction, ef_search)
      │
      ├── Row Serde          ← clawdb_serde.{h,cc}
      │     serialize_row / deserialize_row (MySQL record ↔ byte stream)
      │     BLOB field helpers, vector extraction
      │
      ├── ClawdbTableStore   ← clawdb_store.{h,cc}
      │     Flat file: <db>/<table>.clawdb
      │     Layout: [FileHeader][RowHeader+RowData] ...
      │     Large-file safe (fseeko/ftello)
      │
      ├── ClawdbHnswIndex    ← clawdb_hnsw.{h,cc}
      │     In-memory HNSW graph with binary persistence (.hnsw file)
      │     Ported from PGVector, pure C++ STL
      │     RAII resource management (FileGuard)
      │
      ├── ClawdbVector       ← clawdb_vec.{h,cc}
      │     Parse/serialize '[f0,f1,...]' ↔ binary blob
      │     L2 and cosine distance functions
      │
      ├── UDFs               ← clawdb_udf.{h,cc}
      │     vector_distance(), clawdb_to_vector(), clawdb_from_vector()
      │     8.0+: auto-registered via plugin registry service
      │     5.7:  manual CREATE FUNCTION after INSTALL PLUGIN
      │
      └── Compat Layer       ← clawdb_compat.h
            Centralizes all #if MYSQL_VERSION_ID conditionals
            (incl. down_cast<> fallback for 5.7)
            so the rest of the code is version-agnostic
```

### Data File Format

```
Offset  Size  Field
------  ----  -----
0       4     magic = 0xC1ADB001
4       2     version = 1
6       2     reserved = 0
8       8     row_count (live rows)

Per row:
0       1     flags (0x01=alive, 0x00=deleted)
1       4     data_length
5       N     MySQL row buffer (raw record[0] bytes)
```

### HNSW Index

The HNSW index is persisted to a binary `.hnsw` file alongside the data file.
On `open()`, ClawDB first attempts to load the persisted index; if the file
is missing or invalid, the index is rebuilt by scanning the data file.  The
index is updated incrementally on `write_row()`, `update_row()`, and
`delete_row()`, and saved back to disk when the last handler closes (if dirty)
or after a full rebuild.

**Persistence format** (native byte order):

```
[HnswFileHeader 40 bytes]
For each node:
  [uint64  node_id]
  [int32   level]
  [uint32  vector_dim]
  [float × dim]                  (vector data)
  For each layer 0..level:
    [uint32  neighbor_count]
    [uint64 × neighbor_count]    (neighbor node_ids)
```

Default parameters (aligned with PGVector defaults):

| Parameter | Default | Customizable via |
|---|---|---|
| M | 16 | `CREATE INDEX ... COMMENT 'HNSW(m=32)'` |
| ef_construction | 64 | `CREATE INDEX ... COMMENT 'HNSW(ef_construction=200)'` |
| ef_search | 40 | `CREATE INDEX ... COMMENT 'HNSW(ef_search=100)'` |
| metric | L2 | `CREATE INDEX ... COMMENT 'HNSW(metric=cosine)'` |

---

## Test Suite

### clawdb_test.sql

A comprehensive SQL functional test suite located at `test/clawdb_test.sql`.
It covers 14 test sections:

| # | Test Section | What It Verifies |
|---|---|---|
| 1 | Basic table creation | `CREATE TABLE ... ENGINE=CLAWDB` |
| 2 | Insert vectors | `clawdb_to_vector()` UDF, multi-row INSERT |
| 3 | Read back vectors | `clawdb_from_vector()` UDF |
| 4 | L2 distance | `vector_distance()` default metric, KNN ordering |
| 5 | Cosine distance | `vector_distance(..., 'cosine')` |
| 6 | UPDATE | Modify vector data, verify row count unchanged |
| 7 | DELETE | Remove rows, verify count and remaining IDs |
| 8 | KNN after mutations | Correct ordering after UPDATE + DELETE |
| 9 | NULL handling | NULL vector column, NULL distance result |
| 10 | Vector index creation | `CREATE INDEX ... COMMENT 'HNSW(...)'` with custom params |
| 11 | Large dimensions | 128-dim vectors, distance correctness |
| 12 | TRUNCATE | `delete_all_rows()` path |
| 13 | DROP TABLE | Clean removal of table and data file |
| 14 | Cleanup | Drop test database |

**Running the test:**

```bash
# MySQL 8.0+ (UDFs auto-registered)
mysql -u root -p < test/clawdb_test.sql

# MySQL 5.7 (register UDFs first, then run)
mysql -u root -p -e "
  CREATE FUNCTION vector_distance    RETURNS REAL   SONAME 'ha_clawdb.so';
  CREATE FUNCTION clawdb_to_vector   RETURNS STRING SONAME 'ha_clawdb.so';
  CREATE FUNCTION clawdb_from_vector RETURNS STRING SONAME 'ha_clawdb.so';
"
mysql -u root -p < test/clawdb_test.sql
```

Expected final output: `ClawDB test suite completed successfully.`

---

## Version Compatibility Design

All MySQL version differences are centralized in **`clawdb_compat.h`**.
The rest of the codebase is version-agnostic.

Key differences handled by the compat layer:

| Difference | 5.7 | 8.0+ |
|---|---|---|
| Include headers | `my_global.h`, `mysql_com.h` | `my_inttypes.h`, component services |
| `dd::Table` parameter | Not present | Added to open/create/delete/rename |
| `Field::field_ptr()` | `field->ptr` (< 8.0.25) | `field->field_ptr()` (≥ 8.0.25) |
| `Field_blob::get_blob_data()` | `get_ptr(&ptr)` | `get_blob_data()` |
| `ha_statistic_increment` | `SSV::` | `System_status_var::` |
| `bas_ext()` | Required (pure virtual) | Removed from base class |
| `HA_REC_NOT_IN_SEQ` | Present | Removed |
| Handler factory | 3 params | 4 params (+ `partitioned`) |
| UDF registration | Manual `CREATE FUNCTION` | Component registry service |
| `file_extensions` | Not present | Required on handlerton |
| `down_cast<>` | Fallback via `static_cast` | Provided by `my_dbug.h` |
| Plugin descriptor | No "Check Uninstall" slot | Has "Check Uninstall" slot |

---

## Limitations & Roadmap

| Item | Status |
|---|---|
| Multi-version support (5.7/8.0/8.4) | ✅ Implemented |
| HNSW in-memory index | ✅ Implemented |
| L2 + cosine distance | ✅ Implemented |
| Brute-force KNN scan | ✅ Implemented |
| Customizable HNSW parameters | ✅ Implemented (`CREATE INDEX ... COMMENT 'HNSW(...)'`) |
| Persistent HNSW index (.hnsw file) | ✅ Implemented |
| IVFFlat index | 🔲 Planned |
| Concurrent write safety | 🔲 Planned (per-table mutex) |
| SIMD distance acceleration | 🔲 Planned |
| `VECTOR(n)` native type | 🔲 Planned (requires MySQL type system extension) |

---

## Contributing

ClawDB is designed to be a clean, minimal, well-documented storage engine that
can serve as a reference implementation for vector search in MySQL.  Patches,
bug reports, and feature requests are welcome.

---

## License

GNU General Public License, version 2.0.  See the `LICENSE` file for details.
