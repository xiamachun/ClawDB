-- =============================================================================
-- ClawDB SQL Functional Test Suite
-- =============================================================================
-- Run this file against a MySQL 8.0 instance that has the clawdb plugin
-- installed:
--
--   mysql -u root -p < storage/clawdb/test/clawdb_test.sql
--
-- Expected: all SELECT statements return the values shown in the comments.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Setup: install plugin and create test database
-- ---------------------------------------------------------------------------

-- Plugin is loaded via INSTALL PLUGIN before running this script.
-- If not yet installed, run: INSTALL PLUGIN clawdb SONAME 'ha_clawdb.so';
-- INSTALL PLUGIN IF NOT EXISTS clawdb SONAME 'ha_clawdb.so';

DROP DATABASE IF EXISTS clawdb_test;
CREATE DATABASE clawdb_test;
USE clawdb_test;

-- ---------------------------------------------------------------------------
-- 1. Basic table creation
-- ---------------------------------------------------------------------------

CREATE TABLE vec4 (
  id        INT          NOT NULL PRIMARY KEY,
  label     VARCHAR(64)  NOT NULL,
  embedding BLOB         COMMENT 'VECTOR(4)'
) ENGINE=CLAWDB;

-- Verify the table exists and uses CLAWDB engine
SELECT TABLE_NAME, ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'clawdb_test' AND TABLE_NAME = 'vec4';
-- Expected: vec4 | CLAWDB

-- ---------------------------------------------------------------------------
-- 2. Insert vectors using clawdb_to_vector()
-- ---------------------------------------------------------------------------

INSERT INTO vec4 VALUES
  (1, 'a', clawdb_to_vector('[1.0, 0.0, 0.0, 0.0]')),
  (2, 'b', clawdb_to_vector('[0.0, 1.0, 0.0, 0.0]')),
  (3, 'c', clawdb_to_vector('[0.0, 0.0, 1.0, 0.0]')),
  (4, 'd', clawdb_to_vector('[0.0, 0.0, 0.0, 1.0]')),
  (5, 'e', clawdb_to_vector('[1.0, 1.0, 0.0, 0.0]'));

-- Verify row count
SELECT COUNT(*) AS row_count FROM vec4;
-- Expected: 5

-- ---------------------------------------------------------------------------
-- 3. Read back vectors using clawdb_from_vector()
-- ---------------------------------------------------------------------------

SELECT id, label, clawdb_from_vector(embedding) AS vec_str
FROM vec4
ORDER BY id;
-- Expected:
--   1 | a | [1,0,0,0]
--   2 | b | [0,1,0,0]
--   3 | c | [0,0,1,0]
--   4 | d | [0,0,0,1]
--   5 | e | [1,1,0,0]

-- ---------------------------------------------------------------------------
-- 4. L2 distance: vector_distance() default metric
-- ---------------------------------------------------------------------------

-- Distance from [1,0,0,0] to itself should be 0
SELECT vector_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') AS dist
FROM vec4
WHERE id = 1;
-- Expected: 0

-- Distance from [1,0,0,0] to [0,1,0,0] should be 2 (squared L2)
SELECT vector_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') AS dist
FROM vec4
WHERE id = 2;
-- Expected: 2

-- KNN: find 3 nearest to [1,0,0,0] by L2
SELECT id, label,
       vector_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') AS dist
FROM vec4
ORDER BY dist
LIMIT 3;
-- Expected (order by dist asc):
--   1 | a | 0     (exact match)
--   5 | e | 1     (distance = (1-1)^2 + (1-0)^2 = 1)
--   2 | b | 2     (distance = (0-1)^2 + (1-0)^2 = 2)

-- ---------------------------------------------------------------------------
-- 5. Cosine distance: vector_distance() with 'cosine' metric
-- ---------------------------------------------------------------------------

-- Cosine distance from [1,0,0,0] to itself should be 0
SELECT vector_distance(embedding, '[1.0, 0.0, 0.0, 0.0]', 'cosine') AS dist
FROM vec4
WHERE id = 1;
-- Expected: 0

-- Cosine distance from [1,0,0,0] to [0,1,0,0] should be 1 (orthogonal)
SELECT vector_distance(embedding, '[1.0, 0.0, 0.0, 0.0]', 'cosine') AS dist
FROM vec4
WHERE id = 2;
-- Expected: 1

-- KNN by cosine distance: [1,1,0,0] is closest to [1,0,0,0] and [0,1,0,0]
SELECT id, label,
       vector_distance(embedding, '[1.0, 1.0, 0.0, 0.0]', 'cosine') AS dist
FROM vec4
ORDER BY dist
LIMIT 3;
-- Expected:
--   5 | e | 0     (exact match [1,1,0,0])
--   1 | a | ~0.29 (cosine distance to [1,0,0,0])
--   2 | b | ~0.29 (cosine distance to [0,1,0,0])

-- ---------------------------------------------------------------------------
-- 6. UPDATE: modify a vector row
-- ---------------------------------------------------------------------------

UPDATE vec4
SET embedding = clawdb_to_vector('[0.5, 0.5, 0.5, 0.5]')
WHERE id = 5;

SELECT id, clawdb_from_vector(embedding) AS vec_str
FROM vec4
WHERE id = 5;
-- Expected: 5 | [0.5,0.5,0.5,0.5]

-- Verify count unchanged after update
SELECT COUNT(*) AS row_count FROM vec4;
-- Expected: 5

-- ---------------------------------------------------------------------------
-- 7. DELETE: remove a row
-- ---------------------------------------------------------------------------

DELETE FROM vec4 WHERE id = 4;

SELECT COUNT(*) AS row_count FROM vec4;
-- Expected: 4

SELECT id FROM vec4 ORDER BY id;
-- Expected: 1, 2, 3, 5

-- ---------------------------------------------------------------------------
-- 8. KNN after update and delete
-- ---------------------------------------------------------------------------

SELECT id, label,
       vector_distance(embedding, '[0.5, 0.5, 0.5, 0.5]') AS dist
FROM vec4
ORDER BY dist
LIMIT 3;
-- Expected: row 5 (updated to [0.5,0.5,0.5,0.5]) should be first with dist=0

-- ---------------------------------------------------------------------------
-- 9. NULL handling
-- ---------------------------------------------------------------------------

INSERT INTO vec4 VALUES (6, 'null_vec', NULL);

SELECT vector_distance(embedding, '[1.0, 0.0, 0.0, 0.0]') AS dist
FROM vec4
WHERE id = 6;
-- Expected: NULL

SELECT clawdb_from_vector(embedding) AS vec_str
FROM vec4
WHERE id = 6;
-- Expected: NULL

-- ---------------------------------------------------------------------------
-- 10. Larger dimension test (128-dim)
-- ---------------------------------------------------------------------------

CREATE TABLE vec128 (
  id        INT  NOT NULL PRIMARY KEY,
  embedding BLOB COMMENT 'VECTOR(128)'
) ENGINE=CLAWDB;

-- Build a 128-dim zero vector string
SET @zero128 = CONCAT('[', REPEAT('0.0,', 127), '0.0]');
SET @one128  = CONCAT('[1.0,', REPEAT('0.0,', 126), '0.0]');

INSERT INTO vec128 VALUES
  (1, clawdb_to_vector(@zero128)),
  (2, clawdb_to_vector(@one128));

-- Distance from zero to one-hot should be 1 (squared L2)
SELECT vector_distance(embedding, @one128) AS dist
FROM vec128
WHERE id = 1;
-- Expected: 1

SELECT vector_distance(embedding, @one128) AS dist
FROM vec128
WHERE id = 2;
-- Expected: 0

-- ---------------------------------------------------------------------------
-- 11. TRUNCATE (delete_all_rows)
-- ---------------------------------------------------------------------------

TRUNCATE TABLE vec4;

SELECT COUNT(*) AS row_count FROM vec4;
-- Expected: 0

-- ---------------------------------------------------------------------------
-- 12. DROP TABLE
-- ---------------------------------------------------------------------------

DROP TABLE vec4;
DROP TABLE vec128;

-- Verify tables are gone
SELECT COUNT(*) AS remaining_tables
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'clawdb_test';
-- Expected: 0

SELECT 'ClawDB test suite completed successfully.' AS result;
