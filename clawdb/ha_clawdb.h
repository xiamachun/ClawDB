/* Copyright (c) 2024, 2025, ClawDB Authors.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file storage/clawdb/ha_clawdb.h
  @brief
  ClawDB storage engine handler class definition.

  ClawDB is a MySQL plugin storage engine that provides native vector
  support.  Vectors are stored as BLOB columns with a binary encoding
  (4-byte uint32 dimension + N float32 values).  The engine supports:

    - CREATE TABLE with BLOB columns used as VECTOR(n) fields
    - INSERT / UPDATE / DELETE of rows containing vector data
    - Full-table scan with ORDER BY vector_distance() for KNN search
    - In-memory HNSW index for accelerated approximate KNN search

  Usage example:
  @code
    CREATE TABLE vec_tbl (
      id INT PRIMARY KEY,
      embedding BLOB COMMENT 'VECTOR(128)'
    ) ENGINE=CLAWDB;

    INSERT INTO vec_tbl VALUES (1, clawdb_to_vector('[0.1,0.2,...]'));

    SELECT id, vector_distance(embedding, '[0.1,0.2,...]') AS dist
    FROM vec_tbl
    ORDER BY dist
    LIMIT 10;
  @endcode

  @see
  /sql/handler.h and /storage/clawdb/ha_clawdb.cc
*/

#ifndef STORAGE_CLAWDB_HA_CLAWDB_H
#define STORAGE_CLAWDB_HA_CLAWDB_H

#include "clawdb_compat.h"
#include "clawdb_serde.h"
#include "clawdb_share.h"
#include "clawdb_udf.h"
#include "my_base.h"
#include "sql/handler.h"
#include "thr_lock.h"

#include <cstddef>
#include <string>
#include <vector>

/* -----------------------------------------------------------------------
   ha_clawdb: the storage engine handler
   ----------------------------------------------------------------------- */

/**
  ClawDB storage engine handler.

  Implements the MySQL handler interface for the CLAWDB storage engine.
  Each open table instance has one ha_clawdb object; multiple instances
  share a single ClawdbShare.
*/
class ha_clawdb : public handler {
 public:
  ha_clawdb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_clawdb() override = default;

  /* ---- Engine identification ---- */

  const char *table_type() const override { return "CLAWDB"; }

  CLAWDB_BAS_EXT_OVERRIDE
  CLAWDB_INDEX_ALGORITHM_OVERRIDES

  /* ---- Capability flags ---- */

  ulonglong table_flags() const override {
    return HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE |
           HA_NO_TRANSACTIONS | HA_CAN_INDEX_BLOBS |
           HA_NULL_IN_KEY | CLAWDB_TABLE_FLAGS_EXTRA;
  }

  ulong index_flags(uint /*inx*/, uint /*part*/,
                    bool /*all_parts*/) const override {
    return 0;
  }

  uint max_supported_record_length() const override {
    return HA_MAX_REC_LENGTH;
  }

  /* Allow up to 2 keys: one PRIMARY KEY on integer columns and one
     secondary index on the BLOB (vector) column for HNSW configuration.
     ClawDB does not use the key for lookups; full-table scan is used
     for all reads.  The key metadata is stored by MySQL; the HNSW
     parameters are extracted from the index COMMENT during create/open. */
  uint max_supported_keys() const override { return 2; }
  uint max_supported_key_parts() const override { return 1; }
  uint max_supported_key_length() const override { return 3072; }

  double scan_time() override {
    return static_cast<double>(stats.records + stats.deleted) / 20.0 + 10.0;
  }

  double read_time(uint /*index*/, uint /*ranges*/, ha_rows rows) override {
    return static_cast<double>(rows) / 20.0 + 1.0;
  }

  /* ---- Lifecycle ---- */

  int open(CLAWDB_OPEN_ARGS) override;
  int close() override;
  int create(CLAWDB_CREATE_ARGS) override;
  int delete_table(CLAWDB_DELETE_TABLE_ARGS) override;
  int rename_table(CLAWDB_RENAME_TABLE_ARGS) override;

  /* ---- DML ---- */

  int write_row(uchar *buf) override;
  int update_row(const uchar *old_data, uchar *new_data) override;
  int delete_row(const uchar *buf) override;

  /* ---- Full-table scan ---- */

  int rnd_init(bool scan) override;
  int rnd_end() override;
  int rnd_next(uchar *buf) override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;

  /* ---- Index scan (not supported; KNN is done via rnd_next + filesort) ---- */

  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;
  int index_next(uchar *buf) override;
  int index_prev(uchar *buf) override;
  int index_first(uchar *buf) override;
  int index_last(uchar *buf) override;

  /* ---- Metadata ---- */

  int info(uint flag) override;
  int extra(enum ha_extra_function operation) override;
  int external_lock(THD *thd, int lock_type) override;
  int delete_all_rows() override;
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;

 private:
  /**
    Build the data file path for a table.
    Format: <mysql_data_dir>/<db>/<table>.clawdb
  */
  static std::string make_data_file_path(const char *table_name);

  /**
    Rebuild the HNSW index from the current contents of the store.
    Called during open() to restore the in-memory index.
  */
  int rebuild_hnsw_index();

  /**
    Scan the table's key definitions for a BLOB-column index whose
    COMMENT contains HNSW parameters, and apply them to the share.
    Called during create() and open().
  */
  void apply_hnsw_index_params();

  /* ---- Per-handler state ---- */

  THR_LOCK_DATA lock_;           ///< MySQL lock data
  ClawdbShare *share_{nullptr};  ///< Shared table state

  /** Current scan position (file offset of the next row to read). */
  ClawdbRowPosition scan_position_{CLAWDB_INVALID_POSITION};

  /** Position of the last row returned by rnd_next() / rnd_pos(). */
  ClawdbRowPosition current_position_{CLAWDB_INVALID_POSITION};

  /**
    Scan forward from scan_position_ looking for the next row whose key
    fields match the saved index key.  Shared logic for index_read_map()
    and index_next().

    @param[out] buf  MySQL record buffer to fill on match
    @return 0 on success, HA_ERR_END_OF_FILE when no more rows match,
            HA_ERR_CRASHED_ON_USAGE on I/O error.
  */
  int scan_and_match_key(uchar *buf);

  /**
    Saved key buffer and length for index scans.
    index_read_map() stores the lookup key here so that index_next() can
    continue scanning for additional matching rows.
  */
  std::vector<uchar> index_key_buf_;
  uint index_key_len_{0};
  uint index_key_part_map_{0};

  /* ---- HNSW-accelerated scan state ---- */

  /** true when rnd_next() should iterate over hnsw_scan_results_
      instead of performing a full-table scan. */
  bool hnsw_scan_active_{false};

  /** true after the first rnd_next() has checked the thread-local
      HNSW query hint.  Prevents repeated probe attempts. */
  bool hnsw_probe_attempted_{false};

  /** Cached HNSW search results (row positions ordered by distance). */
  std::vector<HnswCandidate> hnsw_scan_results_;

  /** Current index into hnsw_scan_results_ during an HNSW scan. */
  size_t hnsw_scan_index_{0};
};

#endif  /* STORAGE_CLAWDB_HA_CLAWDB_H */
