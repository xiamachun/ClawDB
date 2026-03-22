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
#include "my_base.h"
#include "sql/handler.h"
#include "thr_lock.h"

#include <cstddef>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "clawdb_hnsw.h"
#include "clawdb_store.h"
#include "clawdb_vec.h"

/* -----------------------------------------------------------------------
   Version-portable field helpers
   Defined in ha_clawdb.cc (after Field is fully declared).
   ----------------------------------------------------------------------- */

/** Return true if the given MySQL field type is a BLOB variant. */
bool is_blob_field(const Field *field);

/** Return a mutable pointer to the field's in-record storage. */
uchar *clawdb_field_ptr(Field *field);

/* -----------------------------------------------------------------------
   ClawdbShare: shared state across all open handlers for one table
   ----------------------------------------------------------------------- */

/**
  Shared state for a single ClawDB table.

  One ClawdbShare instance exists per open table, shared among all
  concurrent handler instances.  Protected by the THR_LOCK and by
  share_mutex for the HNSW index and store.
*/
class ClawdbShare : public Handler_share {
 public:
  THR_LOCK lock;                          ///< MySQL table-level lock
  std::string table_name;                 ///< Fully qualified table name
  std::unique_ptr<ClawdbTableStore> store; ///< Row storage
  std::unique_ptr<ClawdbHnswIndex> hnsw;  ///< In-memory HNSW index
  std::mutex share_mutex;                 ///< Protects store and hnsw

  explicit ClawdbShare(const std::string &name);
  ~ClawdbShare() override;
};

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

#if MYSQL_VERSION_ID < 80000
  /* MySQL 5.7 requires bas_ext() as a pure virtual override. */
  const char **bas_ext() const override {
    static const char *empty[] = {NullS};
    return empty;
  }
#endif

  enum ha_key_alg get_default_index_algorithm() const override {
    return HA_KEY_ALG_HASH;
  }

  bool is_index_algorithm_supported(enum ha_key_alg key_alg) const override {
    return key_alg == HA_KEY_ALG_HASH;
  }

  /* ---- Capability flags ---- */

  ulonglong table_flags() const override {
    return HA_BINLOG_STMT_CAPABLE | HA_BINLOG_ROW_CAPABLE |
           HA_NO_TRANSACTIONS | CLAWDB_TABLE_FLAGS_EXTRA;
  }

  ulong index_flags(uint /*inx*/, uint /*part*/,
                    bool /*all_parts*/) const override {
    return 0;
  }

  uint max_supported_record_length() const override {
    return HA_MAX_REC_LENGTH;
  }

  /* Allow up to 1 key so that PRIMARY KEY on integer columns is accepted.
     ClawDB does not actually use the key for lookups; full-table scan is used
     for all reads.  The key metadata is stored by MySQL but ignored by us. */
  uint max_supported_keys() const override { return 1; }
  uint max_supported_key_parts() const override { return 1; }
  uint max_supported_key_length() const override { return 255; }

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
  /** Get or create the ClawdbShare for this table. */
  ClawdbShare *get_share(const char *table_name);

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
    Find the BLOB field index that holds the vector data.
    Returns the field index, or -1 if no BLOB field is found.
  */
  int find_vector_field_index() const;

  /**
    Extract the vector from a MySQL row buffer.

    @param[in]  buf        MySQL row buffer
    @param[in]  field_idx  Index of the BLOB field in table->field[]
    @param[out] vec        Parsed vector
    @param[out] errmsg     Error message on failure
    @return true on success.
  */
  bool extract_vector_from_row(const uchar *buf, int field_idx,
                               ClawdbVector *vec, std::string *errmsg) const;

  /**
    Serialize a MySQL record buffer into a portable byte stream.

    BLOB fields are stored inline (length prefix + raw bytes) rather than
    as in-memory pointers.  All other fields are copied verbatim.

    @param[in]  buf   MySQL record buffer (table->s->reclength bytes)
    @param[out] out   Serialized bytes
  */
  void serialize_row(const uchar *buf, std::vector<unsigned char> *out) const;

  /**
    Deserialize a portable byte stream back into a MySQL record buffer.

    Reconstructs BLOB fields: allocates heap memory for each BLOB's data
    and stores the pointer in the correct location within buf.  The caller
    is responsible for freeing BLOB memory via free_blob_buffers().

    @param[in]  data  Serialized bytes produced by serialize_row()
    @param[in]  len   Length of data
    @param[out] buf   MySQL record buffer to fill (table->s->reclength bytes)
    @return true on success, false if data is malformed.
  */
  bool deserialize_row(const unsigned char *data, size_t len,
                       uchar *buf) const;

  /**
    Free any BLOB buffers that were heap-allocated by deserialize_row().

    @param[in] buf  MySQL record buffer whose BLOB pointers should be freed.
  */
  void free_blob_buffers(uchar *buf) const;

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
};

#endif  /* STORAGE_CLAWDB_HA_CLAWDB_H */
