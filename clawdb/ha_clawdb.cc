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

/** @file storage/clawdb/ha_clawdb.cc

  @author Amazon.Xia
  @date   2026-03-22


  @brief
  ClawDB storage engine implementation.

  ClawDB is a MySQL plugin storage engine with native vector support.
  Vectors are stored as BLOB columns using a compact binary format:
    [uint32_t dim][float32 x0][float32 x1]...[float32 x_{dim-1}]

  The engine maintains an in-memory HNSW index per table for
  accelerated approximate nearest-neighbor search.  The index is
  rebuilt from the data file on table open.

  SQL usage:
  @code
    -- Create a table with a vector column (stored as BLOB)
    CREATE TABLE vec_tbl (
      id INT PRIMARY KEY,
      embedding BLOB COMMENT 'VECTOR(128)'
    ) ENGINE=CLAWDB;

    -- Insert a vector
    INSERT INTO vec_tbl VALUES (1, clawdb_to_vector('[0.1,0.2,...]'));

    -- KNN search (brute-force via filesort on vector_distance)
    SELECT id, vector_distance(embedding, '[0.1,0.2,...]') AS dist
    FROM vec_tbl
    ORDER BY dist
    LIMIT 10;
  @endcode

  Plugin registration:
    The plugin registers three UDFs (vector_distance, clawdb_to_vector,
    clawdb_from_vector) via the mysql_udf_registration service during
    plugin init, and unregisters them during plugin deinit.
*/

#include "ha_clawdb.h"

#include <cerrno>
#include <cstring>
#include <string>

/* MySQL server headers */
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/table.h"

/* -----------------------------------------------------------------------
   apply_hnsw_index_params: scan KEY definitions for HNSW COMMENT
   ----------------------------------------------------------------------- */

void ha_clawdb::apply_hnsw_index_params() {
  if (table == nullptr || share_ == nullptr) return;

  ClawdbHnswParams params;

  /* Scan all keys looking for a secondary index on a BLOB column
     whose COMMENT contains HNSW parameters. */
  for (uint key_idx = 0; key_idx < table->s->keys; ++key_idx) {
    KEY *key_info = table->key_info + key_idx;

    /* Skip PRIMARY KEY — we only care about secondary indexes. */
    if (key_info->flags & HA_NOSAME) continue;

    /* Check if this key covers a BLOB field. */
    bool covers_blob = false;
    for (uint part_idx = 0; part_idx < key_info->user_defined_key_parts;
         ++part_idx) {
      Field *field = key_info->key_part[part_idx].field;
      if (is_blob_field(field)) {
        covers_blob = true;
        break;
      }
    }

    if (!covers_blob) continue;

    /* Found a secondary index on a BLOB column.  Parse its COMMENT. */
    const char *comment_str = CLAWDB_KEY_COMMENT_STR(*key_info);
    size_t comment_len = CLAWDB_KEY_COMMENT_LEN(*key_info);

    if (comment_str != nullptr && comment_len > 0) {
      clawdb_parse_hnsw_comment(comment_str, comment_len, &params);
    } else {
      /* Secondary index on BLOB without COMMENT: treat as HNSW with defaults. */
      params.has_vector_index = true;
    }
    break;  /* Only one vector index per table is supported. */
  }

  /* Apply parsed parameters to the share. */
  share_->hnsw_params = params;

  if (params.has_vector_index && !share_->hnsw) {
    /* Create the HNSW index only if one does not already exist.
       Subsequent open() calls on the same share must not replace a
       populated index with an empty one — rebuild_hnsw_index() is
       called exactly once after the data file is first opened. */
    share_->hnsw = std::make_unique<ClawdbHnswIndex>(
        params.m, params.ef_construction, params.ef_search, params.metric);
  }
}

/* -----------------------------------------------------------------------
   ha_clawdb constructor
   ----------------------------------------------------------------------- */

ha_clawdb::ha_clawdb(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg) {
  /* Set ref_length early so that position() / rnd_pos() always have the
     correct size, even before info(HA_STATUS_CONST) is called.  This is
     critical on MySQL 5.7 where filesort with BLOB columns calls
     position() before info(HA_STATUS_CONST). */
  ref_length = sizeof(ClawdbRowPosition);
}

/* -----------------------------------------------------------------------
   Data file path helpers
   ----------------------------------------------------------------------- */

std::string ha_clawdb::make_data_file_path(const char *table_name) {
  /* table_name from MySQL is of the form: ./db_name/table_name
     We replace the leading './' with the actual data directory path
     and append the .clawdb extension. */
  std::string path(table_name);
  path += ".clawdb";
  return path;
}

/* -----------------------------------------------------------------------
   HNSW index rebuild
   ----------------------------------------------------------------------- */

int ha_clawdb::rebuild_hnsw_index() {
  DBUG_TRACE;

  if (share_ == nullptr || !share_->store->is_open()) return 0;

  share_->hnsw->clear();

  int vector_field_idx = clawdb_find_vector_field_index(table);
  if (vector_field_idx < 0) {
    /* No BLOB field found; nothing to index. */
    return 0;
  }

  ClawdbRowPosition position = share_->store->scan_start_position();
  size_t rows_scanned = 0;
  size_t rows_indexed = 0;
  size_t rows_failed = 0;

  while (true) {
    ClawdbRowPosition live_position = CLAWDB_INVALID_POSITION;
    int rc = share_->store->next_live_row(position, &live_position);
    if (rc == HA_ERR_END_OF_FILE) break;
    if (rc != 0) return rc;

    ++rows_scanned;

    std::vector<unsigned char> row_data;
    uint32_t row_length = 0;
    rc = share_->store->read_row_at(live_position, &row_data, &row_length);
    if (rc != 0) {
      position = live_position + sizeof(ClawdbRowHeader) + 1;
      continue;
    }

    /* Deserialize the stored row into a temporary record buffer so that
       Field objects can correctly decode BLOB columns. */
    std::vector<uchar> tmp_record(table->s->reclength, 0);
    if (!clawdb_deserialize_row(table, row_data.data(), row_data.size(), tmp_record.data())) {
      ++rows_failed;
      position = live_position + sizeof(ClawdbRowHeader) + row_length;
      continue;
    }

    /* extract_vector_from_row uses move_field_offset(buf - record[0]) to
       temporarily point the Field at the given buffer.  Since tmp_record
       is NOT table->record[0], the offset will be non-zero and the Field
       will correctly read from tmp_record without needing to swap
       table->record[0]. */
    ClawdbVector vec;
    std::string errmsg;
    bool ok = clawdb_extract_vector_from_row(table, tmp_record.data(),
                                             vector_field_idx, &vec, &errmsg);

    clawdb_free_blob_buffers(table, tmp_record.data());

    if (ok) {
      share_->hnsw->insert(live_position, vec);
      ++rows_indexed;
    } else {
      ++rows_failed;
    }

    position = live_position + sizeof(ClawdbRowHeader) + row_length;
  }

  DBUG_PRINT("info", ("ClawDB: HNSW rebuild: scanned=%zu, indexed=%zu, "
                        "failed=%zu, hnsw_size=%zu",
                        rows_scanned, rows_indexed, rows_failed,
                        share_->hnsw->size()));

  return 0;
}

/* -----------------------------------------------------------------------
   Lifecycle: open / close / create / delete / rename
   ----------------------------------------------------------------------- */

int ha_clawdb::open(CLAWDB_OPEN_ARGS) {
  DBUG_TRACE;

  share_ = clawdb_get_share(name);
  if (share_ == nullptr) return HA_ERR_OUT_OF_MEM;

  thr_lock_data_init(&share_->lock, &lock_, nullptr);

  /* Parse HNSW parameters from index COMMENT (if any).
     This must happen before rebuild_hnsw_index() so that the index
     is constructed with the correct parameters. */
  apply_hnsw_index_params();

  std::unique_lock<std::mutex> lock(share_->share_mutex);

  /* Compute the .hnsw file path (parallel to the .clawdb data file). */
  if (share_->hnsw_file_path.empty()) {
    std::string data_path = make_data_file_path(name);
    /* Replace .clawdb extension with .hnsw */
    std::string hnsw_path = data_path;
    size_t ext_pos = hnsw_path.rfind(".clawdb");
    if (ext_pos != std::string::npos) {
      hnsw_path.replace(ext_pos, 7, ".hnsw");
    } else {
      hnsw_path += ".hnsw";
    }
    share_->hnsw_file_path = hnsw_path;
  }

  ++share_->open_count;

  if (!share_->store->is_open()) {
    std::string file_path = make_data_file_path(name);
    int rc = share_->store->open(file_path, false /* do not create */);
    if (rc != 0 && rc != ENOENT) return HA_ERR_CRASHED_ON_USAGE;

    if (rc == ENOENT) {
      /* Table file does not exist yet; will be created by create(). */
      return 0;
    }

    /* Try to load the persisted HNSW index from the .hnsw file.
       If successful, skip the expensive rebuild from the data file. */
    if (share_->hnsw_params.has_vector_index && share_->hnsw) {
      bool loaded = share_->hnsw->load(share_->hnsw_file_path);
      if (loaded && share_->hnsw->size() > 0) {
        share_->hnsw_dirty = false;
        return 0;
      }
    }

    lock.unlock();
    rc = rebuild_hnsw_index();
    if (rc != 0) return rc;

    /* After a successful rebuild, persist the index immediately so that
       subsequent restarts can load it directly. */
    if (share_->hnsw_params.has_vector_index && share_->hnsw &&
        share_->hnsw->size() > 0) {
      share_->hnsw->save(share_->hnsw_file_path);
      share_->hnsw_dirty = false;
    }
  } else if (share_->hnsw_params.has_vector_index &&
             share_->hnsw && share_->hnsw->size() == 0 &&
             share_->store->row_count() > 0) {
    /* The store was already opened (e.g. by a DD check during startup)
       but the HNSW index is empty — the earlier open() likely had an
       incomplete TABLE object.  Try loading from file first, then
       re-attempt the rebuild. */
    bool loaded = share_->hnsw->load(share_->hnsw_file_path);
    if (loaded && share_->hnsw->size() > 0) {
      share_->hnsw_dirty = false;
      return 0;
    }

    lock.unlock();
    int rc = rebuild_hnsw_index();
    if (rc != 0) return rc;

    if (share_->hnsw_params.has_vector_index && share_->hnsw &&
        share_->hnsw->size() > 0) {
      share_->hnsw->save(share_->hnsw_file_path);
      share_->hnsw_dirty = false;
    }
  }

  return 0;
}

int ha_clawdb::close() {
  DBUG_TRACE;

  if (share_ != nullptr) {
    std::unique_lock<std::mutex> lock(share_->share_mutex);
    --share_->open_count;

    /* When the last handler closes, persist the HNSW index if dirty. */
    if (share_->open_count <= 0 && share_->hnsw_dirty &&
        share_->hnsw_params.has_vector_index && share_->hnsw &&
        share_->hnsw->size() > 0 && !share_->hnsw_file_path.empty()) {
      lock.unlock();
      share_->hnsw->save(share_->hnsw_file_path);
      share_->hnsw_dirty = false;
    }
  }

  share_ = nullptr;
  return 0;
}

int ha_clawdb::create(CLAWDB_CREATE_ARGS) {
  DBUG_TRACE;

  std::string file_path = make_data_file_path(name);

  /* Get or create the share. */
  ClawdbShare *new_share = clawdb_get_share(name);
  if (new_share == nullptr) return HA_ERR_OUT_OF_MEM;

  /* Parse HNSW parameters from index COMMENT before opening the store.
     share_ must be set for apply_hnsw_index_params() to work. */
  share_ = new_share;
  apply_hnsw_index_params();

  std::unique_lock<std::mutex> lock(new_share->share_mutex);

  if (new_share->store->is_open()) {
    /* Already open (e.g. CREATE TABLE IF NOT EXISTS). */
    return 0;
  }

  int rc = new_share->store->open(file_path, true /* create */);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  return 0;
}

int ha_clawdb::delete_table(CLAWDB_DELETE_TABLE_ARGS) {
  DBUG_TRACE;

  std::string file_path = make_data_file_path(name);

  /* Compute the .hnsw file path. */
  std::string hnsw_path = file_path;
  size_t ext_pos = hnsw_path.rfind(".clawdb");
  if (ext_pos != std::string::npos) {
    hnsw_path.replace(ext_pos, 7, ".hnsw");
  } else {
    hnsw_path += ".hnsw";
  }

  /* Remove from share map. */
  {
    std::unique_lock<std::mutex> lock(global_share_mutex);
    auto it = global_share_map.find(std::string(name));
    if (it != global_share_map.end()) {
      it->second->store->close();
      delete it->second;
      global_share_map.erase(it);
    }
  }

  if (std::remove(file_path.c_str()) != 0 && errno != ENOENT) {
    return HA_ERR_CRASHED_ON_USAGE;
  }

  /* Also remove the .hnsw index file. */
  std::remove(hnsw_path.c_str());

  return 0;
}

int ha_clawdb::rename_table(CLAWDB_RENAME_TABLE_ARGS) {
  DBUG_TRACE;

  std::string from_path = make_data_file_path(from);
  std::string to_path = make_data_file_path(to);

  /* Compute .hnsw file paths. */
  auto make_hnsw_path = [](const std::string &data_path) -> std::string {
    std::string hnsw_path = data_path;
    size_t ext_pos = hnsw_path.rfind(".clawdb");
    if (ext_pos != std::string::npos) {
      hnsw_path.replace(ext_pos, 7, ".hnsw");
    } else {
      hnsw_path += ".hnsw";
    }
    return hnsw_path;
  };

  std::string from_hnsw = make_hnsw_path(from_path);
  std::string to_hnsw = make_hnsw_path(to_path);

  /* Close and remove the old share. */
  {
    std::unique_lock<std::mutex> lock(global_share_mutex);
    auto it = global_share_map.find(std::string(from));
    if (it != global_share_map.end()) {
      it->second->store->close();
      delete it->second;
      global_share_map.erase(it);
    }
  }

  if (std::rename(from_path.c_str(), to_path.c_str()) != 0) {
    return HA_ERR_CRASHED_ON_USAGE;
  }

  /* Also rename the .hnsw index file (ignore errors if it doesn't exist). */
  std::rename(from_hnsw.c_str(), to_hnsw.c_str());

  return 0;
}

/* -----------------------------------------------------------------------
   DML: write_row / update_row / delete_row
   ----------------------------------------------------------------------- */

int ha_clawdb::write_row(uchar *buf) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;

  std::unique_lock<std::mutex> lock(share_->share_mutex);

  if (!share_->store->is_open()) return HA_ERR_CRASHED_ON_USAGE;

  /* Serialize the row into a portable byte stream (BLOB pointers are
     replaced with inline data so the file is valid across restarts). */
  std::vector<unsigned char> serialized;
  clawdb_serialize_row(table, buf, &serialized);

  /* Append the serialized row and capture its file position. */
  ClawdbRowPosition new_position = CLAWDB_INVALID_POSITION;
  int rc = share_->store->append_row_at(serialized.data(),
                                        static_cast<uint32_t>(serialized.size()),
                                        &new_position);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Update the HNSW index with the new vector.
     We unlock before calling into the HNSW index because HNSW operations
     can be expensive and the index has its own internal locking. */
  int vector_field_idx = clawdb_find_vector_field_index(table);
  if (vector_field_idx >= 0) {
    ClawdbVector vec;
    std::string errmsg;
    if (clawdb_extract_vector_from_row(table, buf, vector_field_idx, &vec, &errmsg)) {
      share_->hnsw_dirty = true;
      lock.unlock();
      share_->hnsw->insert(new_position, vec);
    }
  }

  return 0;
}

int ha_clawdb::update_row(const uchar * /*old_data*/,
                          uchar *new_data) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;
  if (current_position_ == CLAWDB_INVALID_POSITION)
    return HA_ERR_CRASHED_ON_USAGE;

  std::unique_lock<std::mutex> lock(share_->share_mutex);

  /* Serialize the new row into a portable byte stream. */
  std::vector<unsigned char> serialized;
  clawdb_serialize_row(table, new_data, &serialized);

  ClawdbRowPosition new_position = CLAWDB_INVALID_POSITION;
  int rc = share_->store->update_row_at(current_position_, serialized.data(),
                                        static_cast<uint32_t>(serialized.size()),
                                        &new_position);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Update HNSW index: remove old entry, insert new one. */
  int vector_field_idx = clawdb_find_vector_field_index(table);
  if (vector_field_idx >= 0) {
    share_->hnsw->remove(current_position_);

    ClawdbVector new_vec;
    std::string errmsg;
    if (clawdb_extract_vector_from_row(table, new_data, vector_field_idx,
                                       &new_vec, &errmsg)) {
      share_->hnsw_dirty = true;
      lock.unlock();
      share_->hnsw->insert(new_position, new_vec);
    }
  }

  current_position_ = new_position;
  return 0;
}

int ha_clawdb::delete_row(const uchar * /*buf*/) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;
  if (current_position_ == CLAWDB_INVALID_POSITION)
    return HA_ERR_CRASHED_ON_USAGE;

  std::unique_lock<std::mutex> lock(share_->share_mutex);

  int rc = share_->store->delete_row_at(current_position_);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Remove from HNSW index. */
  share_->hnsw_dirty = true;
  lock.unlock();
  share_->hnsw->remove(current_position_);

  current_position_ = CLAWDB_INVALID_POSITION;
  return 0;
}

/* -----------------------------------------------------------------------
   Full-table scan: rnd_init / rnd_next / rnd_end / rnd_pos / position
   ----------------------------------------------------------------------- */

int ha_clawdb::rnd_init(bool /*scan*/) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;

  scan_position_ = share_->store->scan_start_position();
  current_position_ = CLAWDB_INVALID_POSITION;

  /* Reset HNSW scan state from any previous scan. */
  hnsw_scan_active_ = false;
  hnsw_scan_results_.clear();
  hnsw_scan_index_ = 0;

  /* Mark that we have not yet attempted HNSW probe.  The actual HNSW
     search is deferred to the first rnd_next() call because MySQL may
     call rnd_init() before vector_distance_init() has set the
     thread-local query hint. */
  hnsw_probe_attempted_ = false;

  return 0;
}

int ha_clawdb::rnd_end() {
  DBUG_TRACE;
  scan_position_ = CLAWDB_INVALID_POSITION;

  /* Clean up HNSW scan state. */
  hnsw_scan_active_ = false;
  hnsw_scan_results_.clear();
  hnsw_scan_index_ = 0;

  return 0;
}

int ha_clawdb::rnd_next(uchar *buf) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;
  if (!share_->store->is_open()) return HA_ERR_END_OF_FILE;

  ha_statistic_increment(&CLAWDB_STAT(ha_read_rnd_next_count));

  /* ---- Deferred HNSW probe ----
     MySQL calls vector_distance_init() lazily — only when the first row
     is being evaluated, which is AFTER the first rnd_next() has already
     returned a row.  Therefore we check the thread-local hint on EVERY
     rnd_next() call until either:
       (a) we successfully activate HNSW mode, or
       (b) we have already probed and the hint was not set.
     Once HNSW mode is activated mid-scan, we discard the rows already
     returned by the full-table scan path and switch to returning only
     HNSW candidates.  MySQL's filesort will re-sort everything anyway,
     so returning a superset (some full-scan rows + all HNSW candidates)
     is correct — the extra rows just get a higher distance and are
     filtered out by LIMIT. */

  if (!hnsw_scan_active_ && !hnsw_probe_attempted_ &&
      share_->hnsw_params.has_vector_index &&
      share_->hnsw && share_->hnsw->size() > 0) {
    ClawdbHnswQueryHint &hint = clawdb_get_thread_query_hint();
    if (hint.active && hint.query_vec.dim > 0) {
      hnsw_probe_attempted_ = true;

      int candidate_count = share_->hnsw_params.ef_search;
      if (candidate_count < 10) candidate_count = 10;

      HnswSearchResult results =
          share_->hnsw->search(hint.query_vec, candidate_count);

      if (!results.empty()) {
        hnsw_scan_results_ = std::move(results);
        hnsw_scan_index_ = 0;
        hnsw_scan_active_ = true;

        DBUG_PRINT("info", ("ClawDB: HNSW index used: %zu candidates "
                            "(ef_search=%d, index_size=%zu)",
                            hnsw_scan_results_.size(), candidate_count,
                            share_->hnsw->size()));
      }

      clawdb_clear_thread_query_hint();
    }
  }

  /* ---- HNSW-accelerated path ----
     When hnsw_scan_active_ is true, we iterate over the pre-computed
     candidate list instead of scanning the entire data file.  Each
     candidate's node_id is the row's file position (ClawdbRowPosition). */
  if (hnsw_scan_active_) {
    while (hnsw_scan_index_ < hnsw_scan_results_.size()) {
      ClawdbRowPosition candidate_position =
          static_cast<ClawdbRowPosition>(
              hnsw_scan_results_[hnsw_scan_index_].node_id);
      hnsw_scan_index_++;

      std::vector<unsigned char> row_data;
      uint32_t row_length = 0;
      int rc = share_->store->read_row_at(candidate_position,
                                          &row_data, &row_length);
      if (rc != 0) {
        /* Row may have been deleted since the HNSW index was built;
           skip to the next candidate. */
        continue;
      }

      std::memset(buf, 0, table->s->reclength);
      if (!clawdb_deserialize_row(table, row_data.data(), row_data.size(), buf)) {
        continue;
      }

      current_position_ = candidate_position;
      return 0;
    }

    /* All HNSW candidates exhausted. */
    return HA_ERR_END_OF_FILE;
  }

  /* ---- Original full-table scan path ---- */
  ClawdbRowPosition live_position = CLAWDB_INVALID_POSITION;
  int rc = share_->store->next_live_row(scan_position_, &live_position);
  if (rc == HA_ERR_END_OF_FILE) return HA_ERR_END_OF_FILE;
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  std::vector<unsigned char> row_data;
  uint32_t row_length = 0;
  rc = share_->store->read_row_at(live_position, &row_data, &row_length);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Deserialize the portable byte stream back into the MySQL record buffer.
     This correctly reconstructs BLOB fields with valid heap pointers. */
  std::memset(buf, 0, table->s->reclength);
  if (!clawdb_deserialize_row(table, row_data.data(), row_data.size(), buf)) {
    return HA_ERR_CRASHED_ON_USAGE;
  }

  current_position_ = live_position;
  /* Advance scan_position_ past this row for the next call. */
  scan_position_ = live_position + sizeof(ClawdbRowHeader) + row_length;

  return 0;
}

void ha_clawdb::position(const uchar * /*record*/) {
  DBUG_TRACE;
  /* Store the current row position in the ref buffer.
     ref_length is set to sizeof(ClawdbRowPosition) in the constructor
     and again in info(HA_STATUS_CONST). */
  my_store_ptr(ref, ref_length, current_position_);
}

int ha_clawdb::rnd_pos(uchar *buf, uchar *pos) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;

  ha_statistic_increment(&CLAWDB_STAT(ha_read_rnd_count));

  ClawdbRowPosition row_position =
      static_cast<ClawdbRowPosition>(my_get_ptr(pos, ref_length));

  std::vector<unsigned char> row_data;
  uint32_t row_length = 0;
  int rc = share_->store->read_row_at(row_position, &row_data, &row_length);
  if (rc != 0) return HA_ERR_KEY_NOT_FOUND;

  /* Deserialize the portable byte stream back into the MySQL record buffer. */
  std::memset(buf, 0, table->s->reclength);
  if (!clawdb_deserialize_row(table, row_data.data(), row_data.size(), buf)) {
    return HA_ERR_CRASHED_ON_USAGE;
  }

  current_position_ = row_position;
  return 0;
}

/* -----------------------------------------------------------------------
   Index scan: implemented as a full-table scan with key comparison.

   ClawDB does not maintain a real B-tree index.  When MySQL calls
   index_read_map() (e.g. for WHERE id = 1), we fall back to a sequential
   scan of the data file and compare each row against the supplied key using
   the standard MySQL key-comparison infrastructure.  This is O(n) but
   correct, and avoids the HA_ERR_WRONG_COMMAND / HA_ERR_UNSUPPORTED errors
   that would otherwise propagate to the client.
   ----------------------------------------------------------------------- */

/**
  Scan forward from scan_position_ looking for the next row whose key
  fields match the saved index key (index_key_buf_ / index_key_part_map_).

  On success, buf is filled with the matching row, current_position_ is
  updated, and scan_position_ is advanced past the matched row.

  @param[out] buf  MySQL record buffer to fill on match
  @return 0 on match, HA_ERR_END_OF_FILE / HA_ERR_KEY_NOT_FOUND when no
          more rows match, HA_ERR_CRASHED_ON_USAGE on I/O error.
*/
int ha_clawdb::scan_and_match_key(uchar *buf) {
  KEY *key_info = table->key_info + active_index;

  while (true) {
    ClawdbRowPosition live_position = CLAWDB_INVALID_POSITION;
    int rc = share_->store->next_live_row(scan_position_, &live_position);
    if (rc == HA_ERR_END_OF_FILE) return HA_ERR_END_OF_FILE;
    if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

    std::vector<unsigned char> row_data;
    uint32_t row_length = 0;
    rc = share_->store->read_row_at(live_position, &row_data, &row_length);

    scan_position_ = live_position + sizeof(ClawdbRowHeader) + row_length;

    if (rc != 0) continue;

    std::memset(buf, 0, table->s->reclength);
    if (!clawdb_deserialize_row(table, row_data.data(), row_data.size(), buf)) continue;

    /* Compare each key part against the saved lookup key. */
    key_part_map remaining_map = index_key_part_map_;
    const uchar *key_ptr = index_key_buf_.data();
    bool matches = true;

    for (uint part_idx = 0;
         part_idx < key_info->user_defined_key_parts && remaining_map;
         ++part_idx, remaining_map >>= 1) {
      if (!(remaining_map & 1)) continue;

      KEY_PART_INFO *key_part = key_info->key_part + part_idx;
      Field *field = key_part->field;

      const ptrdiff_t offset = static_cast<ptrdiff_t>(buf - table->record[0]);
      field->move_field_offset(offset);
      int cmp = field->key_cmp(key_ptr, key_part->length);
      field->move_field_offset(-offset);

      key_ptr += key_part->store_length;

      if (cmp != 0) {
        matches = false;
        break;
      }
    }

    if (matches) {
      current_position_ = live_position;
      return 0;
    }

    clawdb_free_blob_buffers(table, buf);
  }
}

int ha_clawdb::index_read_map(uchar *buf, const uchar *key,
                              key_part_map keypart_map,
                              enum ha_rkey_function /*find_flag*/) {
  DBUG_TRACE;

  if (share_ == nullptr || !share_->store->is_open()) return HA_ERR_CRASHED_ON_USAGE;

  /* Save the key so that index_next() can continue scanning. */
  index_key_len_ = calculate_key_len(table, active_index, keypart_map);
  index_key_buf_.assign(key, key + index_key_len_);
  index_key_part_map_ = keypart_map;

  /* Start scanning from the beginning of the file. */
  scan_position_ = share_->store->scan_start_position();
  current_position_ = CLAWDB_INVALID_POSITION;

  int rc = scan_and_match_key(buf);
  return (rc == HA_ERR_END_OF_FILE) ? HA_ERR_KEY_NOT_FOUND : rc;
}

int ha_clawdb::index_next(uchar *buf) {
  DBUG_TRACE;

  if (share_ == nullptr || !share_->store->is_open()) return HA_ERR_CRASHED_ON_USAGE;
  if (index_key_buf_.empty()) return HA_ERR_END_OF_FILE;

  return scan_and_match_key(buf);
}

int ha_clawdb::index_prev(uchar * /*buf*/) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

int ha_clawdb::index_first(uchar * /*buf*/) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

int ha_clawdb::index_last(uchar * /*buf*/) {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/* -----------------------------------------------------------------------
   Metadata and locking
   ----------------------------------------------------------------------- */

int ha_clawdb::info(uint flag) {
  DBUG_TRACE;

  if (flag & HA_STATUS_VARIABLE) {
    stats.records =
        (share_ != nullptr) ? share_->store->row_count() : 0;
    if (stats.records < 2) stats.records = 2;
  }

  if (flag & HA_STATUS_CONST) {
    /* ref_length: size of the position reference stored by position(). */
    ref_length = sizeof(ClawdbRowPosition);
  }

  return 0;
}

int ha_clawdb::extra(enum ha_extra_function /*operation*/) {
  DBUG_TRACE;
  return 0;
}

int ha_clawdb::external_lock(THD * /*thd*/, int /*lock_type*/) {
  DBUG_TRACE;
  return 0;
}

int ha_clawdb::delete_all_rows() {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;

  /* Clear the HNSW index first (it has its own internal mutex), then
     truncate the data file under the share mutex.  This ordering ensures
     that no concurrent reader can see an empty index with stale data. */
  share_->hnsw->clear();

  std::unique_lock<std::mutex> lock(share_->share_mutex);
  int rc = share_->store->truncate();
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Remove the persisted .hnsw file since the index is now empty. */
  if (!share_->hnsw_file_path.empty()) {
    std::remove(share_->hnsw_file_path.c_str());
  }
  share_->hnsw_dirty = false;

  return 0;
}

ha_rows ha_clawdb::records_in_range(uint /*inx*/, key_range * /*min_key*/,
                                    key_range * /*max_key*/) {
  DBUG_TRACE;
  return 10;
}

THR_LOCK_DATA **ha_clawdb::store_lock(THD * /*thd*/, THR_LOCK_DATA **to,
                                      enum thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && lock_.type == TL_UNLOCK)
    lock_.type = lock_type;
  *to++ = &lock_;
  return to;
}

/* -----------------------------------------------------------------------
   Plugin registration
   ----------------------------------------------------------------------- */

static handlerton *clawdb_hton = nullptr;

/** Handler factory: creates a new ha_clawdb instance. */
static handler *clawdb_create_handler(CLAWDB_CREATE_HANDLER_ARGS) {
  return new (mem_root) ha_clawdb(hton, table);
}

/**
  Register or unregister all ClawDB UDFs.

  On MySQL 8.0+, uses the plugin registry service for automatic registration.
  On MySQL 5.7, UDFs must be registered manually via CREATE FUNCTION DDL.

  @param[in] do_register  true to register, false to unregister.
  @return false on success, true on failure.
*/
static bool clawdb_manage_udfs(bool do_register) {
#if CLAWDB_HAS_UDF_REGISTRY
  SERVICE_TYPE(registry) *plugin_registry = mysql_plugin_registry_acquire();
  if (plugin_registry == nullptr) return true;

  bool error = false;
  {
    my_service<SERVICE_TYPE(udf_registration)> udf_registrar("udf_registration",
                                                             plugin_registry);
    if (!udf_registrar.is_valid()) {
      error = true;
    } else if (do_register) {
      error |= udf_registrar->udf_register(
          "vector_distance", REAL_RESULT,
          reinterpret_cast<Udf_func_any>(vector_distance),
          vector_distance_init, vector_distance_deinit);

      error |= udf_registrar->udf_register(
          "clawdb_to_vector", STRING_RESULT,
          reinterpret_cast<Udf_func_any>(clawdb_to_vector),
          clawdb_to_vector_init, clawdb_to_vector_deinit);

      error |= udf_registrar->udf_register(
          "clawdb_from_vector", STRING_RESULT,
          reinterpret_cast<Udf_func_any>(clawdb_from_vector),
          clawdb_from_vector_init, clawdb_from_vector_deinit);

      if (error) {
        int was_present = 0;
        udf_registrar->udf_unregister("vector_distance", &was_present);
        udf_registrar->udf_unregister("clawdb_to_vector", &was_present);
        udf_registrar->udf_unregister("clawdb_from_vector", &was_present);
      }
    } else {
      int was_present = 0;
      udf_registrar->udf_unregister("vector_distance", &was_present);
      udf_registrar->udf_unregister("clawdb_to_vector", &was_present);
      udf_registrar->udf_unregister("clawdb_from_vector", &was_present);
    }
  }

  mysql_plugin_registry_release(plugin_registry);
  return error;
#else
  /*
    MySQL 5.7 does not have the component service registry.
    UDFs must be registered manually after plugin load:
      CREATE FUNCTION vector_distance RETURNS REAL SONAME 'ha_clawdb.so';
      CREATE FUNCTION clawdb_to_vector RETURNS STRING SONAME 'ha_clawdb.so';
      CREATE FUNCTION clawdb_from_vector RETURNS STRING SONAME 'ha_clawdb.so';
  */
  (void)do_register;
  return false;
#endif
}

/** Plugin init: register the handlerton and UDFs. */
static int clawdb_init(void *plugin_handle) {
  DBUG_TRACE;

  clawdb_hton = static_cast<handlerton *>(plugin_handle);
  clawdb_hton->state = SHOW_OPTION_YES;
  clawdb_hton->create = clawdb_create_handler;
  clawdb_hton->flags = HTON_CAN_RECREATE;
  CLAWDB_SET_FILE_EXTENSIONS(clawdb_hton);

  /* Register UDFs via the plugin registry service. */
  if (clawdb_manage_udfs(true)) {
    /* UDF registration failed; log but do not abort plugin load.
       The engine itself is still functional; UDFs can be registered
       manually via CREATE FUNCTION if needed. */
    DBUG_PRINT("warning", ("ClawDB: failed to register UDFs "
                          "(vector_distance, clawdb_to_vector, "
                          "clawdb_from_vector). Engine still available"));
  }

  return 0;
}

/** Plugin deinit: unregister UDFs and clean up shares. */
static int clawdb_deinit(void * /*plugin_handle*/) {
  DBUG_TRACE;

  /* Unregister UDFs. */
  clawdb_manage_udfs(false);

  /* Close and free all open shares. */
  clawdb_close_all_shares();

  return 0;
}

/* -----------------------------------------------------------------------
   Plugin descriptor
   ----------------------------------------------------------------------- */

static struct st_mysql_storage_engine clawdb_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(clawdb){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &clawdb_storage_engine,
    "CLAWDB",
    "ClawDB Authors",
    "ClawDB: MySQL storage engine with native vector/embedding support. "
    "Supports BLOB-based VECTOR columns, KNN search via vector_distance(), "
    "and in-memory HNSW index for approximate nearest-neighbor retrieval.",
    PLUGIN_LICENSE_GPL,
    clawdb_init,    /* Plugin Init */
    CLAWDB_PLUGIN_CHECK_UNINSTALL
    clawdb_deinit,  /* Plugin Deinit */
    0x0100,         /* Version: 1.0 */
    nullptr,        /* Status variables */
    nullptr,        /* System variables */
    nullptr,        /* Config options */
    0,              /* Flags */
} mysql_declare_plugin_end;
