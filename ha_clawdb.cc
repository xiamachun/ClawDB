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
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>

/* MySQL server headers */
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql/plugin.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/table.h"

/* ClawDB headers */
#include "clawdb_udf.h"
#include "clawdb_vec.h"

/* -----------------------------------------------------------------------
   Version-portable field helpers (declared in ha_clawdb.h)
   ----------------------------------------------------------------------- */

bool is_blob_field(const Field *field) {
  const enum_field_types field_type = field->type();
  return field_type == MYSQL_TYPE_BLOB ||
         field_type == MYSQL_TYPE_MEDIUM_BLOB ||
         field_type == MYSQL_TYPE_LONG_BLOB ||
         field_type == MYSQL_TYPE_TINY_BLOB;
}

uchar *clawdb_field_ptr(Field *field) {
#if MYSQL_VERSION_ID >= 80025
  return field->field_ptr();
#else
  return field->ptr;
#endif
}

/**
  Read the raw data pointer from a BLOB field, portably across MySQL versions.

  MySQL 8.0+ provides Field_blob::get_blob_data() which returns const uchar*.
  MySQL 5.7 uses Field_blob::get_ptr(uchar**) instead.

  @param[in]  blob_field  The BLOB field to read from.
  @return Pointer to the BLOB data, or nullptr if empty.
*/
static const uchar *clawdb_get_blob_data(Field_blob *blob_field) {
#if MYSQL_VERSION_ID >= 80000
  return blob_field->get_blob_data();
#else
  uchar *ptr = nullptr;
  blob_field->get_ptr(&ptr);
  return ptr;
#endif
}

/* -----------------------------------------------------------------------
   Global share registry
   ----------------------------------------------------------------------- */

/** Protects the global share map. */
static std::mutex global_share_mutex;

/** Maps table_name -> ClawdbShare*. */
static std::unordered_map<std::string, ClawdbShare *> global_share_map;

/* -----------------------------------------------------------------------
   ClawdbShare
   ----------------------------------------------------------------------- */

ClawdbShare::ClawdbShare(const std::string &name) : table_name(name) {
  thr_lock_init(&lock);
  store = std::make_unique<ClawdbTableStore>();
  hnsw = std::make_unique<ClawdbHnswIndex>(
      CLAWDB_HNSW_DEFAULT_M, CLAWDB_HNSW_DEFAULT_EF_CONSTRUCTION,
      CLAWDB_HNSW_DEFAULT_EF_SEARCH, ClawdbDistanceMetric::L2);
}

ClawdbShare::~ClawdbShare() { thr_lock_delete(&lock); }

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
   Share management
   ----------------------------------------------------------------------- */

ClawdbShare *ha_clawdb::get_share(const char *table_name) {
  std::string key(table_name);
  std::unique_lock<std::mutex> lock(global_share_mutex);

  auto it = global_share_map.find(key);
  if (it != global_share_map.end()) {
    return it->second;
  }

  auto *new_share = new ClawdbShare(key);
  global_share_map[key] = new_share;
  return new_share;
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

  int vector_field_idx = find_vector_field_index();
  if (vector_field_idx < 0) {
    /* No BLOB field found; nothing to index. */
    return 0;
  }

  ClawdbRowPosition position = share_->store->scan_start_position();

  while (true) {
    ClawdbRowPosition live_position = CLAWDB_INVALID_POSITION;
    int rc = share_->store->next_live_row(position, &live_position);
    if (rc == HA_ERR_END_OF_FILE) break;
    if (rc != 0) return rc;

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
    if (!deserialize_row(row_data.data(), row_data.size(), tmp_record.data())) {
      position = live_position + sizeof(ClawdbRowHeader) + row_length;
      continue;
    }

    /* Temporarily redirect table->record[0] so that extract_vector_from_row
       can use move_field_offset(0) and still read from tmp_record. */
    uchar *saved_record = table->record[0];
    table->record[0] = tmp_record.data();

    ClawdbVector vec;
    std::string errmsg;
    bool ok = extract_vector_from_row(tmp_record.data(), vector_field_idx,
                                      &vec, &errmsg);

    table->record[0] = saved_record;
    free_blob_buffers(tmp_record.data());

    if (ok) {
      share_->hnsw->insert(live_position, vec);
    }

    position = live_position + sizeof(ClawdbRowHeader) + row_length;
  }

  return 0;
}

/* -----------------------------------------------------------------------
   Vector field helpers
   ----------------------------------------------------------------------- */

int ha_clawdb::find_vector_field_index() const {
  if (table == nullptr) return -1;

  for (uint i = 0; i < table->s->fields; ++i) {
    if (is_blob_field(table->field[i])) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

bool ha_clawdb::extract_vector_from_row(const uchar *buf, int field_idx,
                                        ClawdbVector *vec,
                                        std::string *errmsg) const {
  Field *field = table->field[field_idx];

  /* Point the field at the given row buffer using move_field_offset().
     The offset is the difference between the target buffer and record[0]. */
  const ptrdiff_t row_offset =
      static_cast<ptrdiff_t>(buf - table->record[0]);
  field->move_field_offset(row_offset);

  if (field->is_null()) {
    field->move_field_offset(-row_offset);
    if (errmsg) *errmsg = "vector field is NULL";
    return false;
  }

  /* Read the BLOB data. */
  Field_blob *blob_field = down_cast<Field_blob *>(field);
  uint32_t blob_length = blob_field->get_length();
  const uchar *blob_data = clawdb_get_blob_data(blob_field);

  field->move_field_offset(-row_offset);

  if (blob_data == nullptr || blob_length == 0) {
    if (errmsg) *errmsg = "empty vector blob";
    return false;
  }

  return clawdb_deserialize_vector(blob_data, blob_length, vec, errmsg);
}

/* -----------------------------------------------------------------------
   Row serialization / deserialization
   ----------------------------------------------------------------------- */

/**
  Serialize a MySQL record buffer into a portable byte stream.

  Format:
    [null_bitmap: table->s->null_bytes bytes]
    For each non-BLOB field:
      [field data: field->pack_length() bytes]
    For each BLOB field:
      [uint32_t blob_len][blob_len bytes of actual blob data]

  This avoids storing raw in-memory pointers (which BLOB fields contain
  in the standard MySQL record format) and makes the data file portable
  across process restarts.
*/
void ha_clawdb::serialize_row(const uchar *buf,
                              std::vector<unsigned char> *out) const {
  out->clear();

  /* Null bitmap. */
  const uint null_bytes = table->s->null_bytes;
  out->insert(out->end(), buf, buf + null_bytes);

  for (uint field_idx = 0; field_idx < table->s->fields; ++field_idx) {
    Field *field = table->field[field_idx];

    /* Point the field at the source buffer. */
    const ptrdiff_t offset = static_cast<ptrdiff_t>(buf - table->record[0]);
    field->move_field_offset(offset);

    if (is_blob_field(field)) {
      Field_blob *blob_field = down_cast<Field_blob *>(field);
      uint32_t blob_len = blob_field->get_length();
      const uchar *blob_data = clawdb_get_blob_data(blob_field);

      /* Store 4-byte little-endian length followed by raw blob bytes. */
      unsigned char len_buf[4];
      std::memcpy(len_buf, &blob_len, sizeof(uint32_t));
      out->insert(out->end(), len_buf, len_buf + 4);

      if (blob_len > 0 && blob_data != nullptr) {
        out->insert(out->end(), blob_data, blob_data + blob_len);
      }
    } else {
      /* Non-BLOB: copy the field's in-record bytes verbatim. */
      const uchar *fld_ptr = clawdb_field_ptr(field);
      uint32_t field_len = field->pack_length();
      out->insert(out->end(), fld_ptr, fld_ptr + field_len);
    }

    field->move_field_offset(-offset);
  }
}

/**
  Deserialize a portable byte stream back into a MySQL record buffer.

  Reconstructs BLOB fields by allocating heap memory for each BLOB's
  data and storing the pointer in the correct location within buf.
  The caller must call free_blob_buffers(buf) when done with the row.
*/
bool ha_clawdb::deserialize_row(const unsigned char *data, size_t len,
                                uchar *buf) const {
  const unsigned char *ptr = data;
  const unsigned char *end = data + len;

  /* Restore null bitmap. */
  const uint null_bytes = table->s->null_bytes;
  if (ptr + null_bytes > end) return false;
  std::memcpy(buf, ptr, null_bytes);
  ptr += null_bytes;

  for (uint field_idx = 0; field_idx < table->s->fields; ++field_idx) {
    Field *field = table->field[field_idx];

    /* Point the field at the destination buffer. */
    const ptrdiff_t offset = static_cast<ptrdiff_t>(buf - table->record[0]);
    field->move_field_offset(offset);

    if (is_blob_field(field)) {
      Field_blob *blob_field = down_cast<Field_blob *>(field);

      /* Read 4-byte little-endian length. */
      if (ptr + 4 > end) {
        field->move_field_offset(-offset);
        return false;
      }
      uint32_t blob_len = 0;
      std::memcpy(&blob_len, ptr, sizeof(uint32_t));
      ptr += 4;

      if (ptr + blob_len > end) {
        field->move_field_offset(-offset);
        return false;
      }

      if (blob_len == 0) {
        /* NULL or empty blob: mark null and store zero-length pointer. */
        blob_field->set_null();
        blob_field->set_ptr(static_cast<uint32_t>(0),
                            static_cast<const uchar *>(nullptr));
      } else {
        /* Allocate heap memory and copy blob data. */
        uchar *heap_buf = new (std::nothrow) uchar[blob_len];
        if (heap_buf == nullptr) {
          field->move_field_offset(-offset);
          return false;
        }
        std::memcpy(heap_buf, ptr, blob_len);
        blob_field->set_notnull();
        blob_field->set_ptr(blob_len, heap_buf);
        ptr += blob_len;
      }
    } else {
      /* Non-BLOB: copy bytes directly into the record buffer. */
      uint32_t field_len = field->pack_length();
      if (ptr + field_len > end) {
        field->move_field_offset(-offset);
        return false;
      }
      uchar *fld_ptr = clawdb_field_ptr(field);
      std::memcpy(fld_ptr, ptr, field_len);
      ptr += field_len;
    }

    field->move_field_offset(-offset);
  }

  return true;
}

/**
  Free BLOB buffers allocated by deserialize_row().
*/
void ha_clawdb::free_blob_buffers(uchar *buf) const {
  for (uint field_idx = 0; field_idx < table->s->fields; ++field_idx) {
    Field *field = table->field[field_idx];

    if (!is_blob_field(field)) continue;

    const ptrdiff_t offset = static_cast<ptrdiff_t>(buf - table->record[0]);
    field->move_field_offset(offset);

    Field_blob *blob_field = down_cast<Field_blob *>(field);
    uchar *blob_ptr = const_cast<uchar *>(clawdb_get_blob_data(blob_field));
    if (blob_ptr != nullptr) {
      delete[] blob_ptr;
      /* Zero out the pointer in the record buffer to avoid double-free. */
      blob_field->set_ptr(static_cast<uint32_t>(0),
                          static_cast<const uchar *>(nullptr));
    }

    field->move_field_offset(-offset);
  }
}

/* -----------------------------------------------------------------------
   Lifecycle: open / close / create / delete / rename
   ----------------------------------------------------------------------- */

int ha_clawdb::open(CLAWDB_OPEN_ARGS) {
  DBUG_TRACE;

  share_ = get_share(name);
  if (share_ == nullptr) return HA_ERR_OUT_OF_MEM;

  thr_lock_data_init(&share_->lock, &lock_, nullptr);

  std::unique_lock<std::mutex> lock(share_->share_mutex);

  if (!share_->store->is_open()) {
    std::string file_path = make_data_file_path(name);
    int rc = share_->store->open(file_path, false /* do not create */);
    if (rc != 0 && rc != ENOENT) return HA_ERR_CRASHED_ON_USAGE;

    if (rc == ENOENT) {
      /* Table file does not exist yet; will be created by create(). */
      return 0;
    }

    lock.unlock();
    rc = rebuild_hnsw_index();
    if (rc != 0) return rc;
  }

  return 0;
}

int ha_clawdb::close() {
  DBUG_TRACE;
  /* The share (and its store/hnsw) lives until the plugin is unloaded.
     We do not close the file here because other handler instances may
     still be using the share. */
  share_ = nullptr;
  return 0;
}

int ha_clawdb::create(CLAWDB_CREATE_ARGS) {
  DBUG_TRACE;

  std::string file_path = make_data_file_path(name);

  /* Get or create the share. */
  ClawdbShare *new_share = get_share(name);
  if (new_share == nullptr) return HA_ERR_OUT_OF_MEM;

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

  return 0;
}

int ha_clawdb::rename_table(CLAWDB_RENAME_TABLE_ARGS) {
  DBUG_TRACE;

  std::string from_path = make_data_file_path(from);
  std::string to_path = make_data_file_path(to);

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
  serialize_row(buf, &serialized);

  /* Append the serialized row and capture its file position. */
  ClawdbRowPosition new_position = CLAWDB_INVALID_POSITION;
  int rc = share_->store->append_row_at(serialized.data(),
                                        static_cast<uint32_t>(serialized.size()),
                                        &new_position);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Update the HNSW index with the new vector.
     We unlock before calling into the HNSW index because HNSW operations
     can be expensive and the index has its own internal locking. */
  int vector_field_idx = find_vector_field_index();
  if (vector_field_idx >= 0) {
    ClawdbVector vec;
    std::string errmsg;
    if (extract_vector_from_row(buf, vector_field_idx, &vec, &errmsg)) {
      lock.unlock();
      share_->hnsw->insert(new_position, vec);
    }
  }

  return 0;
}

int ha_clawdb::update_row(const uchar *old_data [[maybe_unused]],
                          uchar *new_data) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;
  if (current_position_ == CLAWDB_INVALID_POSITION)
    return HA_ERR_CRASHED_ON_USAGE;

  std::unique_lock<std::mutex> lock(share_->share_mutex);

  /* Serialize the new row into a portable byte stream. */
  std::vector<unsigned char> serialized;
  serialize_row(new_data, &serialized);

  ClawdbRowPosition new_position = CLAWDB_INVALID_POSITION;
  int rc = share_->store->update_row_at(current_position_, serialized.data(),
                                        static_cast<uint32_t>(serialized.size()),
                                        &new_position);
  if (rc != 0) return HA_ERR_CRASHED_ON_USAGE;

  /* Update HNSW index: remove old entry, insert new one. */
  int vector_field_idx = find_vector_field_index();
  if (vector_field_idx >= 0) {
    share_->hnsw->remove(current_position_);

    ClawdbVector new_vec;
    std::string errmsg;
    if (extract_vector_from_row(new_data, vector_field_idx, &new_vec,
                                &errmsg)) {
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
  return 0;
}

int ha_clawdb::rnd_end() {
  DBUG_TRACE;
  scan_position_ = CLAWDB_INVALID_POSITION;
  return 0;
}

int ha_clawdb::rnd_next(uchar *buf) {
  DBUG_TRACE;

  if (share_ == nullptr) return HA_ERR_CRASHED_ON_USAGE;
  if (!share_->store->is_open()) return HA_ERR_END_OF_FILE;

  ha_statistic_increment(&CLAWDB_STAT(ha_read_rnd_next_count));

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
  if (!deserialize_row(row_data.data(), row_data.size(), buf)) {
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
  if (!deserialize_row(row_data.data(), row_data.size(), buf)) {
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
    if (!deserialize_row(row_data.data(), row_data.size(), buf)) continue;

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

    free_blob_buffers(buf);
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
    fprintf(stderr,
            "[ClawDB] WARNING: failed to register UDFs "
            "(vector_distance, clawdb_to_vector, clawdb_from_vector). "
            "The storage engine is still available.\n");
  }

  return 0;
}

/** Plugin deinit: unregister UDFs and clean up shares. */
static int clawdb_deinit(void * /*plugin_handle*/) {
  DBUG_TRACE;

  /* Unregister UDFs. */
  clawdb_manage_udfs(false);

  /* Close and free all open shares. */
  {
    std::unique_lock<std::mutex> lock(global_share_mutex);
    for (auto &kv : global_share_map) {
      kv.second->store->close();
      delete kv.second;
    }
    global_share_map.clear();
  }

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
