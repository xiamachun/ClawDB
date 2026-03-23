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

/** @file storage/clawdb/clawdb_serde.cc
  @brief
  Row serialization/deserialization implementation for ClawDB.
*/

#include "clawdb_serde.h"
#include "clawdb_compat.h"

#include "my_dbug.h"
#include "sql/field.h"
#include "sql/table.h"

/* -----------------------------------------------------------------------
   Field type helpers
   ----------------------------------------------------------------------- */

bool is_blob_field(const Field *field) {
  const enum_field_types field_type = field->type();
  return field_type == MYSQL_TYPE_BLOB ||
         field_type == MYSQL_TYPE_MEDIUM_BLOB ||
         field_type == MYSQL_TYPE_LONG_BLOB ||
         field_type == MYSQL_TYPE_TINY_BLOB;
}

uchar *clawdb_field_ptr(Field *field) {
  return CLAWDB_FIELD_PTR(field);
}

const uchar *clawdb_get_blob_data(Field_blob *blob_field) {
#if CLAWDB_HAS_GET_BLOB_DATA
  return blob_field->get_blob_data();
#else
  uchar *ptr = nullptr;
  blob_field->get_ptr(&ptr);
  return ptr;
#endif
}

/* -----------------------------------------------------------------------
   Row serialization / deserialization
   ----------------------------------------------------------------------- */

void clawdb_serialize_row(const TABLE *table, const uchar *buf,
                          std::vector<unsigned char> *out) {
  DBUG_TRACE;
  out->clear();

  /* Null bitmap. */
  const uint null_bytes = table->s->null_bytes;
  out->insert(out->end(), buf, buf + null_bytes);

  for (uint field_idx = 0; field_idx < table->s->fields; ++field_idx) {
    Field *field = table->field[field_idx];

    /* Point the field at the source buffer. */
    const ptrdiff_t offset =
        static_cast<ptrdiff_t>(buf - table->record[0]);
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

bool clawdb_deserialize_row(const TABLE *table, const unsigned char *data,
                            size_t len, uchar *buf) {
  DBUG_TRACE;
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
    const ptrdiff_t offset =
        static_cast<ptrdiff_t>(buf - table->record[0]);
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
                            static_cast<uchar *>(nullptr));
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

void clawdb_free_blob_buffers(const TABLE *table, uchar *buf) {
  DBUG_TRACE;
  for (uint field_idx = 0; field_idx < table->s->fields; ++field_idx) {
    Field *field = table->field[field_idx];

    if (!is_blob_field(field)) continue;

    const ptrdiff_t offset =
        static_cast<ptrdiff_t>(buf - table->record[0]);
    field->move_field_offset(offset);

    Field_blob *blob_field = down_cast<Field_blob *>(field);
    uchar *blob_ptr = const_cast<uchar *>(clawdb_get_blob_data(blob_field));
    if (blob_ptr != nullptr) {
      delete[] blob_ptr;
      /* Zero out the pointer in the record buffer to avoid double-free. */
      blob_field->set_ptr(static_cast<uint32_t>(0),
                          static_cast<uchar *>(nullptr));
    }

    field->move_field_offset(-offset);
  }
}

/* -----------------------------------------------------------------------
   Vector field helpers
   ----------------------------------------------------------------------- */

int clawdb_find_vector_field_index(const TABLE *table) {
  DBUG_TRACE;
  if (table == nullptr) return -1;

  for (uint i = 0; i < table->s->fields; ++i) {
    if (is_blob_field(table->field[i])) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

bool clawdb_extract_vector_from_row(const TABLE *table, const uchar *buf,
                                    int field_idx, ClawdbVector *vec,
                                    std::string *errmsg) {
  DBUG_TRACE;
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
