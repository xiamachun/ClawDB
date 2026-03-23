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

/** @file storage/clawdb/clawdb_serde.h
  @brief
  Row serialization/deserialization utilities for ClawDB.

  Provides functions for converting MySQL record buffers to/from portable
  byte streams, with special handling for BLOB fields (which store vector
  data in ClawDB).
*/

#ifndef STORAGE_CLAWDB_CLAWDB_SERDE_H
#define STORAGE_CLAWDB_CLAWDB_SERDE_H

#include "clawdb_compat.h"
#include "clawdb_vec.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

/* Forward declarations */
class Field;
class Field_blob;
struct TABLE;

/* -----------------------------------------------------------------------
   Field type helpers
   ----------------------------------------------------------------------- */

/**
  Check if a field is a BLOB type.

  @param[in]  field  The field to check
  @return true if the field is any BLOB variant, false otherwise
*/
bool is_blob_field(const Field *field);

/**
  Get the raw data pointer from a field, portably across MySQL versions.

  @param[in]  field  The field to read from
  @return Pointer to the field's data in the record buffer
*/
uchar *clawdb_field_ptr(Field *field);

/**
  Read the raw data pointer from a BLOB field, portably across MySQL versions.

  MySQL 8.0+ provides Field_blob::get_blob_data() which returns const uchar*.
  MySQL 5.7 uses Field_blob::get_ptr(uchar**) instead.

  @param[in]  blob_field  The BLOB field to read from
  @return Pointer to the BLOB data, or nullptr if empty
*/
const uchar *clawdb_get_blob_data(Field_blob *blob_field);

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

  @param[in]  table  The table structure
  @param[in]  buf    The MySQL record buffer to serialize
  @param[out] out    Output vector to receive the serialized bytes
*/
void clawdb_serialize_row(const TABLE *table, const uchar *buf,
                          std::vector<unsigned char> *out);

/**
  Deserialize a portable byte stream back into a MySQL record buffer.

  Reconstructs BLOB fields by allocating heap memory for each BLOB's
  data and storing the pointer in the correct location within buf.
  The caller must call clawdb_free_blob_buffers() when done with the row.

  @param[in]  table  The table structure
  @param[in]  data   The serialized byte stream
  @param[in]  len    Length of the byte stream
  @param[out] buf    The MySQL record buffer to fill
  @return true on success, false on error
*/
bool clawdb_deserialize_row(const TABLE *table, const unsigned char *data,
                            size_t len, uchar *buf);

/**
  Free BLOB buffers allocated by clawdb_deserialize_row().

  @param[in]  table  The table structure
  @param[in]  buf    The MySQL record buffer containing BLOB fields
*/
void clawdb_free_blob_buffers(const TABLE *table, uchar *buf);

/* -----------------------------------------------------------------------
   Vector field helpers
   ----------------------------------------------------------------------- */

/**
  Find the index of the first BLOB field in the table.

  ClawDB uses the first BLOB field as the vector column for HNSW indexing.

  @param[in]  table  The table structure
  @return Index of the first BLOB field, or -1 if none found
*/
int clawdb_find_vector_field_index(const TABLE *table);

/**
  Extract a vector from a row's BLOB field.

  @param[in]  table      The table structure
  @param[in]  buf        The MySQL record buffer
  @param[in]  field_idx  Index of the BLOB field containing the vector
  @param[out] vec        Output vector (populated on success)
  @param[out] errmsg     Human-readable error message on failure
  @return true on success, false on error
*/
bool clawdb_extract_vector_from_row(const TABLE *table, const uchar *buf,
                                    int field_idx, ClawdbVector *vec,
                                    std::string *errmsg);

#endif  /* STORAGE_CLAWDB_CLAWDB_SERDE_H */
