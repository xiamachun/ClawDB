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

/** @file storage/clawdb/clawdb_udf.cc
  @brief
  ClawDB User-Defined Functions (UDFs).

  Registers the following SQL functions as part of the clawdb plugin:

    vector_distance(vec_col, query_str [, metric_str])
      Computes the distance between a stored vector (BLOB column) and a
      query vector string.  Returns a DOUBLE.

      Parameters:
        vec_col    - BLOB column containing a serialized ClawDB vector
        query_str  - Vector string, e.g. '[0.1, 0.2, 0.3]'
        metric_str - Optional: 'l2' (default) or 'cosine'

      Example:
        SELECT id, vector_distance(embedding, '[0.1,0.2,...]') AS dist
        FROM vec_tbl
        ORDER BY dist
        LIMIT 10;

    clawdb_to_vector(str)
      Converts a vector string '[f0, f1, ...]' to the binary BLOB format
      used by ClawDB for storage.  Use this when inserting vector data.

      Example:
        INSERT INTO vec_tbl VALUES (1, clawdb_to_vector('[0.1,0.2,...]'));

    clawdb_from_vector(blob)
      Converts a ClawDB binary vector BLOB back to the human-readable
      string '[f0, f1, ...]'.

      Example:
        SELECT id, clawdb_from_vector(embedding) FROM vec_tbl;

  UDFs are registered via the MySQL UDF plugin mechanism using
  mysql_declare_plugin.  They are installed automatically when the
  clawdb plugin is loaded with INSTALL PLUGIN.

  @note
  These UDFs are implemented as native (C++) UDFs registered through
  the plugin's init function, using the mysql_udf_registration service.
  This avoids the need for CREATE FUNCTION DDL statements.
*/

#include "clawdb_udf.h"

#include <cstring>
#include <new>
#include <string>

#include "clawdb_vec.h"

/* MYSQL_ERRMSG_SIZE is defined in mysql_com.h (512 bytes).
   We include it via the public header path. */
#ifndef MYSQL_ERRMSG_SIZE
#define MYSQL_ERRMSG_SIZE 512
#endif

/* -----------------------------------------------------------------------
   vector_distance()
   ----------------------------------------------------------------------- */

/**
  UDF init function for vector_distance().
  Validates argument count and types.
*/
bool vector_distance_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count < 2 || args->arg_count > 3) {
    std::strncpy(message,
                 "vector_distance() requires 2 or 3 arguments: "
                 "vector_distance(vec_col, query_str [, metric])",
                 MYSQL_ERRMSG_SIZE - 1);
    message[MYSQL_ERRMSG_SIZE - 1] = '\0';
    return true;  /* error */
  }

  /* arg[0]: BLOB column (binary vector) */
  args->arg_type[0] = STRING_RESULT;

  /* arg[1]: query vector string */
  args->arg_type[1] = STRING_RESULT;

  /* arg[2] (optional): metric name string */
  if (args->arg_count == 3) {
    args->arg_type[2] = STRING_RESULT;
  }

  initid->maybe_null = true;
  initid->decimals = 6;
  initid->max_length = 20;
  return false;  /* success */
}

/** UDF deinit function for vector_distance(). */
void vector_distance_deinit(UDF_INIT * /*initid*/) {}

/**
  UDF implementation for vector_distance().

  Returns the distance between the stored vector (arg[0]) and the
  query vector string (arg[1]).  Metric is determined by arg[2] if
  present (default: L2 squared distance).
*/
double vector_distance(UDF_INIT * /*initid*/, UDF_ARGS *args, char *is_null,
                       char *error) {
  /* Validate arg[0]: stored vector blob. */
  if (args->args[0] == nullptr || args->lengths[0] == 0) {
    *is_null = 1;
    return 0.0;
  }

  /* Validate arg[1]: query vector string. */
  if (args->args[1] == nullptr || args->lengths[1] == 0) {
    *is_null = 1;
    return 0.0;
  }

  /* Determine distance metric. */
  ClawdbDistanceMetric metric = ClawdbDistanceMetric::L2;
  if (args->arg_count == 3 && args->args[2] != nullptr) {
    std::string metric_name(args->args[2], args->lengths[2]);
    /* Normalize to lowercase for comparison. */
    for (char &ch : metric_name) {
      if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch + 32);
    }
    if (metric_name == "cosine") {
      metric = ClawdbDistanceMetric::COSINE;
    } else if (metric_name != "l2" && metric_name != "euclidean") {
      *error = 1;
      return 0.0;
    }
  }

  /* Deserialize the stored vector from the BLOB column. */
  ClawdbVector stored_vec;
  std::string errmsg;
  const auto *blob_data =
      reinterpret_cast<const unsigned char *>(args->args[0]);
  if (!clawdb_deserialize_vector(blob_data, args->lengths[0], &stored_vec,
                                 &errmsg)) {
    *error = 1;
    return 0.0;
  }

  /* Parse the query vector string. */
  ClawdbVector query_vec;
  /* Null-terminate the query string for parsing. */
  std::string query_str(args->args[1], args->lengths[1]);
  if (!clawdb_parse_vector_string(query_str.c_str(), &query_vec, &errmsg)) {
    *error = 1;
    return 0.0;
  }

  /* Compute and return the distance. */
  float dist = clawdb_compute_distance(stored_vec, query_vec, metric);
  if (dist < 0.0f) {
    /* Dimension mismatch. */
    *error = 1;
    return 0.0;
  }

  return static_cast<double>(dist);
}

/* -----------------------------------------------------------------------
   clawdb_to_vector()
   ----------------------------------------------------------------------- */

/** UDF init for clawdb_to_vector(). */
bool clawdb_to_vector_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    std::strncpy(message,
                 "clawdb_to_vector() requires exactly 1 argument: "
                 "clawdb_to_vector(vector_string)",
                 MYSQL_ERRMSG_SIZE - 1);
    message[MYSQL_ERRMSG_SIZE - 1] = '\0';
    return true;
  }
  args->arg_type[0] = STRING_RESULT;
  initid->maybe_null = true;

  /* The binary blob can be up to header + 16000 * 4 bytes = 64004 bytes,
     which far exceeds the 255-byte UDF result buffer.  Allocate a heap
     buffer that MySQL will free after the query; we own it via initid->ptr. */
  size_t max_blob_size =
      CLAWDB_VECTOR_HEADER_SIZE + CLAWDB_VECTOR_MAX_DIM * sizeof(float);
  initid->ptr = new (std::nothrow) char[max_blob_size];
  if (initid->ptr == nullptr) {
    std::strncpy(message, "clawdb_to_vector(): out of memory",
                 MYSQL_ERRMSG_SIZE - 1);
    message[MYSQL_ERRMSG_SIZE - 1] = '\0';
    return true;
  }
  initid->max_length = static_cast<unsigned long>(max_blob_size);
  return false;
}

/** UDF deinit for clawdb_to_vector(): free the heap buffer. */
void clawdb_to_vector_deinit(UDF_INIT *initid) {
  delete[] initid->ptr;
  initid->ptr = nullptr;
}

/**
  UDF implementation for clawdb_to_vector().
  Converts '[f0, f1, ...]' string to binary BLOB format.
  Returns a pointer to initid->ptr (heap-allocated in the init function).
*/
char *clawdb_to_vector(UDF_INIT *initid, UDF_ARGS *args, char * /*result*/,
                       unsigned long *length, char *is_null, char *error) {
  if (args->args[0] == nullptr || args->lengths[0] == 0) {
    *is_null = 1;
    return nullptr;
  }

  std::string input_str(args->args[0], args->lengths[0]);
  ClawdbVector vec;
  std::string errmsg;

  if (!clawdb_parse_vector_string(input_str.c_str(), &vec, &errmsg)) {
    *error = 1;
    return nullptr;
  }

  size_t blob_size = clawdb_vector_byte_size(vec.dim);
  size_t max_blob_size =
      CLAWDB_VECTOR_HEADER_SIZE + CLAWDB_VECTOR_MAX_DIM * sizeof(float);
  if (blob_size > max_blob_size || initid->ptr == nullptr) {
    *error = 1;
    return nullptr;
  }

  clawdb_serialize_vector(vec, reinterpret_cast<unsigned char *>(initid->ptr));
  *length = static_cast<unsigned long>(blob_size);
  return initid->ptr;
}

/* -----------------------------------------------------------------------
   clawdb_from_vector()
   ----------------------------------------------------------------------- */

/** UDF init for clawdb_from_vector(). */
bool clawdb_from_vector_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    std::strncpy(message,
                 "clawdb_from_vector() requires exactly 1 argument: "
                 "clawdb_from_vector(blob_column)",
                 MYSQL_ERRMSG_SIZE - 1);
    message[MYSQL_ERRMSG_SIZE - 1] = '\0';
    return true;
  }
  args->arg_type[0] = STRING_RESULT;
  initid->maybe_null = true;

  /* Max string: '[' + 16000 * (max float chars + ',') + ']'
     Each float32 printed with %g can be up to ~15 chars + comma = 16.
     Total: 2 + 16000 * 16 = 256002 bytes — far exceeds the 255-byte
     UDF result buffer, so we allocate a heap buffer via initid->ptr. */
  size_t max_str_size = 2 + CLAWDB_VECTOR_MAX_DIM * 16;
  initid->ptr = new (std::nothrow) char[max_str_size];
  if (initid->ptr == nullptr) {
    std::strncpy(message, "clawdb_from_vector(): out of memory",
                 MYSQL_ERRMSG_SIZE - 1);
    message[MYSQL_ERRMSG_SIZE - 1] = '\0';
    return true;
  }
  initid->max_length = static_cast<unsigned long>(max_str_size);
  return false;
}

/** UDF deinit for clawdb_from_vector(): free the heap buffer. */
void clawdb_from_vector_deinit(UDF_INIT *initid) {
  delete[] initid->ptr;
  initid->ptr = nullptr;
}

/**
  UDF implementation for clawdb_from_vector().
  Converts binary BLOB to '[f0, f1, ...]' string.
  Returns a pointer to initid->ptr (heap-allocated in the init function).
*/
char *clawdb_from_vector(UDF_INIT *initid, UDF_ARGS *args, char * /*result*/,
                         unsigned long *length, char *is_null, char *error) {
  if (args->args[0] == nullptr || args->lengths[0] == 0) {
    *is_null = 1;
    return nullptr;
  }

  if (initid->ptr == nullptr) {
    *error = 1;
    return nullptr;
  }

  const auto *blob_data =
      reinterpret_cast<const unsigned char *>(args->args[0]);
  ClawdbVector vec;
  std::string errmsg;

  if (!clawdb_deserialize_vector(blob_data, args->lengths[0], &vec, &errmsg)) {
    *error = 1;
    return nullptr;
  }

  std::string vec_str = clawdb_vector_to_string(vec);
  if (vec_str.size() >= initid->max_length) {
    *error = 1;
    return nullptr;
  }

  std::memcpy(initid->ptr, vec_str.c_str(), vec_str.size());
  *length = static_cast<unsigned long>(vec_str.size());
  return initid->ptr;
}
