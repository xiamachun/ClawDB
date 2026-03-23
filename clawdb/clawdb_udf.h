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

/** @file storage/clawdb/clawdb_udf.h
  @brief
  Declarations for ClawDB User-Defined Functions (UDFs).

  Functions declared here are registered via the MySQL UDF plugin
  mechanism in ha_clawdb.cc's plugin init/deinit functions.
*/

#ifndef STORAGE_CLAWDB_CLAWDB_UDF_H
#define STORAGE_CLAWDB_CLAWDB_UDF_H

#include "clawdb_compat.h"

/* -----------------------------------------------------------------------
   Thread-local HNSW query hint.

   When vector_distance() is called, it stores the parsed query vector
   and metric in a thread-local variable.  ha_clawdb::rnd_init() checks
   this hint and, if an HNSW index is available, performs an approximate
   nearest-neighbor search to narrow down the rows returned by rnd_next().

   This avoids modifying the MySQL optimizer and works across all
   supported MySQL versions (5.7, 8.0, 8.4).
   ----------------------------------------------------------------------- */

#ifdef __cplusplus

#include "clawdb_vec.h"
#include <vector>

/**
  Thread-local hint set by vector_distance() for HNSW-accelerated scans.
*/
struct ClawdbHnswQueryHint {
  bool active{false};                   ///< true when a query vector is set
  ClawdbVector query_vec;               ///< The parsed query vector
  ClawdbDistanceMetric metric{ClawdbDistanceMetric::L2};
  int top_k{0};                         ///< Requested LIMIT (0 = unknown)
};

/** Get the thread-local HNSW query hint for the current thread. */
ClawdbHnswQueryHint &clawdb_get_thread_query_hint();

/** Clear the thread-local HNSW query hint. */
void clawdb_clear_thread_query_hint();

extern "C" {
#endif

/* -----------------------------------------------------------------------
   vector_distance(vec_blob, query_str [, metric_str]) -> DOUBLE
   ----------------------------------------------------------------------- */

bool vector_distance_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void vector_distance_deinit(UDF_INIT *initid);
double vector_distance(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                       char *error);

/* -----------------------------------------------------------------------
   clawdb_to_vector(vector_string) -> BLOB
   ----------------------------------------------------------------------- */

bool clawdb_to_vector_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void clawdb_to_vector_deinit(UDF_INIT *initid);
char *clawdb_to_vector(UDF_INIT *initid, UDF_ARGS *args, char *result,
                       unsigned long *length, char *is_null, char *error);

/* -----------------------------------------------------------------------
   clawdb_from_vector(blob) -> STRING
   ----------------------------------------------------------------------- */

bool clawdb_from_vector_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void clawdb_from_vector_deinit(UDF_INIT *initid);
char *clawdb_from_vector(UDF_INIT *initid, UDF_ARGS *args, char *result,
                         unsigned long *length, char *is_null, char *error);

#ifdef __cplusplus
}
#endif

#endif  /* STORAGE_CLAWDB_CLAWDB_UDF_H */
