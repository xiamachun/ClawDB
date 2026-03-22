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

#ifdef __cplusplus
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
