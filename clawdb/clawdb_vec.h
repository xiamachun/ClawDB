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

/** @file storage/clawdb/clawdb_vec.h
  @brief
  ClawDB vector type definitions, serialization, and distance functions.

  Vectors are stored as binary blobs with the layout:
    [uint32_t dim][float32 x0][float32 x1]...[float32 x_{dim-1}]

  The SQL-level representation is a JSON-style array string: '[0.1, 0.2, ...]'
  which is parsed on write and serialized back on read.

  Maximum supported dimension: CLAWDB_VECTOR_MAX_DIM (16000), aligned with
  PGVector's VECTOR_MAX_DIM.
*/

#ifndef STORAGE_CLAWDB_CLAWDB_VEC_H
#define STORAGE_CLAWDB_CLAWDB_VEC_H

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

/** Maximum number of dimensions for a VECTOR column. */
static constexpr uint32_t CLAWDB_VECTOR_MAX_DIM = 16000;

/**
  On-disk binary layout of a serialized vector.

  The blob stored in the BLOB column is:
    4 bytes: dim  (little-endian uint32_t)
    dim * 4 bytes: float32 elements (little-endian IEEE 754)
*/
static constexpr size_t CLAWDB_VECTOR_HEADER_SIZE = sizeof(uint32_t);

/** Compute the total byte size of a serialized vector with `dim` dimensions. */
inline size_t clawdb_vector_byte_size(uint32_t dim) {
  return CLAWDB_VECTOR_HEADER_SIZE + static_cast<size_t>(dim) * sizeof(float);
}

/**
  In-memory representation of a vector used throughout ClawDB.
  Owns its float data via std::vector<float>.
*/
struct ClawdbVector {
  uint32_t dim{0};
  std::vector<float> values;

  ClawdbVector() = default;

  explicit ClawdbVector(uint32_t dimension) : dim(dimension), values(dimension, 0.0f) {}

  ClawdbVector(uint32_t dimension, const float *data)
      : dim(dimension), values(data, data + dimension) {}
};

/**
  Distance metric types supported by ClawDB.
*/
enum class ClawdbDistanceMetric {
  L2,      ///< Squared Euclidean distance (L2^2), consistent with PGVector
  COSINE,  ///< Cosine distance: 1 - cosine_similarity
};

/* -----------------------------------------------------------------------
   Serialization / deserialization
   ----------------------------------------------------------------------- */

/**
  Parse a SQL-level vector string '[f0, f1, ..., fn]' into a ClawdbVector.

  @param[in]  str    Null-terminated string, e.g. "[0.1, 0.2, 0.3]"
  @param[out] vec    Output vector (populated on success)
  @param[out] errmsg Human-readable error message on failure

  @return true on success, false on parse error.
*/
bool clawdb_parse_vector_string(const char *str, ClawdbVector *vec,
                                std::string *errmsg);

/**
  Serialize a ClawdbVector into the binary blob format.

  @param[in]  vec   Source vector
  @param[out] blob  Output buffer; caller must ensure it is at least
                    clawdb_vector_byte_size(vec.dim) bytes.
*/
void clawdb_serialize_vector(const ClawdbVector &vec, unsigned char *blob);

/**
  Deserialize a binary blob into a ClawdbVector.

  @param[in]  blob      Pointer to the binary blob
  @param[in]  blob_len  Length of the blob in bytes
  @param[out] vec       Output vector
  @param[out] errmsg    Human-readable error message on failure

  @return true on success, false on error.
*/
bool clawdb_deserialize_vector(const unsigned char *blob, size_t blob_len,
                               ClawdbVector *vec, std::string *errmsg);

/**
  Serialize a ClawdbVector back to the SQL string representation '[f0, f1, ...]'.

  @param[in] vec  Source vector
  @return         String representation suitable for returning to MySQL client.
*/
std::string clawdb_vector_to_string(const ClawdbVector &vec);

/* -----------------------------------------------------------------------
   Distance functions
   ----------------------------------------------------------------------- */

/**
  Compute the squared L2 (Euclidean) distance between two vectors.

  Using squared distance avoids a sqrt() and is monotone with true L2,
  which is sufficient for KNN ranking.

  @param[in] a  First vector
  @param[in] b  Second vector (must have the same dim as a)
  @return       Squared L2 distance, or -1.0f on dimension mismatch.
*/
float clawdb_l2_distance(const ClawdbVector &a, const ClawdbVector &b);

/**
  Compute the cosine distance between two vectors.
  cosine_distance = 1 - dot(a, b) / (|a| * |b|)

  Returns 2.0f when either vector has zero norm (maximum distance).

  @param[in] a  First vector
  @param[in] b  Second vector (must have the same dim as a)
  @return       Cosine distance in [0, 2], or -1.0f on dimension mismatch.
*/
float clawdb_cosine_distance(const ClawdbVector &a, const ClawdbVector &b);

/**
  Dispatch distance computation based on metric type.

  @param[in] a       First vector
  @param[in] b       Second vector
  @param[in] metric  Distance metric to use
  @return            Distance value, or -1.0f on error.
*/
float clawdb_compute_distance(const ClawdbVector &a, const ClawdbVector &b,
                              ClawdbDistanceMetric metric);

#endif  /* STORAGE_CLAWDB_CLAWDB_VEC_H */
