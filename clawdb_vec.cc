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

/** @file storage/clawdb/clawdb_vec.cc

  @author Amazon.Xia
  @date   2026-03-22

  @brief
  ClawDB vector type: parsing, serialization, and distance computation.
*/

#include "clawdb_vec.h"

#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <sstream>

/* -----------------------------------------------------------------------
   Parsing
   ----------------------------------------------------------------------- */

bool clawdb_parse_vector_string(const char *str, ClawdbVector *vec,
                                std::string *errmsg) {
  assert(str != nullptr);
  assert(vec != nullptr);

  /* Skip leading whitespace. */
  while (*str == ' ' || *str == '\t') ++str;

  if (*str != '[') {
    if (errmsg) *errmsg = "vector string must start with '['";
    return false;
  }
  ++str;  /* consume '[' */

  std::vector<float> elements;
  elements.reserve(128);

  while (true) {
    /* Skip whitespace and commas. */
    while (*str == ' ' || *str == '\t' || *str == ',') ++str;

    if (*str == ']') {
      ++str;  /* consume ']' */
      break;
    }

    if (*str == '\0') {
      if (errmsg) *errmsg = "unexpected end of string, missing ']'";
      return false;
    }

    /* Parse one float value. */
    char *end_ptr = nullptr;
    errno = 0;
    float value = std::strtof(str, &end_ptr);

    if (end_ptr == str) {
      if (errmsg) {
        *errmsg = std::string("cannot parse float near: ") + str;
      }
      return false;
    }

    if (errno == ERANGE) {
      if (errmsg) *errmsg = "float value out of range";
      return false;
    }

    elements.push_back(value);
    str = end_ptr;

    if (elements.size() > CLAWDB_VECTOR_MAX_DIM) {
      if (errmsg) {
        *errmsg = std::string("vector exceeds maximum dimension ") +
                  std::to_string(CLAWDB_VECTOR_MAX_DIM);
      }
      return false;
    }
  }

  if (elements.empty()) {
    if (errmsg) *errmsg = "vector must have at least 1 dimension";
    return false;
  }

  vec->dim = static_cast<uint32_t>(elements.size());
  vec->values = std::move(elements);
  return true;
}

/* -----------------------------------------------------------------------
   Serialization
   ----------------------------------------------------------------------- */

void clawdb_serialize_vector(const ClawdbVector &vec, unsigned char *blob) {
  assert(blob != nullptr);

  /* Write dimension as little-endian uint32_t. */
  uint32_t dim = vec.dim;
  std::memcpy(blob, &dim, sizeof(uint32_t));
  blob += sizeof(uint32_t);

  /* Write float elements. */
  std::memcpy(blob, vec.values.data(), vec.dim * sizeof(float));
}

bool clawdb_deserialize_vector(const unsigned char *blob, size_t blob_len,
                               ClawdbVector *vec, std::string *errmsg) {
  assert(blob != nullptr);
  assert(vec != nullptr);

  if (blob_len < CLAWDB_VECTOR_HEADER_SIZE) {
    if (errmsg) *errmsg = "blob too short to contain vector header";
    return false;
  }

  uint32_t dim = 0;
  std::memcpy(&dim, blob, sizeof(uint32_t));

  if (dim == 0) {
    if (errmsg) *errmsg = "vector dimension must be > 0";
    return false;
  }

  if (dim > CLAWDB_VECTOR_MAX_DIM) {
    if (errmsg) {
      *errmsg = std::string("vector dimension ") + std::to_string(dim) +
                " exceeds maximum " + std::to_string(CLAWDB_VECTOR_MAX_DIM);
    }
    return false;
  }

  const size_t expected_size = clawdb_vector_byte_size(dim);
  if (blob_len < expected_size) {
    if (errmsg) {
      *errmsg = std::string("blob length ") + std::to_string(blob_len) +
                " is too short for " + std::to_string(dim) +
                "-dimensional vector (need " + std::to_string(expected_size) +
                " bytes)";
    }
    return false;
  }

  vec->dim = dim;
  vec->values.resize(dim);
  std::memcpy(vec->values.data(), blob + sizeof(uint32_t), dim * sizeof(float));
  return true;
}

std::string clawdb_vector_to_string(const ClawdbVector &vec) {
  std::ostringstream oss;
  oss << '[';
  for (uint32_t i = 0; i < vec.dim; ++i) {
    if (i > 0) oss << ',';
    oss << vec.values[i];
  }
  oss << ']';
  return oss.str();
}

/* -----------------------------------------------------------------------
   Distance functions
   ----------------------------------------------------------------------- */

float clawdb_l2_distance(const ClawdbVector &a, const ClawdbVector &b) {
  if (a.dim != b.dim) return -1.0f;

  float squared_sum = 0.0f;
  const float *pa = a.values.data();
  const float *pb = b.values.data();
  const uint32_t dim = a.dim;

  for (uint32_t i = 0; i < dim; ++i) {
    float diff = pa[i] - pb[i];
    squared_sum += diff * diff;
  }

  return squared_sum;
}

float clawdb_cosine_distance(const ClawdbVector &a, const ClawdbVector &b) {
  if (a.dim != b.dim) return -1.0f;

  float dot_product = 0.0f;
  float norm_a = 0.0f;
  float norm_b = 0.0f;

  const float *pa = a.values.data();
  const float *pb = b.values.data();
  const uint32_t dim = a.dim;

  for (uint32_t i = 0; i < dim; ++i) {
    dot_product += pa[i] * pb[i];
    norm_a += pa[i] * pa[i];
    norm_b += pb[i] * pb[i];
  }

  if (norm_a == 0.0f || norm_b == 0.0f) {
    /* Zero-norm vector: maximum distance. */
    return 2.0f;
  }

  float cosine_similarity = dot_product / (std::sqrt(norm_a) * std::sqrt(norm_b));

  /* Clamp to [-1, 1] to guard against floating-point rounding. */
  if (cosine_similarity > 1.0f) cosine_similarity = 1.0f;
  if (cosine_similarity < -1.0f) cosine_similarity = -1.0f;

  return 1.0f - cosine_similarity;
}

float clawdb_compute_distance(const ClawdbVector &a, const ClawdbVector &b,
                              ClawdbDistanceMetric metric) {
  switch (metric) {
    case ClawdbDistanceMetric::L2:
      return clawdb_l2_distance(a, b);
    case ClawdbDistanceMetric::COSINE:
      return clawdb_cosine_distance(a, b);
  }
  return -1.0f;
}
