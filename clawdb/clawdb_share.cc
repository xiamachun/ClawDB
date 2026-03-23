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

/** @file storage/clawdb/clawdb_share.cc
  @brief
  Implementation of shared state management for ClawDB tables.

  This file implements the ClawdbShare class constructor/destructor,
  global share management functions, and HNSW comment parsing utilities.
*/

#include "clawdb_share.h"
#include "my_dbug.h"

/* -----------------------------------------------------------------------
   Global share management
   ----------------------------------------------------------------------- */

std::mutex global_share_mutex;
std::unordered_map<std::string, ClawdbShare *> global_share_map;

/* -----------------------------------------------------------------------
   ClawdbShare implementation
   ----------------------------------------------------------------------- */

ClawdbShare::ClawdbShare(const std::string &name) : table_name(name) {
  thr_lock_init(&lock);
  store = std::make_unique<ClawdbTableStore>();
  hnsw = std::make_unique<ClawdbHnswIndex>(
      CLAWDB_HNSW_DEFAULT_M, CLAWDB_HNSW_DEFAULT_EF_CONSTRUCTION,
      CLAWDB_HNSW_DEFAULT_EF_SEARCH, ClawdbDistanceMetric::L2);
}

ClawdbShare::~ClawdbShare() { thr_lock_delete(&lock); }

ClawdbShare *clawdb_get_share(const char *table_name) {
  DBUG_TRACE;

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

void clawdb_close_all_shares() {
  DBUG_TRACE;

  std::unique_lock<std::mutex> lock(global_share_mutex);
  for (auto &kv : global_share_map) {
    kv.second->store->close();
    delete kv.second;
  }
  global_share_map.clear();
}

/* -----------------------------------------------------------------------
   HNSW index COMMENT parser helpers
   ----------------------------------------------------------------------- */

/**
  Helper: convert a string to lowercase in-place.
*/
static void clawdb_str_to_lower(std::string &str) {
  for (char &ch : str) {
    if (ch >= 'A' && ch <= 'Z') ch = static_cast<char>(ch + ('a' - 'A'));
  }
}

/**
  Helper: trim leading and trailing whitespace from a string.
*/
static std::string clawdb_str_trim(const std::string &str) {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) return "";
  size_t end = str.find_last_not_of(" \t\r\n");
  return str.substr(start, end - start + 1);
}

/* -----------------------------------------------------------------------
   HNSW index COMMENT parser
   ----------------------------------------------------------------------- */

bool clawdb_parse_hnsw_comment(const char *comment_str, size_t comment_len,
                               ClawdbHnswParams *params) {
  DBUG_TRACE;

  if (comment_str == nullptr || comment_len == 0 || params == nullptr) {
    return true;  /* No comment is not an error; use defaults. */
  }

  std::string comment(comment_str, comment_len);
  std::string comment_lower = comment;
  clawdb_str_to_lower(comment_lower);

  /* Look for the 'hnsw(' prefix (case-insensitive). */
  size_t hnsw_pos = comment_lower.find("hnsw(");
  if (hnsw_pos == std::string::npos) {
    /* No HNSW prefix found; this is a plain comment, not an error. */
    return true;
  }

  params->has_vector_index = true;

  /* Extract the content between 'hnsw(' and the matching ')'. */
  size_t content_start = hnsw_pos + 5;  /* length of "hnsw(" */
  size_t paren_end = comment.find(')', content_start);
  if (paren_end == std::string::npos) {
    /* Malformed: missing closing parenthesis.  Use defaults. */
    DBUG_PRINT("warning", ("ClawDB: malformed HNSW comment, missing closing parenthesis"));
    return true;
  }

  std::string params_str = comment.substr(content_start,
                                          paren_end - content_start);

  /* Parse comma-separated key=value pairs. */
  size_t pos = 0;
  while (pos < params_str.size()) {
    /* Skip whitespace and commas. */
    while (pos < params_str.size() &&
           (params_str[pos] == ' ' || params_str[pos] == '\t' ||
            params_str[pos] == ',')) {
      ++pos;
    }
    if (pos >= params_str.size()) break;

    /* Find '=' separator. */
    size_t eq_pos = params_str.find('=', pos);
    if (eq_pos == std::string::npos) break;

    std::string key = clawdb_str_trim(params_str.substr(pos, eq_pos - pos));
    clawdb_str_to_lower(key);

    /* Find the end of the value (next comma or end of string). */
    size_t value_start = eq_pos + 1;
    size_t comma_pos = params_str.find(',', value_start);
    size_t value_end = (comma_pos != std::string::npos)
                           ? comma_pos
                           : params_str.size();
    std::string value = clawdb_str_trim(
        params_str.substr(value_start, value_end - value_start));
    std::string value_lower = value;
    clawdb_str_to_lower(value_lower);

    /* Apply recognized keys. */
    if (key == "metric") {
      if (value_lower == "cosine") {
        params->metric = ClawdbDistanceMetric::COSINE;
      } else if (value_lower == "l2" || value_lower == "euclidean") {
        params->metric = ClawdbDistanceMetric::L2;
      } else {
        DBUG_PRINT("warning", ("ClawDB: unknown metric, using L2"));
      }
    } else if (key == "m") {
      int parsed_m = std::atoi(value.c_str());
      if (parsed_m >= CLAWDB_HNSW_MIN_M && parsed_m <= CLAWDB_HNSW_MAX_M) {
        params->m = parsed_m;
      } else {
        DBUG_PRINT("warning", ("ClawDB: m out of range, using default"));
      }
    } else if (key == "ef_construction") {
      int parsed_ef = std::atoi(value.c_str());
      if (parsed_ef >= CLAWDB_HNSW_MIN_EF_CONSTRUCTION &&
          parsed_ef <= CLAWDB_HNSW_MAX_EF_CONSTRUCTION) {
        params->ef_construction = parsed_ef;
      } else {
        DBUG_PRINT("warning", ("ClawDB: ef_construction out of range, using default"));
      }
    } else if (key == "ef_search") {
      int parsed_ef = std::atoi(value.c_str());
      if (parsed_ef >= CLAWDB_HNSW_MIN_EF_SEARCH &&
          parsed_ef <= CLAWDB_HNSW_MAX_EF_SEARCH) {
        params->ef_search = parsed_ef;
      } else {
        DBUG_PRINT("warning", ("ClawDB: ef_search out of range, using default"));
      }
    }
    /* Unknown keys are silently ignored for forward compatibility. */

    pos = (comma_pos != std::string::npos) ? comma_pos + 1 : params_str.size();
  }

  return true;
}
