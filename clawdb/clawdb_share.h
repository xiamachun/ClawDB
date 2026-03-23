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

/** @file storage/clawdb/clawdb_share.h
  @brief
  Shared state management for ClawDB tables.

  This file defines the ClawdbShare class which maintains shared state
  across all open handler instances for a single table, including the
  row storage, HNSW index, and HNSW parameters parsed from index comments.
*/

#ifndef STORAGE_CLAWDB_CLAWDB_SHARE_H
#define STORAGE_CLAWDB_CLAWDB_SHARE_H

#include "clawdb_compat.h"
#include "clawdb_hnsw.h"
#include "clawdb_store.h"
#include "clawdb_vec.h"
#include "sql/handler.h"
#include "thr_lock.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

/* -----------------------------------------------------------------------
   ClawdbHnswParams: parsed HNSW index parameters
   ----------------------------------------------------------------------- */

/**
  Parsed HNSW index parameters from CREATE INDEX ... COMMENT 'HNSW(...)'.

  Users can customize the HNSW index behavior by creating an index on
  the BLOB (vector) column with a specially formatted COMMENT string:

    CREATE INDEX vec_idx ON tbl(embedding(768))
      COMMENT 'HNSW(metric=cosine, m=32, ef_construction=200, ef_search=100)';

  All parameters are optional; unspecified ones use HNSW defaults.
*/
struct ClawdbHnswParams {
  int m{CLAWDB_HNSW_DEFAULT_M};
  int ef_construction{CLAWDB_HNSW_DEFAULT_EF_CONSTRUCTION};
  int ef_search{CLAWDB_HNSW_DEFAULT_EF_SEARCH};
  ClawdbDistanceMetric metric{ClawdbDistanceMetric::L2};
  bool has_vector_index{false};  ///< true if a HNSW index was declared
};

/**
  Parse an HNSW parameter string from an index COMMENT.

  Accepted format (case-insensitive, whitespace-tolerant):
    HNSW(key=value, key=value, ...)

  Recognized keys:
    metric           - 'l2', 'euclidean', or 'cosine'
    m                - integer in [2, 100]
    ef_construction  - integer in [4, 1000]
    ef_search        - integer in [1, 1000]

  @param[in]  comment_str  The raw COMMENT string from the KEY definition.
  @param[in]  comment_len  Length of comment_str.
  @param[out] params       Populated with parsed values on success.
  @return true on success (even if no HNSW prefix found — params stay default),
          false on parse error (malformed values).
*/
bool clawdb_parse_hnsw_comment(const char *comment_str, size_t comment_len,
                               ClawdbHnswParams *params);

/* -----------------------------------------------------------------------
   ClawdbShare: shared state across all open handlers for one table
   ----------------------------------------------------------------------- */

/**
  Shared state for a single ClawDB table.

  One ClawdbShare instance exists per open table, shared among all
  concurrent handler instances.  Protected by the THR_LOCK and by
  share_mutex for the HNSW index and store.
*/
class ClawdbShare : public Handler_share {
 public:
  THR_LOCK lock;                          ///< MySQL table-level lock
  std::string table_name;                 ///< Fully qualified table name
  std::unique_ptr<ClawdbTableStore> store; ///< Row storage
  std::unique_ptr<ClawdbHnswIndex> hnsw;  ///< In-memory HNSW index
  std::mutex share_mutex;                 ///< Protects store and hnsw
  ClawdbHnswParams hnsw_params;           ///< HNSW parameters from index COMMENT

  /** Path to the .hnsw index file (parallel to the .clawdb data file). */
  std::string hnsw_file_path;

  /** True when the in-memory HNSW index has been modified since the last
      save to disk.  Checked in close() to decide whether to persist. */
  bool hnsw_dirty{false};

  /** Number of handler instances currently referencing this share.
      When it drops to zero, the HNSW index is saved if dirty. */
  int open_count{0};

  explicit ClawdbShare(const std::string &name);
  ~ClawdbShare() override;
};

/* -----------------------------------------------------------------------
   Global share management
   ----------------------------------------------------------------------- */

/**
  Get or create the ClawdbShare for a table.

  @param[in]  table_name  Fully qualified table name (db/table).
  @return Pointer to the shared ClawdbShare instance.
*/
ClawdbShare *clawdb_get_share(const char *table_name);

/**
  Close and free all open shares.

  Called during plugin deinitialization to clean up all resources.
*/
void clawdb_close_all_shares();

/** Global mutex protecting the share map. */
extern std::mutex global_share_mutex;

/** Global map from table name to ClawdbShare instance. */
extern std::unordered_map<std::string, ClawdbShare *> global_share_map;

#endif  /* STORAGE_CLAWDB_CLAWDB_SHARE_H */
