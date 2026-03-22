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

/** @file storage/clawdb/clawdb_hnsw.h
  @brief
  HNSW (Hierarchical Navigable Small World) in-memory index for ClawDB.

  Algorithm reference:
    Malkov, Y. A., & Yashunin, D. A. (2018).
    "Efficient and robust approximate nearest neighbor search using
     Hierarchical Navigable Small World graphs."
    IEEE Transactions on Pattern Analysis and Machine Intelligence.

  This implementation is a self-contained C++ port of the core HNSW
  algorithm from PGVector (src/hnswutils.c, src/hnswbuild.c,
  src/hnswinsert.c), adapted to work without any PostgreSQL runtime
  dependencies.  All memory management uses standard C++ containers.

  Key parameters (aligned with PGVector defaults):
    M              = 16   (max connections per node per layer)
    ef_construction = 64  (candidate list size during build)
    ef_search       = 40  (candidate list size during search)

  The index is rebuilt from the data file on table open and lives
  entirely in memory.  It is persisted implicitly because every insert
  also writes to the ClawdbTableStore; on the next open the index is
  reconstructed by scanning the store.
*/

#ifndef STORAGE_CLAWDB_CLAWDB_HNSW_H
#define STORAGE_CLAWDB_CLAWDB_HNSW_H

#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "clawdb_vec.h"

/* -----------------------------------------------------------------------
   HNSW tuning constants
   ----------------------------------------------------------------------- */

static constexpr int CLAWDB_HNSW_DEFAULT_M = 16;
static constexpr int CLAWDB_HNSW_MIN_M = 2;
static constexpr int CLAWDB_HNSW_MAX_M = 100;

static constexpr int CLAWDB_HNSW_DEFAULT_EF_CONSTRUCTION = 64;
static constexpr int CLAWDB_HNSW_MIN_EF_CONSTRUCTION = 4;
static constexpr int CLAWDB_HNSW_MAX_EF_CONSTRUCTION = 1000;

static constexpr int CLAWDB_HNSW_DEFAULT_EF_SEARCH = 40;
static constexpr int CLAWDB_HNSW_MIN_EF_SEARCH = 1;
static constexpr int CLAWDB_HNSW_MAX_EF_SEARCH = 1000;

/* -----------------------------------------------------------------------
   Internal node representation
   ----------------------------------------------------------------------- */

/** Unique identifier for a node in the HNSW graph.
    Corresponds to the row's file position in ClawdbTableStore. */
using HnswNodeId = uint64_t;

static constexpr HnswNodeId CLAWDB_HNSW_INVALID_NODE =
    std::numeric_limits<HnswNodeId>::max();

/**
  A single node in the HNSW graph.
  Each node stores its vector and per-layer neighbor lists.
*/
struct ClawdbHnswNode {
  HnswNodeId  node_id;    ///< Equals the row's ClawdbRowPosition
  ClawdbVector vector;    ///< The stored vector
  int         level;      ///< Maximum layer this node participates in
  /** neighbors[lc] = list of neighbor node_ids at layer lc */
  std::vector<std::vector<HnswNodeId>> neighbors;

  ClawdbHnswNode() = default;
  ClawdbHnswNode(HnswNodeId id, const ClawdbVector &vec, int max_level, int m);
};

/* -----------------------------------------------------------------------
   Candidate / result types
   ----------------------------------------------------------------------- */

/** A (distance, node_id) pair used in priority queues during search. */
struct HnswCandidate {
  float      distance;
  HnswNodeId node_id;

  /** Min-heap ordering (smallest distance first). */
  bool operator>(const HnswCandidate &other) const {
    return distance > other.distance;
  }

  /** Max-heap ordering (largest distance first). */
  bool operator<(const HnswCandidate &other) const {
    return distance < other.distance;
  }
};

/** Result of a KNN search: ordered list of (distance, node_id) pairs. */
using HnswSearchResult = std::vector<HnswCandidate>;

/* -----------------------------------------------------------------------
   ClawdbHnswIndex
   ----------------------------------------------------------------------- */

/**
  In-memory HNSW index for a single ClawDB table.

  Thread safety: all public methods acquire index_mutex_ internally.
  Callers do not need external locking.
*/
class ClawdbHnswIndex {
 public:
  /**
    Construct an HNSW index with the given parameters.

    @param[in] m                Max connections per node per layer
    @param[in] ef_construction  Candidate list size during insert
    @param[in] ef_search        Candidate list size during search
    @param[in] metric           Distance metric (L2 or COSINE)
  */
  explicit ClawdbHnswIndex(
      int m = CLAWDB_HNSW_DEFAULT_M,
      int ef_construction = CLAWDB_HNSW_DEFAULT_EF_CONSTRUCTION,
      int ef_search = CLAWDB_HNSW_DEFAULT_EF_SEARCH,
      ClawdbDistanceMetric metric = ClawdbDistanceMetric::L2);

  ~ClawdbHnswIndex() = default;

  /* Disable copy; index owns large in-memory state. */
  ClawdbHnswIndex(const ClawdbHnswIndex &) = delete;
  ClawdbHnswIndex &operator=(const ClawdbHnswIndex &) = delete;

  /**
    Insert a vector into the index.

    @param[in] node_id  Unique identifier for this vector (row file position)
    @param[in] vec      The vector to insert
  */
  void insert(HnswNodeId node_id, const ClawdbVector &vec);

  /**
    Remove a node from the index.

    @param[in] node_id  Node to remove
  */
  void remove(HnswNodeId node_id);

  /**
    Search for the k nearest neighbors of a query vector.

    @param[in] query  Query vector
    @param[in] k      Number of nearest neighbors to return
    @return           Up to k results ordered by ascending distance.
  */
  HnswSearchResult search(const ClawdbVector &query, int k) const;

  /** Return the number of nodes currently in the index. */
  size_t size() const;

  /** Remove all nodes from the index. */
  void clear();

  /** Return the distance metric used by this index. */
  ClawdbDistanceMetric metric() const { return metric_; }

 private:
  /* ---- Algorithm helpers (Algorithm 1-5 from the HNSW paper) ---- */

  /**
    Algorithm 5: K-NN search within a single layer.
    Returns ef nearest candidates to query starting from entry_points.
  */
  std::vector<HnswCandidate> search_layer(
      const ClawdbVector &query,
      const std::vector<HnswNodeId> &entry_points,
      int ef,
      int layer) const;

  /**
    Algorithm 4: Select M neighbors from a candidate set using the
    heuristic that prefers diverse neighbors (same as PGVector).
  */
  std::vector<HnswNodeId> select_neighbors(
      const ClawdbVector &query,
      const std::vector<HnswCandidate> &candidates,
      int max_connections) const;

  /** Compute distance between query and the node with the given id. */
  float node_distance(const ClawdbVector &query, HnswNodeId node_id) const;

  /** Return the max layer for a new node (random level generation). */
  int random_level();

  /* ---- State ---- */

  int m_;                   ///< Max connections per layer (M)
  int m0_;                  ///< Max connections at layer 0 (= 2*M)
  int ef_construction_;     ///< ef during build
  int ef_search_;           ///< ef during search
  ClawdbDistanceMetric metric_;

  /** All nodes in the graph, keyed by node_id. */
  std::unordered_map<HnswNodeId, std::unique_ptr<ClawdbHnswNode>> nodes_;

  /** Entry point node_id for the graph (-1 if empty). */
  HnswNodeId entry_point_{CLAWDB_HNSW_INVALID_NODE};

  /** Maximum layer currently in the graph. */
  int max_level_{-1};

  /** Random number generator for level assignment. */
  mutable std::mt19937 rng_;

  /** Protects all mutable state. */
  mutable std::mutex index_mutex_;
};

#endif  /* STORAGE_CLAWDB_CLAWDB_HNSW_H */
