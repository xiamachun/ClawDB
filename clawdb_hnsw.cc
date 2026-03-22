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

/** @file storage/clawdb/clawdb_hnsw.cc
  @brief
  HNSW index implementation for ClawDB.

  Ported from PGVector (src/hnswutils.c, src/hnswbuild.c, src/hnswinsert.c)
  with all PostgreSQL runtime dependencies removed.  Uses standard C++ STL
  containers and algorithms throughout.

  Algorithm references:
    Algorithm 1: INSERT (HnswFindElementNeighbors equivalent)
    Algorithm 2: SEARCH-LAYER (search_layer)
    Algorithm 4: SELECT-NEIGHBORS-HEURISTIC (select_neighbors)
    Algorithm 5: K-NN-SEARCH (search)
*/

#include "clawdb_hnsw.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <limits>
#include <unordered_set>

/* -----------------------------------------------------------------------
   ClawdbHnswNode
   ----------------------------------------------------------------------- */

ClawdbHnswNode::ClawdbHnswNode(HnswNodeId id, const ClawdbVector &vec,
                               int max_level, int m)
    : node_id(id), vector(vec), level(max_level) {
  neighbors.resize(max_level + 1);
  /* Layer 0 gets 2*M connections; upper layers get M connections. */
  neighbors[0].reserve(2 * m);
  for (int lc = 1; lc <= max_level; ++lc) {
    neighbors[lc].reserve(m);
  }
}

/* -----------------------------------------------------------------------
   ClawdbHnswIndex constructor
   ----------------------------------------------------------------------- */

ClawdbHnswIndex::ClawdbHnswIndex(int m, int ef_construction, int ef_search,
                                 ClawdbDistanceMetric metric)
    : m_(m),
      m0_(2 * m),
      ef_construction_(ef_construction),
      ef_search_(ef_search),
      metric_(metric),
      rng_(std::random_device{}()) {}

/* -----------------------------------------------------------------------
   Helpers
   ----------------------------------------------------------------------- */

float ClawdbHnswIndex::node_distance(const ClawdbVector &query,
                                     HnswNodeId node_id) const {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return std::numeric_limits<float>::max();
  return clawdb_compute_distance(query, it->second->vector, metric_);
}

int ClawdbHnswIndex::random_level() {
  /* Optimal level generation: level = floor(-ln(uniform(0,1)) * ml)
     where ml = 1/ln(M).  This matches PGVector's HnswGetMl(m). */
  const double ml = 1.0 / std::log(static_cast<double>(m_));
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  int level = static_cast<int>(-std::log(dist(rng_)) * ml);
  return level;
}

/* -----------------------------------------------------------------------
   Algorithm 2: SEARCH-LAYER
   Returns up to `ef` nearest candidates to `query` at layer `layer`,
   starting from the given entry points.
   ----------------------------------------------------------------------- */

std::vector<HnswCandidate> ClawdbHnswIndex::search_layer(
    const ClawdbVector &query,
    const std::vector<HnswNodeId> &entry_points,
    int ef,
    int layer) const {
  /* C: min-heap of candidates to explore (nearest first). */
  std::priority_queue<HnswCandidate,
                      std::vector<HnswCandidate>,
                      std::greater<HnswCandidate>>
      candidates;

  /* W: max-heap of current result set (farthest first, for pruning). */
  std::priority_queue<HnswCandidate,
                      std::vector<HnswCandidate>,
                      std::less<HnswCandidate>>
      result_set;

  std::unordered_set<HnswNodeId> visited;

  for (HnswNodeId ep_id : entry_points) {
    if (nodes_.find(ep_id) == nodes_.end()) continue;
    float dist = node_distance(query, ep_id);
    candidates.push({dist, ep_id});
    result_set.push({dist, ep_id});
    visited.insert(ep_id);
  }

  while (!candidates.empty()) {
    HnswCandidate closest = candidates.top();
    candidates.pop();

    /* If the closest candidate is farther than the farthest in W, stop. */
    if (!result_set.empty() && closest.distance > result_set.top().distance)
      break;

    auto node_it = nodes_.find(closest.node_id);
    if (node_it == nodes_.end()) continue;

    const ClawdbHnswNode *node = node_it->second.get();
    if (layer > node->level) continue;

    const std::vector<HnswNodeId> &layer_neighbors = node->neighbors[layer];

    for (HnswNodeId neighbor_id : layer_neighbors) {
      if (visited.count(neighbor_id)) continue;
      visited.insert(neighbor_id);

      float neighbor_dist = node_distance(query, neighbor_id);
      float farthest_in_result =
          result_set.empty() ? std::numeric_limits<float>::max()
                             : result_set.top().distance;

      if (neighbor_dist < farthest_in_result ||
          static_cast<int>(result_set.size()) < ef) {
        candidates.push({neighbor_dist, neighbor_id});
        result_set.push({neighbor_dist, neighbor_id});

        /* Prune result set to ef elements. */
        if (static_cast<int>(result_set.size()) > ef) {
          result_set.pop();
        }
      }
    }
  }

  /* Convert result_set (max-heap) to a sorted vector (ascending distance). */
  std::vector<HnswCandidate> results;
  results.reserve(result_set.size());
  while (!result_set.empty()) {
    results.push_back(result_set.top());
    result_set.pop();
  }
  std::sort(results.begin(), results.end(),
            [](const HnswCandidate &a, const HnswCandidate &b) {
              return a.distance < b.distance;
            });
  return results;
}

/* -----------------------------------------------------------------------
   Algorithm 4: SELECT-NEIGHBORS-HEURISTIC
   Selects up to max_connections diverse neighbors from candidates.
   Prefers neighbors that are closer to the query than to each other
   (same heuristic as PGVector's SelectNeighbors).
   ----------------------------------------------------------------------- */

std::vector<HnswNodeId> ClawdbHnswIndex::select_neighbors(
    const ClawdbVector &query,
    const std::vector<HnswCandidate> &candidates,
    int max_connections) const {
  /* query is not used in this simplified heuristic; suppress the warning. */
  (void)query;

  if (static_cast<int>(candidates.size()) <= max_connections) {
    std::vector<HnswNodeId> result;
    result.reserve(candidates.size());
    for (const auto &c : candidates) result.push_back(c.node_id);
    return result;
  }

  /* Working set: sorted ascending by distance to query. */
  std::vector<HnswCandidate> working_set = candidates;
  std::sort(working_set.begin(), working_set.end(),
            [](const HnswCandidate &a, const HnswCandidate &b) {
              return a.distance < b.distance;
            });

  std::vector<HnswNodeId> result;
  result.reserve(max_connections);

  /* Discarded candidates (pruned but kept for filling up to max_connections). */
  std::vector<HnswCandidate> discarded;

  for (const auto &candidate : working_set) {
    if (static_cast<int>(result.size()) >= max_connections) break;

    auto candidate_node_it = nodes_.find(candidate.node_id);
    if (candidate_node_it == nodes_.end()) continue;

    const ClawdbVector &candidate_vec = candidate_node_it->second->vector;
    bool is_closer_to_query = true;

    /* Check if candidate is closer to query than to any already-selected
       neighbor.  This is the "diverse neighbor" heuristic from the paper. */
    for (HnswNodeId selected_id : result) {
      auto selected_it = nodes_.find(selected_id);
      if (selected_it == nodes_.end()) continue;

      float dist_to_selected =
          clawdb_compute_distance(candidate_vec, selected_it->second->vector,
                                  metric_);
      if (dist_to_selected <= candidate.distance) {
        is_closer_to_query = false;
        break;
      }
    }

    if (is_closer_to_query) {
      result.push_back(candidate.node_id);
    } else {
      discarded.push_back(candidate);
    }
  }

  /* Fill remaining slots with discarded candidates (pruned connections). */
  for (const auto &d : discarded) {
    if (static_cast<int>(result.size()) >= max_connections) break;
    result.push_back(d.node_id);
  }

  return result;
}

/* -----------------------------------------------------------------------
   Algorithm 1: INSERT
   ----------------------------------------------------------------------- */

void ClawdbHnswIndex::insert(HnswNodeId node_id, const ClawdbVector &vec) {
  std::unique_lock<std::mutex> lock(index_mutex_);

  int new_level = random_level();

  auto new_node = std::make_unique<ClawdbHnswNode>(node_id, vec, new_level, m_);

  if (entry_point_ == CLAWDB_HNSW_INVALID_NODE) {
    /* First node: becomes the entry point. */
    entry_point_ = node_id;
    max_level_ = new_level;
    nodes_[node_id] = std::move(new_node);
    return;
  }

  /* Phase 1: Greedy descent from max_level_ down to new_level + 1.
     Find the single closest entry point at each layer. */
  std::vector<HnswNodeId> entry_points = {entry_point_};

  for (int lc = max_level_; lc > new_level; --lc) {
    auto layer_results = search_layer(vec, entry_points, 1, lc);
    if (!layer_results.empty()) {
      entry_points = {layer_results[0].node_id};
    }
  }

  /* Phase 2: For each layer from min(new_level, max_level_) down to 0,
     find ef_construction_ nearest neighbors and connect. */
  int top_layer = std::min(new_level, max_level_);

  for (int lc = top_layer; lc >= 0; --lc) {
    int layer_m = (lc == 0) ? m0_ : m_;

    auto layer_candidates = search_layer(vec, entry_points, ef_construction_, lc);

    /* Select neighbors for the new node at this layer. */
    std::vector<HnswNodeId> selected =
        select_neighbors(vec, layer_candidates, layer_m);

    /* Connect new node -> selected neighbors. */
    new_node->neighbors[lc] = selected;

    /* Connect selected neighbors -> new node (bidirectional). */
    for (HnswNodeId neighbor_id : selected) {
      auto neighbor_it = nodes_.find(neighbor_id);
      if (neighbor_it == nodes_.end()) continue;

      ClawdbHnswNode *neighbor = neighbor_it->second.get();
      if (lc > neighbor->level) continue;

      std::vector<HnswNodeId> &neighbor_layer_neighbors =
          neighbor->neighbors[lc];
      int max_conn = (lc == 0) ? m0_ : m_;

      neighbor_layer_neighbors.push_back(node_id);

      /* Prune neighbor's connections if they exceed max_connections. */
      if (static_cast<int>(neighbor_layer_neighbors.size()) > max_conn) {
        /* Build candidate list from current neighbors + new node. */
        std::vector<HnswCandidate> prune_candidates;
        prune_candidates.reserve(neighbor_layer_neighbors.size());
        for (HnswNodeId conn_id : neighbor_layer_neighbors) {
          float dist = clawdb_compute_distance(
              neighbor->vector,
              nodes_.count(conn_id) ? nodes_.at(conn_id)->vector : vec,
              metric_);
          prune_candidates.push_back({dist, conn_id});
        }
        neighbor_layer_neighbors =
            select_neighbors(neighbor->vector, prune_candidates, max_conn);
      }
    }

    /* Advance entry points for the next (lower) layer. */
    if (!layer_candidates.empty()) {
      entry_points.clear();
      for (const auto &c : layer_candidates) {
        entry_points.push_back(c.node_id);
      }
    }
  }

  /* Update entry point if new node has a higher level. */
  if (new_level > max_level_) {
    max_level_ = new_level;
    entry_point_ = node_id;
  }

  nodes_[node_id] = std::move(new_node);
}

/* -----------------------------------------------------------------------
   Algorithm 5: K-NN-SEARCH
   ----------------------------------------------------------------------- */

HnswSearchResult ClawdbHnswIndex::search(const ClawdbVector &query,
                                         int k) const {
  std::unique_lock<std::mutex> lock(index_mutex_);

  if (entry_point_ == CLAWDB_HNSW_INVALID_NODE || nodes_.empty()) {
    return {};
  }

  /* Phase 1: Greedy descent from max_level_ down to layer 1. */
  std::vector<HnswNodeId> entry_points = {entry_point_};

  for (int lc = max_level_; lc >= 1; --lc) {
    auto layer_results = search_layer(query, entry_points, 1, lc);
    if (!layer_results.empty()) {
      entry_points = {layer_results[0].node_id};
    }
  }

  /* Phase 2: Search layer 0 with ef_search_ candidates. */
  int ef = std::max(ef_search_, k);
  auto layer0_results = search_layer(query, entry_points, ef, 0);

  /* Return top-k results. */
  if (static_cast<int>(layer0_results.size()) > k) {
    layer0_results.resize(k);
  }

  return layer0_results;
}

/* -----------------------------------------------------------------------
   Utility methods
   ----------------------------------------------------------------------- */

void ClawdbHnswIndex::remove(HnswNodeId node_id) {
  std::unique_lock<std::mutex> lock(index_mutex_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) return;

  /* Remove this node from all its neighbors' connection lists. */
  const ClawdbHnswNode *node = it->second.get();
  for (int lc = 0; lc <= node->level; ++lc) {
    for (HnswNodeId neighbor_id : node->neighbors[lc]) {
      auto neighbor_it = nodes_.find(neighbor_id);
      if (neighbor_it == nodes_.end()) continue;

      std::vector<HnswNodeId> &neighbor_connections =
          neighbor_it->second->neighbors[lc];
      neighbor_connections.erase(
          std::remove(neighbor_connections.begin(), neighbor_connections.end(),
                      node_id),
          neighbor_connections.end());
    }
  }

  /* If this was the entry point, pick a new one. */
  if (entry_point_ == node_id) {
    if (nodes_.size() == 1) {
      entry_point_ = CLAWDB_HNSW_INVALID_NODE;
      max_level_ = -1;
    } else {
      /* Find the node with the highest level as the new entry point. */
      HnswNodeId new_ep = CLAWDB_HNSW_INVALID_NODE;
      int new_max_level = -1;
      for (const auto &kv : nodes_) {
        if (kv.first == node_id) continue;
        if (kv.second->level > new_max_level) {
          new_max_level = kv.second->level;
          new_ep = kv.first;
        }
      }
      entry_point_ = new_ep;
      max_level_ = new_max_level;
    }
  }

  nodes_.erase(it);
}

size_t ClawdbHnswIndex::size() const {
  std::unique_lock<std::mutex> lock(index_mutex_);
  return nodes_.size();
}

void ClawdbHnswIndex::clear() {
  std::unique_lock<std::mutex> lock(index_mutex_);
  nodes_.clear();
  entry_point_ = CLAWDB_HNSW_INVALID_NODE;
  max_level_ = -1;
}
