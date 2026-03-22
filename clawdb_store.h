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

/** @file storage/clawdb/clawdb_store.h
  @brief
  ClawDB row storage layer.

  Each table is stored as a single flat file: <db>/<table>.clawdb
  File layout:
    [ClawdbFileHeader]
    [ClawdbRowHeader][row_data bytes] ...  (repeated for each row)

  Row data is a raw MySQL row buffer (the same format that handler::write_row
  receives).  We store the full MySQL row so that all column types (INT, BLOB,
  TEXT, etc.) are preserved verbatim, and the engine does not need to
  understand individual field encodings beyond the BLOB column that carries
  the vector payload.

  Deleted rows are marked with a tombstone flag in ClawdbRowHeader; they are
  reclaimed on the next OPTIMIZE TABLE (not yet implemented) or when the table
  is recreated.
*/

#ifndef STORAGE_CLAWDB_CLAWDB_STORE_H
#define STORAGE_CLAWDB_CLAWDB_STORE_H

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

/* -----------------------------------------------------------------------
   File format constants
   ----------------------------------------------------------------------- */

/** Magic number written at the start of every .clawdb data file. */
static constexpr uint32_t CLAWDB_FILE_MAGIC = 0xC1ADB001;

/** Current file format version. */
static constexpr uint16_t CLAWDB_FILE_VERSION = 1;

/** Size of the fixed file header in bytes. */
static constexpr size_t CLAWDB_FILE_HEADER_SIZE = 16;

/** Row tombstone flag: row is alive. */
static constexpr uint8_t CLAWDB_ROW_ALIVE = 0x01;

/** Row tombstone flag: row has been deleted. */
static constexpr uint8_t CLAWDB_ROW_DELETED = 0x00;

/* -----------------------------------------------------------------------
   On-disk structures
   ----------------------------------------------------------------------- */

#pragma pack(push, 1)

/** Fixed-size header at the beginning of each .clawdb data file. */
struct ClawdbFileHeader {
  uint32_t magic;        ///< Must equal CLAWDB_FILE_MAGIC
  uint16_t version;      ///< File format version
  uint16_t reserved;     ///< Padding, always 0
  uint64_t row_count;    ///< Total number of live rows
};

static_assert(sizeof(ClawdbFileHeader) == CLAWDB_FILE_HEADER_SIZE,
              "ClawdbFileHeader size mismatch");

/** Per-row header stored immediately before each row's data bytes. */
struct ClawdbRowHeader {
  uint8_t  flags;        ///< CLAWDB_ROW_ALIVE or CLAWDB_ROW_DELETED
  uint32_t data_length;  ///< Length of the row data that follows, in bytes
};

#pragma pack(pop)

/* -----------------------------------------------------------------------
   Row position type
   ----------------------------------------------------------------------- */

/**
  Identifies the file offset of a row's ClawdbRowHeader.
  Used by handler::position() / handler::rnd_pos().
*/
using ClawdbRowPosition = uint64_t;

static constexpr ClawdbRowPosition CLAWDB_INVALID_POSITION =
    static_cast<ClawdbRowPosition>(-1);

/* -----------------------------------------------------------------------
   ClawdbTableStore
   ----------------------------------------------------------------------- */

/**
  Manages the on-disk storage for a single ClawDB table.

  One instance is shared among all open handler instances for the same
  table (via ClawdbShare).  All public methods are protected by the
  caller holding the share mutex (thr_lock).
*/
class ClawdbTableStore {
 public:
  ClawdbTableStore() = default;
  ~ClawdbTableStore();

  /* Disable copy; this object owns a FILE* resource. */
  ClawdbTableStore(const ClawdbTableStore &) = delete;
  ClawdbTableStore &operator=(const ClawdbTableStore &) = delete;

  /**
    Open (or create) the data file for a table.

    @param[in] file_path  Full path to the .clawdb file.
    @param[in] create     If true, create the file if it does not exist.
    @return 0 on success, errno on failure.
  */
  int open(const std::string &file_path, bool create);

  /** Close the data file. */
  void close();

  /** Return true if the store is currently open. */
  bool is_open() const { return data_file_ != nullptr; }

  /**
    Append a new row to the data file.

    @param[in] row_data   Raw MySQL row buffer
    @param[in] row_length Length of row_data in bytes
    @return 0 on success, errno on failure.
  */
  int append_row(const unsigned char *row_data, uint32_t row_length);

  /**
    Read the row at the given file position.

    @param[in]  position   File offset of the row's ClawdbRowHeader
    @param[out] row_data   Buffer to receive the row bytes
    @param[out] row_length Number of bytes written to row_data
    @return 0 on success, HA_ERR_END_OF_FILE if position is past EOF,
            errno on I/O error.
  */
  int read_row_at(ClawdbRowPosition position, std::vector<unsigned char> *row_data,
                  uint32_t *row_length);

  /**
    Mark the row at the given position as deleted.

    @param[in] position  File offset of the row's ClawdbRowHeader
    @return 0 on success, errno on failure.
  */
  int delete_row_at(ClawdbRowPosition position);

  /**
    Update the row at the given position with new data.

    If the new row is the same size as the old row, it is written in-place.
    Otherwise the old row is tombstoned and the new row is appended.

    @param[in]  position       File offset of the existing row's header
    @param[in]  new_row_data   New raw MySQL row buffer
    @param[in]  new_row_length Length of new_row_data in bytes
    @param[out] new_position   File offset of the written row (may differ
                               from position if the row was relocated)
    @return 0 on success, errno on failure.
  */
  int update_row_at(ClawdbRowPosition position,
                    const unsigned char *new_row_data,
                    uint32_t new_row_length,
                    ClawdbRowPosition *new_position);

  /**
    Scan: return the position of the first live row at or after `from`.

    @param[in]  from      Starting file offset (use 0 to start from beginning,
                          or pass the previous position + row_size to advance)
    @param[out] position  File offset of the next live row header
    @return 0 on success, HA_ERR_END_OF_FILE when no more rows exist,
            errno on I/O error.
  */
  int next_live_row(ClawdbRowPosition from, ClawdbRowPosition *position);

  /**
    Return the file offset immediately after the data file header.
    This is the starting position for a full-table scan.
  */
  ClawdbRowPosition scan_start_position() const {
    return static_cast<ClawdbRowPosition>(CLAWDB_FILE_HEADER_SIZE);
  }

  /** Return the number of live rows (as recorded in the file header). */
  uint64_t row_count() const { return header_.row_count; }

  /** Delete all rows (truncate the data file to just the header). */
  int truncate();

  /** Flush pending writes to disk. */
  int flush();

  /**
    Append a new row and return its file position.

    Identical to append_row() but also returns the file offset at which
    the new row's ClawdbRowHeader was written.  This is the value that
    should be passed to the HNSW index so that the index entry can be
    correlated back to the correct row on disk.

    @param[in]  row_data     Raw MySQL row buffer
    @param[in]  row_length   Length of row_data in bytes
    @param[out] new_position File offset of the newly written row header
    @return 0 on success, errno on failure.
  */
  int append_row_at(const unsigned char *row_data, uint32_t row_length,
                    ClawdbRowPosition *new_position);

 private:
  /** Write the in-memory header back to offset 0 of the file. */
  int write_header();

  /** Read the file header from disk into header_. */
  int read_header();

  FILE *data_file_{nullptr};
  std::string file_path_;
  ClawdbFileHeader header_{};
};

#endif  /* STORAGE_CLAWDB_CLAWDB_STORE_H */
