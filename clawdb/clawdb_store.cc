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

/** @file storage/clawdb/clawdb_store.cc
  @brief
  ClawDB row storage implementation.

  File layout:
    [ClawdbFileHeader 16 bytes]
    [ClawdbRowHeader 5 bytes][row_data N bytes]  (repeated)

  All multi-byte integers are stored in native byte order (the file is
  not intended to be portable across architectures; it lives on the
  same server that wrote it).
*/

#include "clawdb_store.h"

#include <cerrno>
#include <cstring>
#include <sys/types.h>

/* MySQL error codes we need. */
#include "my_base.h"
#include "my_sys.h"

/* -----------------------------------------------------------------------
   ClawdbTableStore
   ----------------------------------------------------------------------- */

ClawdbTableStore::~ClawdbTableStore() { close(); }

int ClawdbTableStore::open(const std::string &file_path, bool create) {
  file_path_ = file_path;

  if (create) {
    /* Open for read+write, create if not exists. */
    data_file_ = std::fopen(file_path_.c_str(), "r+b");
    if (data_file_ == nullptr) {
      /* File does not exist yet; create it. */
      data_file_ = std::fopen(file_path_.c_str(), "w+b");
      if (data_file_ == nullptr) return errno;

      /* Write a fresh header. */
      header_.magic = CLAWDB_FILE_MAGIC;
      header_.version = CLAWDB_FILE_VERSION;
      header_.reserved = 0;
      header_.row_count = 0;
      return write_header();
    }
  } else {
    data_file_ = std::fopen(file_path_.c_str(), "r+b");
    if (data_file_ == nullptr) return errno;
  }

  return read_header();
}

void ClawdbTableStore::close() {
  if (data_file_ != nullptr) {
    std::fflush(data_file_);
    std::fclose(data_file_);
    data_file_ = nullptr;
  }
}

int ClawdbTableStore::write_header() {
  if (fseeko(data_file_, 0, SEEK_SET) != 0) return errno;
  if (std::fwrite(&header_, sizeof(ClawdbFileHeader), 1, data_file_) != 1)
    return errno;
  return 0;
}

int ClawdbTableStore::read_header() {
  if (fseeko(data_file_, 0, SEEK_SET) != 0) return errno;
  if (std::fread(&header_, sizeof(ClawdbFileHeader), 1, data_file_) != 1) {
    return errno ? errno : EIO;
  }
  if (header_.magic != CLAWDB_FILE_MAGIC) return EINVAL;
  return 0;
}

int ClawdbTableStore::append_row(const unsigned char *row_data,
                                 uint32_t row_length) {
  /* Seek to end of file. */
  if (fseeko(data_file_, 0, SEEK_END) != 0) return errno;

  ClawdbRowHeader row_header;
  row_header.flags = CLAWDB_ROW_ALIVE;
  row_header.data_length = row_length;

  if (std::fwrite(&row_header, sizeof(ClawdbRowHeader), 1, data_file_) != 1)
    return errno;

  if (std::fwrite(row_data, 1, row_length, data_file_) != row_length)
    return errno;

  /* Update live row count in header. */
  ++header_.row_count;
  return write_header();
}

int ClawdbTableStore::read_row_at(ClawdbRowPosition position,
                                  std::vector<unsigned char> *row_data,
                                  uint32_t *row_length) {
  if (fseeko(data_file_, static_cast<off_t>(position), SEEK_SET) != 0)
    return errno;

  ClawdbRowHeader row_header;
  size_t read_count = std::fread(&row_header, sizeof(ClawdbRowHeader), 1, data_file_);
  if (read_count != 1) {
    if (std::feof(data_file_)) return HA_ERR_END_OF_FILE;
    return errno ? errno : EIO;
  }

  if (row_header.flags == CLAWDB_ROW_DELETED) {
    /* Caller should skip deleted rows via next_live_row(). */
    return ENOENT;
  }

  row_data->resize(row_header.data_length);
  if (std::fread(row_data->data(), 1, row_header.data_length, data_file_) !=
      row_header.data_length) {
    return errno ? errno : EIO;
  }

  *row_length = row_header.data_length;
  return 0;
}

int ClawdbTableStore::delete_row_at(ClawdbRowPosition position) {
  if (fseeko(data_file_, static_cast<off_t>(position), SEEK_SET) != 0)
    return errno;

  ClawdbRowHeader row_header;
  if (std::fread(&row_header, sizeof(ClawdbRowHeader), 1, data_file_) != 1)
    return errno ? errno : EIO;

  if (row_header.flags == CLAWDB_ROW_DELETED) return 0;  /* Already deleted. */

  /* Seek back to the header and overwrite the flags byte. */
  if (fseeko(data_file_, static_cast<off_t>(position), SEEK_SET) != 0)
    return errno;

  row_header.flags = CLAWDB_ROW_DELETED;
  if (std::fwrite(&row_header, sizeof(ClawdbRowHeader), 1, data_file_) != 1)
    return errno;

  if (header_.row_count > 0) --header_.row_count;
  return write_header();
}

int ClawdbTableStore::update_row_at(ClawdbRowPosition position,
                                    const unsigned char *new_row_data,
                                    uint32_t new_row_length,
                                    ClawdbRowPosition *new_position) {
  /* Read the existing row header to check its size. */
  if (fseeko(data_file_, static_cast<off_t>(position), SEEK_SET) != 0)
    return errno;

  ClawdbRowHeader old_header;
  if (std::fread(&old_header, sizeof(ClawdbRowHeader), 1, data_file_) != 1)
    return errno ? errno : EIO;

  if (old_header.data_length == new_row_length) {
    /* Same size: update in-place. */
    if (std::fwrite(new_row_data, 1, new_row_length, data_file_) != new_row_length)
      return errno;
    *new_position = position;
    return 0;
  }

  /* Different size: tombstone old row and append new one.
     delete_row_at() decrements row_count and append_row() increments it,
     so the net effect on row_count is zero — which is correct because
     we are replacing one live row with another. */
  int rc = delete_row_at(position);
  if (rc != 0) return rc;

  if (fseeko(data_file_, 0, SEEK_END) != 0) return errno;
  *new_position = static_cast<ClawdbRowPosition>(ftello(data_file_));

  rc = append_row(new_row_data, new_row_length);
  if (rc != 0) {
    /* append_row failed after we already tombstoned the old row.
       Restore row_count to compensate for the decrement in delete_row_at. */
    ++header_.row_count;
    write_header();
  }
  return rc;
}

int ClawdbTableStore::next_live_row(ClawdbRowPosition from,
                                    ClawdbRowPosition *position) {
  ClawdbRowPosition current = from;

  while (true) {
    if (fseeko(data_file_, static_cast<off_t>(current), SEEK_SET) != 0)
      return errno;

    ClawdbRowHeader row_header;
    size_t read_count =
        std::fread(&row_header, sizeof(ClawdbRowHeader), 1, data_file_);

    if (read_count != 1) {
      if (std::feof(data_file_)) return HA_ERR_END_OF_FILE;
      return errno ? errno : EIO;
    }

    if (row_header.flags == CLAWDB_ROW_ALIVE) {
      *position = current;
      return 0;
    }

    /* Skip over this deleted row. */
    current += sizeof(ClawdbRowHeader) + row_header.data_length;
  }
}

int ClawdbTableStore::truncate() {
  /* Reopen the file in write mode to truncate it. */
  std::fclose(data_file_);
  data_file_ = std::fopen(file_path_.c_str(), "w+b");
  if (data_file_ == nullptr) return errno;

  header_.row_count = 0;
  return write_header();
}

int ClawdbTableStore::flush() {
  if (data_file_ == nullptr) return 0;
  if (std::fflush(data_file_) != 0) return errno;
  return 0;
}

int ClawdbTableStore::append_row_at(const unsigned char *row_data,
                                    uint32_t row_length,
                                    ClawdbRowPosition *new_position) {
  /* Seek to end of file and record the position before writing. */
  if (fseeko(data_file_, 0, SEEK_END) != 0) return errno;

  off_t offset = ftello(data_file_);
  if (offset < 0) return errno;
  *new_position = static_cast<ClawdbRowPosition>(offset);

  ClawdbRowHeader row_header;
  row_header.flags = CLAWDB_ROW_ALIVE;
  row_header.data_length = row_length;

  if (std::fwrite(&row_header, sizeof(ClawdbRowHeader), 1, data_file_) != 1)
    return errno;

  if (std::fwrite(row_data, 1, row_length, data_file_) != row_length)
    return errno;

  ++header_.row_count;
  return write_header();
}
