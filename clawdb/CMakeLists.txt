# Copyright (c) 2024, 2025, ClawDB Authors.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

# ClawDB: A MySQL plugin storage engine with native vector support.
# Supports VECTOR(n) column type, KNN search, HNSW/IVFFlat indexing.

IF(COMMAND DISABLE_MISSING_PROFILE_WARNING)
  DISABLE_MISSING_PROFILE_WARNING()
ENDIF()
ADD_DEFINITIONS(-DMYSQL_SERVER)

SET(CLAWDB_SOURCES
  ha_clawdb.cc
  clawdb_vec.cc
  clawdb_store.cc
  clawdb_hnsw.cc
  clawdb_udf.cc
)

MYSQL_ADD_PLUGIN(clawdb ${CLAWDB_SOURCES}
  STORAGE_ENGINE
  MODULE_ONLY
  )

# MySQL 5.7 defaults to -std=gnu++03 which is too old for ClawDB.
# MYSQL_VERSION_ID is a numeric value (e.g. 50744, 80020, 80045, 80407)
# computed by mysql_version.cmake.
#   5.7.x  → C++14  (override gnu++03)
#   8.0.0–8.0.24 → C++14
#   8.0.25–8.0.x → C++17  (std::string_view, std::optional)
#   8.4+   → C++20  (std::unsigned_integral, std::bit_width)
IF(MYSQL_VERSION_ID LESS 80000)
  TARGET_COMPILE_OPTIONS(clawdb PRIVATE -std=c++14 -fabi-version=0)
ELSEIF(MYSQL_VERSION_ID LESS 80025)
  TARGET_COMPILE_OPTIONS(clawdb PRIVATE -std=c++14)
ELSEIF(MYSQL_VERSION_ID LESS 80400)
  TARGET_COMPILE_OPTIONS(clawdb PRIVATE -std=c++17)
ELSE()
  TARGET_COMPILE_OPTIONS(clawdb PRIVATE -std=c++20)
ENDIF()

# mysql_version.h is generated in the build directory during cmake configure.
# We need both the source root (for "sql/handler.h" etc.) and the build
# directory's include/ (for "mysql_version.h") on the include path.
TARGET_INCLUDE_DIRECTORIES(clawdb PRIVATE
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}/include
)

# rapidjson is bundled with MySQL and needed transitively via sql/field.h
IF(EXISTS "${CMAKE_SOURCE_DIR}/extra/rapidjson/include")
  TARGET_INCLUDE_DIRECTORIES(clawdb PRIVATE
    ${CMAKE_SOURCE_DIR}/extra/rapidjson/include
  )
ENDIF()
