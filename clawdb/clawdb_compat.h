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

/** @file storage/clawdb/clawdb_compat.h
  @brief
  MySQL version compatibility layer for ClawDB.

  All #if MYSQL_VERSION_ID conditionals are centralized here so that the
  rest of the ClawDB source code is version-agnostic.  When porting to a
  new MySQL release, only this file (and CMakeLists.txt) should need
  changes.

  Supported MySQL versions:
    - 5.7.44
    - 8.0.20
    - 8.0.45
    - 8.4.7
*/

#ifndef STORAGE_CLAWDB_CLAWDB_COMPAT_H
#define STORAGE_CLAWDB_CLAWDB_COMPAT_H

#include "mysql_version.h"

/* -----------------------------------------------------------------------
   1. Include path differences
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#include "my_inttypes.h"
#include "mysql/components/my_service.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/udf_registration.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql/service_plugin_registry.h"
#include "mysqld_error.h"
#include "mysql/udf_registration_types.h"
#else
/* MySQL 5.7: UDF types live in mysql_com.h; my_global.h provides
   fundamental typedefs (my_bool, my_socket, etc.). */
#include "my_global.h"
#include "mysql_com.h"
#endif

/* -----------------------------------------------------------------------
   2. dd::Table forward declaration (5.7 has no data-dictionary namespace)
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID < 80000
namespace dd { class Table; }
#endif

/* -----------------------------------------------------------------------
   3. DBUG_TRACE (5.7 uses DBUG_ENTER/DBUG_RETURN instead)

   Note: On 8.0+ DBUG_TRACE is already defined in my_dbug.h which is
   pulled in transitively by sql/handler.h.  We only define it here as
   a fallback for 5.7 where it does not exist.  The #ifndef guard
   prevents a "macro redefined" warning on 8.0+.
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID < 80000
#ifndef DBUG_TRACE
#define DBUG_TRACE
#endif
#endif

/* -----------------------------------------------------------------------
   4. ha_statistic_increment: status variable namespace
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_STAT(name) System_status_var::name
#else
#define CLAWDB_STAT(name) SSV::name
#endif

/* -----------------------------------------------------------------------
   5. Handler lifecycle signatures (dd::Table* parameter in 8.0+)
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_OPEN_ARGS \
  const char *name, int /*mode*/, uint /*test_if_locked*/, \
  const dd::Table * /*table_def*/
#define CLAWDB_CREATE_ARGS \
  const char *name, TABLE * /*form*/, HA_CREATE_INFO * /*create_info*/, \
  dd::Table * /*table_def*/
#define CLAWDB_DELETE_TABLE_ARGS \
  const char *name, const dd::Table * /*table_def*/
#define CLAWDB_RENAME_TABLE_ARGS \
  const char *from, const char *to, \
  const dd::Table * /*from_table_def*/, dd::Table * /*to_table_def*/
#else
#define CLAWDB_OPEN_ARGS \
  const char *name, int /*mode*/, uint /*test_if_locked*/
#define CLAWDB_CREATE_ARGS \
  const char *name, TABLE * /*form*/, HA_CREATE_INFO * /*create_info*/
#define CLAWDB_DELETE_TABLE_ARGS \
  const char *name
#define CLAWDB_RENAME_TABLE_ARGS \
  const char *from, const char *to
#endif

/* -----------------------------------------------------------------------
   6. Handler factory signature (8.0+ adds a 'partitioned' parameter)
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_CREATE_HANDLER_ARGS \
  handlerton *hton, TABLE_SHARE *table, bool /*partitioned*/, MEM_ROOT *mem_root
#else
#define CLAWDB_CREATE_HANDLER_ARGS \
  handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root
#endif

/* -----------------------------------------------------------------------
   7. table_flags(): HA_REC_NOT_IN_SEQ (5.7 only, removed in 8.0)
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID < 80000
#define CLAWDB_TABLE_FLAGS_EXTRA  HA_REC_NOT_IN_SEQ
#else
#define CLAWDB_TABLE_FLAGS_EXTRA  0
#endif

/* -----------------------------------------------------------------------
   8. handlerton::file_extensions (8.0+ only)
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_SET_FILE_EXTENSIONS(hton) \
  do { (hton)->file_extensions = nullptr; } while (0)
#else
#define CLAWDB_SET_FILE_EXTENSIONS(hton) \
  do { (void)(hton); } while (0)
#endif

/* -----------------------------------------------------------------------
   9. mysql_declare_plugin: 8.0+ has an extra "Check Uninstall" slot
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_PLUGIN_CHECK_UNINSTALL  nullptr,
#else
#define CLAWDB_PLUGIN_CHECK_UNINSTALL
#endif

/* -----------------------------------------------------------------------
   10. UDF registration: 8.0+ uses the component registry service;
       5.7 requires manual CREATE FUNCTION DDL after plugin load.
   ----------------------------------------------------------------------- */

#define CLAWDB_HAS_UDF_REGISTRY (MYSQL_VERSION_ID >= 80000)

/* -----------------------------------------------------------------------
   11. KEY::comment type: 8.0+ uses LEX_CSTRING, 5.7 uses LEX_STRING.
       Both have .str and .length members, but the const-ness differs.
       We provide a uniform accessor macro for portability.
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_KEY_COMMENT_STR(key)  ((key).comment.str)
#define CLAWDB_KEY_COMMENT_LEN(key)  ((key).comment.length)
#else
#define CLAWDB_KEY_COMMENT_STR(key)  ((key).comment.str)
#define CLAWDB_KEY_COMMENT_LEN(key)  ((key).comment.length)
#endif

/* -----------------------------------------------------------------------
   12. bas_ext(): required as a pure virtual override on 5.7 only.
       On 8.0+ the base class provides a default implementation.
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID < 80000
#define CLAWDB_BAS_EXT_OVERRIDE                                 \
  const char **bas_ext() const override {                       \
    static const char *empty[] = {NullS};                       \
    return empty;                                               \
  }
#else
#define CLAWDB_BAS_EXT_OVERRIDE
#endif

/* -----------------------------------------------------------------------
   13. Index algorithm overrides: 8.0+ requires get_default_index_algorithm()
       and is_index_algorithm_supported().  These do not exist on 5.7.
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80000
#define CLAWDB_INDEX_ALGORITHM_OVERRIDES                                     \
  enum ha_key_alg get_default_index_algorithm() const override {             \
    return HA_KEY_ALG_HASH;                                                  \
  }                                                                          \
  bool is_index_algorithm_supported(enum ha_key_alg key_alg) const override {\
    return key_alg == HA_KEY_ALG_HASH;                                       \
  }
#else
#define CLAWDB_INDEX_ALGORITHM_OVERRIDES
#endif

/* -----------------------------------------------------------------------
   14. Field pointer access: field_ptr() was added in 8.0.25; earlier
       versions expose the raw 'ptr' member directly.
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID >= 80025
#define CLAWDB_FIELD_PTR(field)  ((field)->field_ptr())
#else
#define CLAWDB_FIELD_PTR(field)  ((field)->ptr)
#endif

/* -----------------------------------------------------------------------
   15. Field_blob data access: 8.0+ provides get_blob_data() returning
       const uchar*; 5.7 uses get_ptr(uchar**) instead.

       Note: we cannot use an inline function here because clawdb_compat.h
       does not include sql/field.h (and must not, to avoid circular
       dependencies).  The helper function is defined in ha_clawdb.cc
       where Field_blob is fully declared.
   ----------------------------------------------------------------------- */

#define CLAWDB_HAS_GET_BLOB_DATA (MYSQL_VERSION_ID >= 80000)

/* -----------------------------------------------------------------------
   16. down_cast<>: MySQL 8.0+ provides down_cast in my_dbug.h;
       5.7 does not.  Provide a simple fallback using static_cast.
   ----------------------------------------------------------------------- */

#if MYSQL_VERSION_ID < 80000
#ifndef down_cast
template <typename To, typename From>
inline To down_cast(From *from) {
  return static_cast<To>(from);
}
#endif
#endif

#endif  /* STORAGE_CLAWDB_CLAWDB_COMPAT_H */
