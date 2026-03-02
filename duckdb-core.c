#include <stdlib.h>
#include <string.h>
#include "duckdb-api.h"

int plugin_is_GPL_compatible;

/* Helper to convert emacs_value to a C string */
static char *
extract_string(emacs_env *env, emacs_value value)
{
  ptrdiff_t size = 0;
  if (!env->copy_string_contents(env, value, NULL, &size))
    return NULL;

  char *buffer = malloc(size);
  if (!env->copy_string_contents(env, value, buffer, &size))
  {
    free(buffer);
    return NULL;
  }
  return buffer;
}

/* Finalizer for duckdb_database */
static void
db_finalizer(void *data)
{
  duckdb_database *db_ptr = (duckdb_database *)data;
  if (db_ptr)
  {
    if (*db_ptr)
      duckdb_close(db_ptr);
    free(db_ptr);
  }
}

/* Finalizer for duckdb_connection */
static void
conn_finalizer(void *data)
{
  duckdb_connection *conn_ptr = (duckdb_connection *)data;
  if (conn_ptr)
  {
    if (*conn_ptr)
      duckdb_disconnect(conn_ptr);
    free(conn_ptr);
  }
}

static emacs_value
Fduckdb_open(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  char *path = extract_string(env, args[0]);
  if (!path)
  {
    SIGNAL_ERROR(env, "error", "Invalid path argument");
    return env->intern(env, "nil");
  }

  duckdb_database db;
  if (duckdb_open(path, &db) == DuckDBError)
  {
    free(path);
    SIGNAL_ERROR(env, "duckdb-error", "Failed to open database");
    return env->intern(env, "nil");
  }

  free(path);

  /* Wrap the database pointer in a user_ptr with a finalizer */
  duckdb_database *db_ptr = malloc(sizeof(duckdb_database));
  *db_ptr = db;

  return env->make_user_ptr(env, db_finalizer, db_ptr);
}

static emacs_value
Fduckdb_close(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-db-ptr");
    return env->intern(env, "nil");
  }

  duckdb_database *db_ptr = (duckdb_database *)env->get_user_ptr(env, args[0]);
  if (db_ptr && *db_ptr)
  {
    duckdb_close(db_ptr);
    /* Set to NULL to avoid double close in finalizer */
    *db_ptr = NULL;
  }

  return env->intern(env, "t");
}

static emacs_value
Fduckdb_connect(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-db-ptr");
    return env->intern(env, "nil");
  }

  duckdb_database *db_ptr = (duckdb_database *)env->get_user_ptr(env, args[0]);
  if (!db_ptr)
  {
    SIGNAL_ERROR(env, "duckdb-error", "Invalid database pointer");
    return env->intern(env, "nil");
  }

  duckdb_connection conn;
  if (duckdb_connect(*db_ptr, &conn) == DuckDBError)
  {
    SIGNAL_ERROR(env, "duckdb-error", "Failed to connect to database");
    return env->intern(env, "nil");
  }

  duckdb_connection *conn_ptr = malloc(sizeof(duckdb_connection));
  *conn_ptr = conn;

  return env->make_user_ptr(env, conn_finalizer, conn_ptr);
}

static emacs_value
Fduckdb_disconnect(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-conn-ptr");
    return env->intern(env, "nil");
  }

  duckdb_connection *conn_ptr = (duckdb_connection *)env->get_user_ptr(env, args[0]);
  if (conn_ptr && *conn_ptr)
  {
    duckdb_disconnect(conn_ptr);
    /* Set to NULL to avoid double free in finalizer */
    *conn_ptr = NULL;
  }

  return env->intern(env, "t");
}

static emacs_value
Fduckdb_execute(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-conn-ptr");
    return env->intern(env, "nil");
  }

  duckdb_connection *conn_ptr = (duckdb_connection *)env->get_user_ptr(env, args[0]);
  if (!conn_ptr || !*conn_ptr)
  {
    SIGNAL_ERROR(env, "duckdb-error", "Invalid connection pointer");
    return env->intern(env, "nil");
  }

  char *sql = extract_string(env, args[1]);
  if (!sql)
  {
    SIGNAL_ERROR(env, "error", "Invalid SQL argument");
    return env->intern(env, "nil");
  }

  duckdb_result result;
  if (duckdb_query(*conn_ptr, sql, &result) == DuckDBError)
  {
    const char *error_msg = duckdb_result_error(&result);
    SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute query");
    duckdb_destroy_result(&result);
    free(sql);
    return env->intern(env, "nil");
  }

  free(sql);
  int64_t rows_changed = duckdb_rows_changed(&result);
  duckdb_destroy_result(&result);

  return env->make_integer(env, rows_changed);
}

static emacs_value
Fduckdb_select(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-conn-ptr");
    return env->intern(env, "nil");
  }

  duckdb_connection *conn_ptr = (duckdb_connection *)env->get_user_ptr(env, args[0]);
  if (!conn_ptr || !*conn_ptr)
  {
    SIGNAL_ERROR(env, "duckdb-error", "Invalid connection pointer");
    return env->intern(env, "nil");
  }

  char *sql = extract_string(env, args[1]);
  if (!sql)
  {
    SIGNAL_ERROR(env, "error", "Invalid SQL argument");
    return env->intern(env, "nil");
  }

  duckdb_result result;
  if (duckdb_query(*conn_ptr, sql, &result) == DuckDBError)
  {
    const char *error_msg = duckdb_result_error(&result);
    SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute query");
    duckdb_destroy_result(&result);
    free(sql);
    return env->intern(env, "nil");
  }

  free(sql);

  idx_t row_count = duckdb_row_count(&result);
  idx_t col_count = duckdb_column_count(&result);

  emacs_value cons_sym = env->intern(env, "cons");
  emacs_value nil_sym = env->intern(env, "nil");
  emacs_value null_symbol_sym = env->intern(env, "duckdb-null-symbol");
  emacs_value null_val = env->funcall(env, env->intern(env, "symbol-value"), 1, &null_symbol_sym);

  emacs_value rows_list = nil_sym;

  /* Iterate rows backwards to build the list with cons efficiently */
  for (idx_t r = row_count; r > 0; r--)
  {
    idx_t row = r - 1;
    emacs_value row_list = nil_sym;

    for (idx_t c = col_count; c > 0; c--)
    {
      idx_t col = c - 1;
      emacs_value val = null_val;

      if (!duckdb_value_is_null(&result, col, row))
      {
        duckdb_type type = duckdb_column_type(&result, col);
        switch (type)
        {
        case DUCKDB_TYPE_BOOLEAN:
          val = env->intern(env, duckdb_value_boolean(&result, col, row) ? "t" : "nil");
          break;
        case DUCKDB_TYPE_TINYINT:
          val = env->make_integer(env, duckdb_value_int8(&result, col, row));
          break;
        case DUCKDB_TYPE_SMALLINT:
          val = env->make_integer(env, duckdb_value_int16(&result, col, row));
          break;
        case DUCKDB_TYPE_INTEGER:
          val = env->make_integer(env, duckdb_value_int32(&result, col, row));
          break;
        case DUCKDB_TYPE_BIGINT:
          val = env->make_integer(env, duckdb_value_int64(&result, col, row));
          break;
        case DUCKDB_TYPE_FLOAT:
          val = env->make_float(env, duckdb_value_float(&result, col, row));
          break;
        case DUCKDB_TYPE_DOUBLE:
          val = env->make_float(env, duckdb_value_double(&result, col, row));
          break;
        case DUCKDB_TYPE_VARCHAR:
        {
          char *str = duckdb_value_varchar(&result, col, row);
          if (str) {
            val = env->make_string(env, str, strlen(str));
            duckdb_free(str);
          }
          break;
        }
        default:
          /* Fallback to string for unhandled types */
          {
            char *str = duckdb_value_varchar(&result, col, row);
            if (str) {
              val = env->make_string(env, str, strlen(str));
              duckdb_free(str);
            }
          }
          break;
        }
      }
      row_list = env->funcall(env, cons_sym, 2, (emacs_value[]){val, row_list});
    }
    rows_list = env->funcall(env, cons_sym, 2, (emacs_value[]){row_list, rows_list});
  }

  duckdb_destroy_result(&result);
  return rows_list;
}

/* Module initialization */
int
emacs_module_init(struct emacs_runtime *ert)
{
  emacs_env *env = ert->get_environment(ert);

  /* Define the duckdb-error symbol */
  emacs_value error_sym = env->intern(env, "duckdb-error");
  env->funcall(env, env->intern(env, "define-error"), 2, (emacs_value[]){ error_sym, env->make_string(env, "DuckDB Error", 12) });

  /* Register Fduckdb_open */
  emacs_value open_func = env->make_function(env, 1, 1, Fduckdb_open, "Open a DuckDB database.", NULL);
  emacs_value open_sym = env->intern(env, "duckdb-open");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ open_sym, open_func });

  /* Register Fduckdb_close */
  emacs_value close_func = env->make_function(env, 1, 1, Fduckdb_close, "Close a DuckDB database.", NULL);
  emacs_value close_sym = env->intern(env, "duckdb-close");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ close_sym, close_func });

  /* Register Fduckdb_connect */
  emacs_value connect_func = env->make_function(env, 1, 1, Fduckdb_connect, "Connect to a DuckDB database.", NULL);
  emacs_value connect_sym = env->intern(env, "duckdb-connect");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ connect_sym, connect_func });

  /* Register Fduckdb_disconnect */
  emacs_value disconnect_func = env->make_function(env, 1, 1, Fduckdb_disconnect, "Disconnect from a DuckDB database.", NULL);
  emacs_value disconnect_sym = env->intern(env, "duckdb-disconnect");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ disconnect_sym, disconnect_func });

  /* Register Fduckdb_execute */
  emacs_value execute_func = env->make_function(env, 2, 2, Fduckdb_execute, "Execute a SQL query in a DuckDB database.", NULL);
  emacs_value execute_sym = env->intern(env, "duckdb-execute");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ execute_sym, execute_func });

  /* Register Fduckdb_select */
  emacs_value select_func = env->make_function(env, 2, 2, Fduckdb_select, "Execute a SQL query and return results as a list of lists.", NULL);
  emacs_value select_sym = env->intern(env, "duckdb-select");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ select_sym, select_func });

  /* Provide duckdb-core */
  emacs_value provide_sym = env->intern(env, "provide");
  emacs_value module_sym = env->intern(env, "duckdb-core");
  env->funcall(env, provide_sym, 1, (emacs_value[]){ module_sym });

  return 0;
}
