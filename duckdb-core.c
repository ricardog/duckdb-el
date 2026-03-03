#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <signal.h>
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

/* Prepared Statement Wrapper */
typedef struct {
  duckdb_prepared_statement stmt;
  duckdb_result result;
  bool result_executed;
  idx_t current_row;
} emacs_duckdb_stmt;

/* Finalizer for duckdb_prepared_statement */
static void
stmt_finalizer(void *data)
{
  emacs_duckdb_stmt *stmt_ptr = (emacs_duckdb_stmt *)data;
  if (stmt_ptr)
  {
    if (stmt_ptr->result_executed)
      duckdb_destroy_result(&stmt_ptr->result);
    if (stmt_ptr->stmt)
      duckdb_destroy_prepare(&stmt_ptr->stmt);
    free(stmt_ptr);
  }
}

/* Asynchronous Query Context */
typedef struct {
  duckdb_prepared_statement stmt;
  duckdb_result result;
  duckdb_state state;
  char *error_msg;
  emacs_value callback_ref;
  bool is_done;
} async_ctx;

static void *
async_worker(void *data)
{
  async_ctx *ctx = (async_ctx *)data;
  ctx->state = duckdb_execute_prepared(ctx->stmt, &ctx->result);
  if (ctx->state == DuckDBError) {
    const char *err = duckdb_result_error(&ctx->result);
    if (err) ctx->error_msg = strdup(err);
  }
  ctx->is_done = true;
  /* Signal the main thread */
  kill(getpid(), SIGUSR1);
  return NULL;
}

static void
async_ctx_finalizer(void *data)
{
  async_ctx *ctx = (async_ctx *)data;
  if (ctx) {
    if (ctx->is_done) {
        duckdb_destroy_result(&ctx->result);
    }
    if (ctx->stmt) {
        duckdb_destroy_prepare(&ctx->stmt);
    }
    if (ctx->error_msg) {
        free(ctx->error_msg);
    }
    free(ctx);
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

/* Helper to bind parameters to a prepared statement */
static bool
bind_parameters(emacs_env *env, duckdb_prepared_statement stmt, emacs_value params)
{
  emacs_value car_sym = env->intern(env, "car");
  emacs_value cdr_sym = env->intern(env, "cdr");
  emacs_value nil_sym = env->intern(env, "nil");
  emacs_value null_symbol_sym = env->intern(env, "duckdb-null-symbol");
  emacs_value null_val = env->funcall(env, env->intern(env, "symbol-value"), 1, &null_symbol_sym);

  idx_t nparams = duckdb_nparams(stmt);
  
  for (idx_t i = 1; i <= nparams; i++)
  {
    if (env->eq(env, params, nil_sym))
    {
       SIGNAL_ERROR(env, "duckdb-error", "Too few parameters provided");
       return false;
    }

    emacs_value val = env->funcall(env, car_sym, 1, &params);
    params = env->funcall(env, cdr_sym, 1, &params);

    emacs_value type = env->type_of(env, val);
    duckdb_state state;

    if (env->eq(env, val, null_val))
    {
      state = duckdb_bind_null(stmt, i);
    }
    else if (env->eq(env, type, env->intern(env, "integer")))
    {
      state = duckdb_bind_int64(stmt, i, env->extract_integer(env, val));
    }
    else if (env->eq(env, type, env->intern(env, "float")))
    {
      state = duckdb_bind_double(stmt, i, env->extract_float(env, val));
    }
    else if (env->eq(env, type, env->intern(env, "string")))
    {
      char *str = extract_string(env, val);
      state = duckdb_bind_varchar(stmt, i, str);
      free(str);
    }
    else if (env->eq(env, val, env->intern(env, "t")))
    {
      state = duckdb_bind_boolean(stmt, i, true);
    }
    else if (env->eq(env, val, nil_sym))
    {
      state = duckdb_bind_boolean(stmt, i, false);
    }
    else
    {
       SIGNAL_ERROR(env, "duckdb-error", "Unsupported parameter type");
       return false;
    }

    if (state == DuckDBError)
    {
      SIGNAL_ERROR(env, "duckdb-error", "Failed to bind parameter");
      return false;
    }
  }

  return true;
}

static emacs_value
Fduckdb_execute(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
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
  if (nargs > 2)
  {
    /* Use prepared statement for parameters */
    duckdb_prepared_statement stmt;
    if (duckdb_prepare(*conn_ptr, sql, &stmt) == DuckDBError)
    {
      const char *error_msg = duckdb_prepare_error(stmt);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to prepare statement");
      duckdb_destroy_prepare(&stmt);
      free(sql);
      return env->intern(env, "nil");
    }
    free(sql);

    if (!bind_parameters(env, stmt, args[2]))
    {
      duckdb_destroy_prepare(&stmt);
      return env->intern(env, "nil");
    }

    if (duckdb_execute_prepared(stmt, &result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute prepared query");
      duckdb_destroy_result(&result);
      duckdb_destroy_prepare(&stmt);
      return env->intern(env, "nil");
    }
    duckdb_destroy_prepare(&stmt);
  }
  else
  {
    if (duckdb_query(*conn_ptr, sql, &result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute query");
      duckdb_destroy_result(&result);
      free(sql);
      return env->intern(env, "nil");
    }
    free(sql);
  }

  int64_t rows_changed = duckdb_rows_changed(&result);
  duckdb_destroy_result(&result);

  return env->make_integer(env, rows_changed);
}

/* Helper to convert a duckdb_result row to an emacs_value list */
static emacs_value
convert_row_to_list(emacs_env *env, duckdb_result *result, idx_t row, emacs_value null_val)
{
  idx_t col_count = duckdb_column_count(result);
  emacs_value row_list = env->intern(env, "nil");
  emacs_value cons_sym = env->intern(env, "cons");

  for (idx_t c = col_count; c > 0; c--)
  {
    idx_t col = c - 1;
    emacs_value val = null_val;

    if (!duckdb_value_is_null(result, col, row))
    {
      duckdb_type type = duckdb_column_type(result, col);
      switch (type)
      {
      case DUCKDB_TYPE_BOOLEAN:
        val = env->intern(env, duckdb_value_boolean(result, col, row) ? "t" : "nil");
        break;
      case DUCKDB_TYPE_TINYINT:
        val = env->make_integer(env, duckdb_value_int8(result, col, row));
        break;
      case DUCKDB_TYPE_SMALLINT:
        val = env->make_integer(env, duckdb_value_int16(result, col, row));
        break;
      case DUCKDB_TYPE_INTEGER:
        val = env->make_integer(env, duckdb_value_int32(result, col, row));
        break;
      case DUCKDB_TYPE_BIGINT:
        val = env->make_integer(env, duckdb_value_int64(result, col, row));
        break;
      case DUCKDB_TYPE_FLOAT:
        val = env->make_float(env, duckdb_value_float(result, col, row));
        break;
      case DUCKDB_TYPE_DOUBLE:
        val = env->make_float(env, duckdb_value_double(result, col, row));
        break;
      case DUCKDB_TYPE_TIMESTAMP:
      {
        char *str = duckdb_value_varchar(result, col, row);
        if (str) {
          val = env->make_string(env, str, strlen(str));
          duckdb_free(str);
        }
        break;
      }
      case DUCKDB_TYPE_BLOB:
      {
        duckdb_blob blob = duckdb_value_blob(result, col, row);
        val = env->make_string(env, blob.data, blob.size);
        duckdb_free(blob.data);
        break;
      }
      case DUCKDB_TYPE_VARCHAR:
      {
        char *str = duckdb_value_varchar(result, col, row);
        if (str) {
          val = env->make_string(env, str, strlen(str));
          duckdb_free(str);
        }
        break;
      }
      default:
        {
          char *str = duckdb_value_varchar(result, col, row);
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
  return row_list;
}

static emacs_value
Fduckdb_select(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
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
  if (nargs > 2)
  {
    duckdb_prepared_statement stmt;
    if (duckdb_prepare(*conn_ptr, sql, &stmt) == DuckDBError)
    {
      const char *error_msg = duckdb_prepare_error(stmt);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to prepare statement");
      duckdb_destroy_prepare(&stmt);
      free(sql);
      return env->intern(env, "nil");
    }
    free(sql);

    if (!bind_parameters(env, stmt, args[2]))
    {
      duckdb_destroy_prepare(&stmt);
      return env->intern(env, "nil");
    }

    if (duckdb_execute_prepared(stmt, &result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute prepared query");
      duckdb_destroy_result(&result);
      duckdb_destroy_prepare(&stmt);
      return env->intern(env, "nil");
    }
    duckdb_destroy_prepare(&stmt);
  }
  else
  {
    if (duckdb_query(*conn_ptr, sql, &result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute query");
      duckdb_destroy_result(&result);
      free(sql);
      return env->intern(env, "nil");
    }
    free(sql);
  }

  idx_t row_count = duckdb_row_count(&result);
  emacs_value nil_sym = env->intern(env, "nil");
  emacs_value cons_sym = env->intern(env, "cons");
  emacs_value null_symbol_sym = env->intern(env, "duckdb-null-symbol");
  emacs_value null_val = env->funcall(env, env->intern(env, "symbol-value"), 1, &null_symbol_sym);

  emacs_value rows_list = nil_sym;

  for (idx_t r = row_count; r > 0; r--)
  {
    idx_t row = r - 1;
    emacs_value row_list = convert_row_to_list(env, &result, row, null_val);
    rows_list = env->funcall(env, cons_sym, 2, (emacs_value[]){row_list, rows_list});
  }

  duckdb_destroy_result(&result);
  return rows_list;
}

static emacs_value
Fduckdb_select_columns(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
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
  if (nargs > 2)
  {
    duckdb_prepared_statement stmt;
    if (duckdb_prepare(*conn_ptr, sql, &stmt) == DuckDBError)
    {
      const char *error_msg = duckdb_prepare_error(stmt);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to prepare statement");
      duckdb_destroy_prepare(&stmt);
      free(sql);
      return env->intern(env, "nil");
    }
    free(sql);

    if (!bind_parameters(env, stmt, args[2]))
    {
      duckdb_destroy_prepare(&stmt);
      return env->intern(env, "nil");
    }

    if (duckdb_execute_prepared(stmt, &result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute prepared query");
      duckdb_destroy_result(&result);
      duckdb_destroy_prepare(&stmt);
      return env->intern(env, "nil");
    }
    duckdb_destroy_prepare(&stmt);
  }
  else
  {
    if (duckdb_query(*conn_ptr, sql, &result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute query");
      duckdb_destroy_result(&result);
      free(sql);
      return env->intern(env, "nil");
    }
    free(sql);
  }

  idx_t col_count = duckdb_column_count(&result);
  idx_t total_rows = duckdb_row_count(&result);
  idx_t chunk_count = duckdb_result_chunk_count(result);

  emacs_value null_symbol_sym = env->intern(env, "duckdb-null-symbol");
  emacs_value null_val = env->funcall(env, env->intern(env, "symbol-value"), 1, &null_symbol_sym);

  /* Create vectors for columns */
  emacs_value *col_vectors = malloc(col_count * sizeof(emacs_value));
  emacs_value make_vector_sym = env->intern(env, "make-vector");
  for (idx_t c = 0; c < col_count; c++) {
    emacs_value vargs[] = { env->make_integer(env, total_rows), null_val };
    col_vectors[c] = env->funcall(env, make_vector_sym, 2, vargs);
  }

  /* Iterate Chunks */
  idx_t global_row_idx = 0;
  for (idx_t ch = 0; ch < chunk_count; ch++) {
    duckdb_data_chunk chunk = duckdb_result_get_chunk(result, ch);
    idx_t chunk_rows = duckdb_data_chunk_get_size(chunk);

    for (idx_t c = 0; c < col_count; c++) {
      duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, c);
      void *data_ptr = duckdb_vector_get_data(vector);
      uint64_t *validity = duckdb_vector_get_validity(vector);
      duckdb_type type = duckdb_column_type(&result, c);

      for (idx_t r = 0; r < chunk_rows; r++) {
        if (validity && !duckdb_validity_row_is_valid(validity, r)) {
          /* It's NULL, already set in make_vector */
          continue;
        }

        emacs_value val = null_val;
        switch (type) {
        case DUCKDB_TYPE_BOOLEAN: {
          bool *d = (bool *)data_ptr;
          val = env->intern(env, d[r] ? "t" : "nil");
          break;
        }
        case DUCKDB_TYPE_TINYINT: {
          int8_t *d = (int8_t *)data_ptr;
          val = env->make_integer(env, d[r]);
          break;
        }
        case DUCKDB_TYPE_SMALLINT: {
          int16_t *d = (int16_t *)data_ptr;
          val = env->make_integer(env, d[r]);
          break;
        }
        case DUCKDB_TYPE_INTEGER: {
          int32_t *d = (int32_t *)data_ptr;
          val = env->make_integer(env, d[r]);
          break;
        }
        case DUCKDB_TYPE_BIGINT: {
          int64_t *d = (int64_t *)data_ptr;
          val = env->make_integer(env, d[r]);
          break;
        }
        case DUCKDB_TYPE_FLOAT: {
          float *d = (float *)data_ptr;
          val = env->make_float(env, d[r]);
          break;
        }
        case DUCKDB_TYPE_DOUBLE: {
          double *d = (double *)data_ptr;
          val = env->make_float(env, d[r]);
          break;
        }
        case DUCKDB_TYPE_TIMESTAMP: {
          /* Use duckdb_value_varchar for simplicity for now as timestamps are complex */
          char *str = duckdb_value_varchar(&result, c, global_row_idx + r);
          if (str) {
            val = env->make_string(env, str, strlen(str));
            duckdb_free(str);
          }
          break;
        }
        case DUCKDB_TYPE_BLOB:
        case DUCKDB_TYPE_VARCHAR: {
          duckdb_string_t *d = (duckdb_string_t *)data_ptr;
          val = env->make_string(env, duckdb_string_t_data(&d[r]), duckdb_string_t_length(d[r]));
          break;
        }
        default:
          break;
        }
        if (val != null_val) {
          env->vec_set(env, col_vectors[c], global_row_idx + r, val);
        }
      }
    }
    duckdb_destroy_data_chunk(&chunk);
    global_row_idx += chunk_rows;
  }

  /* Build Plist */
  emacs_value plist = env->intern(env, "nil");
  emacs_value cons = env->intern(env, "cons");

  for (idx_t c = col_count; c > 0; c--) {
    idx_t idx = c - 1;
    const char *col_name = duckdb_column_name(&result, idx);
    
    char *kw_name = malloc(strlen(col_name) + 2);
    sprintf(kw_name, ":%s", col_name);
    emacs_value key = env->intern(env, kw_name);
    free(kw_name);
    
    emacs_value val = col_vectors[idx];

    plist = env->funcall(env, cons, 2, (emacs_value[]){val, plist});
    plist = env->funcall(env, cons, 2, (emacs_value[]){key, plist});
  }

  free(col_vectors);
  duckdb_destroy_result(&result);

  return plist;
}

static emacs_value
Fduckdb_prepare(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
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

  duckdb_prepared_statement stmt;
  if (duckdb_prepare(*conn_ptr, sql, &stmt) == DuckDBError)
  {
    const char *error_msg = duckdb_prepare_error(stmt);
    SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to prepare statement");
    duckdb_destroy_prepare(&stmt);
    free(sql);
    return env->intern(env, "nil");
  }

  free(sql);

  emacs_duckdb_stmt *stmt_ptr = malloc(sizeof(emacs_duckdb_stmt));
  stmt_ptr->stmt = stmt;
  stmt_ptr->result_executed = false;
  stmt_ptr->current_row = 0;

  return env->make_user_ptr(env, stmt_finalizer, stmt_ptr);
}

static emacs_value
Fduckdb_bind(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-stmt-ptr");
    return env->intern(env, "nil");
  }

  emacs_duckdb_stmt *stmt_ptr = (emacs_duckdb_stmt *)env->get_user_ptr(env, args[0]);
  if (!stmt_ptr || !stmt_ptr->stmt)
  {
    SIGNAL_ERROR(env, "duckdb-error", "Invalid statement pointer");
    return env->intern(env, "nil");
  }

  /* If it was already executed, reset it? DuckDB prepared statements can be re-bound. */
  if (stmt_ptr->result_executed) {
      duckdb_destroy_result(&stmt_ptr->result);
      stmt_ptr->result_executed = false;
      stmt_ptr->current_row = 0;
  }

  if (bind_parameters(env, stmt_ptr->stmt, args[1]))
    return env->intern(env, "t");
  else
    return env->intern(env, "nil");
}

static emacs_value
Fduckdb_step(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    SIGNAL_ERROR(env, "wrong-type-argument", "Expected user-ptr for duckdb-stmt-ptr");
    return env->intern(env, "nil");
  }

  emacs_duckdb_stmt *stmt_ptr = (emacs_duckdb_stmt *)env->get_user_ptr(env, args[0]);
  if (!stmt_ptr || !stmt_ptr->stmt)
  {
    SIGNAL_ERROR(env, "duckdb-error", "Invalid statement pointer");
    return env->intern(env, "nil");
  }

  if (!stmt_ptr->result_executed)
  {
    if (duckdb_execute_prepared(stmt_ptr->stmt, &stmt_ptr->result) == DuckDBError)
    {
      const char *error_msg = duckdb_result_error(&stmt_ptr->result);
      SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to execute prepared statement");
      duckdb_destroy_result(&stmt_ptr->result);
      return env->intern(env, "nil");
    }
    stmt_ptr->result_executed = true;
    stmt_ptr->current_row = 0;
  }

  idx_t row_count = duckdb_row_count(&stmt_ptr->result);
  if (stmt_ptr->current_row >= row_count)
  {
    return env->intern(env, "nil");
  }

  emacs_value null_symbol_sym = env->intern(env, "duckdb-null-symbol");
  emacs_value null_val = env->funcall(env, env->intern(env, "symbol-value"), 1, &null_symbol_sym);

  emacs_value row_list = convert_row_to_list(env, &stmt_ptr->result, stmt_ptr->current_row, null_val);
  stmt_ptr->current_row++;

  return row_list;
}

static emacs_value
Fduckdb_select_async(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
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

  emacs_value callback = args[2];
  emacs_value params = (nargs > 3) ? args[3] : env->intern(env, "nil");

  duckdb_prepared_statement stmt;
  if (duckdb_prepare(*conn_ptr, sql, &stmt) == DuckDBError)
  {
    const char *error_msg = duckdb_prepare_error(stmt);
    SIGNAL_ERROR(env, "duckdb-error", error_msg ? error_msg : "Failed to prepare statement");
    duckdb_destroy_prepare(&stmt);
    free(sql);
    return env->intern(env, "nil");
  }
  free(sql);

  if (!bind_parameters(env, stmt, params))
  {
    duckdb_destroy_prepare(&stmt);
    return env->intern(env, "nil");
  }

  async_ctx *ctx = malloc(sizeof(async_ctx));
  ctx->stmt = stmt;
  ctx->callback_ref = env->make_global_ref(env, callback);
  ctx->state = DuckDBSuccess;
  ctx->error_msg = NULL;
  ctx->is_done = false;
  
  pthread_t thread;
  if (pthread_create(&thread, NULL, async_worker, ctx) != 0) {
    env->free_global_ref(env, ctx->callback_ref);
    duckdb_destroy_prepare(&stmt);
    free(ctx);
    SIGNAL_ERROR(env, "error", "Failed to create thread");
    return env->intern(env, "nil");
  }
  pthread_detach(thread);

  return env->make_user_ptr(env, async_ctx_finalizer, ctx);
}

static emacs_value
Fduckdb_async_poll(emacs_env *env, ptrdiff_t nargs, emacs_value args[], void *data)
{
  (void)nargs;
  (void)data;

  if (!env->eq(env, env->type_of(env, args[0]), env->intern(env, "user-ptr")))
  {
    return env->intern(env, "nil");
  }

  async_ctx *ctx = (async_ctx *)env->get_user_ptr(env, args[0]);
  if (!ctx->is_done) return env->intern(env, "nil");

  if (ctx->state == DuckDBError) {
      emacs_value error_sym = env->intern(env, "duckdb-error");
      emacs_value msg = env->make_string(env, ctx->error_msg ? ctx->error_msg : "Async query failed", 
                                        ctx->error_msg ? strlen(ctx->error_msg) : 18);
      env->non_local_exit_signal(env, error_sym, msg);
      return env->intern(env, "nil");
  }

  /* Convert result to Lisp list of lists */
  idx_t row_count = duckdb_row_count(&ctx->result);
  emacs_value nil_sym = env->intern(env, "nil");
  emacs_value cons_sym = env->intern(env, "cons");
  emacs_value null_symbol_sym = env->intern(env, "duckdb-null-symbol");
  emacs_value null_val = env->funcall(env, env->intern(env, "symbol-value"), 1, &null_symbol_sym);

  emacs_value rows_list = nil_sym;
  for (idx_t r = row_count; r > 0; r--)
  {
    idx_t row = r - 1;
    emacs_value row_list = convert_row_to_list(env, &ctx->result, row, null_val);
    rows_list = env->funcall(env, cons_sym, 2, (emacs_value[]){row_list, rows_list});
  }

  /* Call the callback */
  env->funcall(env, ctx->callback_ref, 1, &rows_list);

  /* Free the global reference to the callback */
  env->free_global_ref(env, ctx->callback_ref);
  ctx->callback_ref = NULL;

  return env->intern(env, "t");
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
  emacs_value execute_func = env->make_function(env, 2, 3, Fduckdb_execute, "Execute a SQL query in a DuckDB database.", NULL);
  emacs_value execute_sym = env->intern(env, "duckdb-execute");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ execute_sym, execute_func });

  /* Register Fduckdb_select */
  emacs_value select_func = env->make_function(env, 2, 3, Fduckdb_select, "Execute a SQL query and return results as a list of lists.", NULL);
  emacs_value select_sym = env->intern(env, "duckdb-select");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ select_sym, select_func });

  /* Register Fduckdb_select_columns */
  emacs_value select_cols_func = env->make_function(env, 2, 3, Fduckdb_select_columns, "Execute a SQL query and return results as a plist of vectors (columnar).", NULL);
  emacs_value select_cols_sym = env->intern(env, "duckdb-select-columns");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ select_cols_sym, select_cols_func });

  /* Register Fduckdb_prepare */
  emacs_value prepare_func = env->make_function(env, 2, 2, Fduckdb_prepare, "Prepare a SQL statement.", NULL);
  emacs_value prepare_sym = env->intern(env, "duckdb-prepare");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ prepare_sym, prepare_func });

  /* Register Fduckdb_bind */
  emacs_value bind_func = env->make_function(env, 2, 2, Fduckdb_bind, "Bind parameters to a prepared statement.", NULL);
  emacs_value bind_sym = env->intern(env, "duckdb-bind");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ bind_sym, bind_func });

  /* Register Fduckdb_step */
  emacs_value step_func = env->make_function(env, 1, 1, Fduckdb_step, "Execute a prepared statement and return one row.", NULL);
  emacs_value step_sym = env->intern(env, "duckdb-step");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ step_sym, step_func });

  /* Register Fduckdb_select_async */
  emacs_value select_async_func = env->make_function(env, 3, 4, Fduckdb_select_async, "Execute a SQL query asynchronously.", NULL);
  emacs_value select_async_sym = env->intern(env, "duckdb--select-async");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ select_async_sym, select_async_func });

  /* Register Fduckdb_async_poll */
  emacs_value async_poll_func = env->make_function(env, 1, 1, Fduckdb_async_poll, "Poll an asynchronous query.", NULL);
  emacs_value async_poll_sym = env->intern(env, "duckdb-async-poll");
  env->funcall(env, env->intern(env, "fset"), 2, (emacs_value[]){ async_poll_sym, async_poll_func });

  /* Provide duckdb-core */
  emacs_value provide_sym = env->intern(env, "provide");
  emacs_value module_sym = env->intern(env, "duckdb-core");
  env->funcall(env, provide_sym, 1, (emacs_value[]){ module_sym });

  return 0;
}
