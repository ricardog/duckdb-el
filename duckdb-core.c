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
    duckdb_close(db_ptr);
    free(db_ptr);
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
  /* Note: libduckdb's duckdb_database is often a pointer itself, 
     but we need to store it safely. */
  duckdb_database *db_ptr = malloc(sizeof(duckdb_database));
  *db_ptr = db;

  return env->make_user_ptr(env, db_finalizer, db_ptr);
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
  emacs_value fset_args[] = { open_sym, open_func };
  env->funcall(env, env->intern(env, "fset"), 2, fset_args);

  /* Provide duckdb-core */
  emacs_value provide_sym = env->intern(env, "provide");
  emacs_value module_sym = env->intern(env, "duckdb-core");
  env->funcall(env, provide_sym, 1, (emacs_value[]){ module_sym });

  return 0;
}
