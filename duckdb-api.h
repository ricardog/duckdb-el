#ifndef DUCKDB_API_H
#define DUCKDB_API_H

#include <emacs-module.h>
#include <duckdb.h>

/* GPL compatibility */
extern int plugin_is_GPL_compatible;

/* Helper macros for error signaling */
#define SIGNAL_ERROR(env, symbol, message)                                     \
  do {                                                                         \
    emacs_value sym = (env)->intern((env), (symbol));                          \
    emacs_value msg = (env)->make_string((env), (message), strlen(message));   \
    emacs_value list_sym = (env)->intern((env), "list");                       \
    emacs_value args_list = (env)->funcall((env), list_sym, 1, &msg);           \
    (env)->non_local_exit_signal((env), sym, args_list);                       \
  } while (0)

#endif /* DUCKDB_API_H */
