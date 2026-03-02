;;; duckdb.el --- Emacs DuckDB Dynamic Module -*- lexical-binding: t; -*-

(require 'cl-lib)

;; Load the dynamic module
(require 'duckdb-core)

(defgroup duckdb nil
  "DuckDB integration for Emacs."
  :group 'data)

(defcustom duckdb-null-symbol nil
  "Symbol to use for representing DuckDB NULL values."
  :type 'symbol
  :group 'duckdb)

(define-error 'duckdb-error "DuckDB error" 'error)

(declare-function duckdb-open "duckdb-core.so" (path))
(declare-function duckdb-close "duckdb-core.so" (db-ptr))
(declare-function duckdb-connect "duckdb-core.so" (db-ptr))
(declare-function duckdb-disconnect "duckdb-core.so" (conn-ptr))
(declare-function duckdb-execute "duckdb-core.so" (conn-ptr sql &optional params))
(declare-function duckdb-select "duckdb-core.so" (conn-ptr sql &optional params))
(declare-function duckdb-select-columns "duckdb-core.so" (conn-ptr sql &optional params))
(declare-function duckdb-prepare "duckdb-core.so" (conn-ptr sql))
(declare-function duckdb-bind "duckdb-core.so" (stmt-ptr params))
(declare-function duckdb-step "duckdb-core.so" (stmt-ptr))
(declare-function duckdb--select-async "duckdb-core.so" (conn-ptr sql callback &optional params))
(declare-function duckdb-async-poll "duckdb-core.so" (ctx))

(defvar duckdb--async-queries nil
  "List of active asynchronous query contexts.")

(defvar duckdb--async-timer nil
  "Timer for polling asynchronous queries.")

(defun duckdb--async-poll-all ()
  "Poll all active asynchronous queries."
  (let ((completed nil))
    (dolist (ctx duckdb--async-queries)
      (condition-case err
          (when (duckdb-async-poll ctx)
            (push ctx completed))
        (duckdb-error
         (message "DuckDB Async Error: %s" (cdr err))
         (push ctx completed))
        (error
         (message "Error during DuckDB async poll: %S" err)
         (push ctx completed))))
    (dolist (ctx completed)
      (setq duckdb--async-queries (delq ctx duckdb--async-queries)))
    (when (and (null duckdb--async-queries) duckdb--async-timer)
      (cancel-timer duckdb--async-timer)
      (setq duckdb--async-timer nil))))

(defun duckdb-select-async (conn-ptr sql callback &optional params)
  "Execute SQL query asynchronously on CONN-PTR and call CALLBACK with results.
Optional PARAMS are bound to the query."
  (let ((ctx (duckdb--select-async conn-ptr sql callback params)))
    (when ctx
      (push ctx duckdb--async-queries)
      (unless duckdb--async-timer
        (setq duckdb--async-timer (run-at-time 0.05 0.05 #'duckdb--async-poll-all)))
      ctx)))

(defmacro with-duckdb (var path &rest body)
  "Open DuckDB at PATH, bind connection to VAR, and execute BODY."
  (declare (indent 2))
  (let ((db-sym (make-symbol "db")))
    `(let* ((,db-sym (duckdb-open ,path))
            (,var (duckdb-connect ,db-sym)))
       (unwind-protect
           (progn ,@body)
         (duckdb-disconnect ,var)
         (duckdb-close ,db-sym)))))

(provide 'duckdb)
;;; duckdb.el ends here
