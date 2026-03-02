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
(declare-function duckdb-execute "duckdb-core.so" (conn-ptr sql))
(declare-function duckdb-select "duckdb-core.so" (conn-ptr sql))

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
