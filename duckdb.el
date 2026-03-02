;;; duckdb.el --- Emacs DuckDB Dynamic Module -*- lexical-binding: t; -*-

(require 'cl-lib)

;; Load the dynamic module
(require 'duckdb-core)

(defgroup duckdb nil
  "DuckDB integration for Emacs."
  :group 'data)

(define-error 'duckdb-error "DuckDB error" 'error)

(declare-function duckdb-open "duckdb-core.so" (path))

(defmacro with-duckdb (var path &rest body)
  "Open DuckDB at PATH, bind database to VAR, and execute BODY."
  (declare (indent 2))
  `(let ((,var (duckdb-open ,path)))
     (unwind-protect
         (progn ,@body)
       ;; duckdb-close will be called by finalizer, but we can add manual closing later
       )))

(provide 'duckdb)
;;; duckdb.el ends here
