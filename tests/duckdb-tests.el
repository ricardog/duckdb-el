;;; duckdb-tests.el --- Tests for emacs-duckdb -*- lexical-binding: t; -*-

(require 'ert)
(require 'duckdb)

(ert-deftest duckdb-open-memory ()
  "Test opening a DuckDB memory database."
  (let ((db (duckdb-open ":memory:")))
    (should (user-ptrp db))))

(ert-deftest duckdb-open-error ()
  "Test opening an invalid path should signal duckdb-error."
  (should-error (duckdb-open "/nonexistent/path/to/db")
                :type 'duckdb-error))

(provide 'duckdb-tests)
;;; duckdb-tests.el ends here
