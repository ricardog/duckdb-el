;;; duckdb-interactive-error-tests.el --- Tests for error handling in interactive functions -*- lexical-binding: t; -*-

(require 'ert)
(require 'duckdb)
(require 'cl-lib)

(ert-deftest duckdb-browse-refresh-error-test ()
  "Test that duckdb-browse-refresh handles duckdb-error."
  (with-duckdb conn ":memory:"
    (let ((buf (get-buffer-create "*DuckDB: error-test*")))
      (unwind-protect
          (with-current-buffer buf
            (duckdb-browse-mode)
            (setq-local duckdb-current-connection conn)
            ;; Mock duckdb--get-tables-with-counts to signal duckdb-error
            (cl-letf (((symbol-function 'duckdb--get-tables-with-counts)
                       (lambda (_) (signal 'duckdb-error '("Mocked DuckDB Error")))))
              (let ((err (should-error (call-interactively #'duckdb-browse-refresh))))
                (should (eq (car err) 'error))
                (should (string= (cadr err) "Mocked DuckDB Error")))))
        (kill-buffer buf)))))

(ert-deftest duckdb-browse-toggle-table-error-test ()
  "Test that duckdb-browse-toggle-table handles duckdb-error."
  (with-duckdb conn ":memory:"
    (let ((buf (get-buffer-create "*DuckDB: error-test*")))
      (unwind-protect
          (with-current-buffer buf
            (duckdb-browse-mode)
            (setq-local duckdb-current-connection conn)
            (let ((inhibit-read-only t))
              (insert (propertize "test-table" 'duckdb-table-name "test-table")))
            (goto-char (point-min))
            ;; Mock duckdb--expand-table to signal duckdb-error
            (cl-letf (((symbol-function 'duckdb--expand-table)
                       (lambda (_) (signal 'duckdb-error '("Expand Error")))))
              (let ((err (should-error (call-interactively #'duckdb-browse-toggle-table))))
                (should (eq (car err) 'error))
                (should (string= (cadr err) "Expand Error")))))
        (kill-buffer buf)))))

(ert-deftest duckdb-browse-toggle-columns-error-test ()
  "Test that duckdb-browse-toggle-columns handles duckdb-error."
  (with-duckdb conn ":memory:"
    (let ((buf (get-buffer-create "*DuckDB: error-test*")))
      (unwind-protect
          (with-current-buffer buf
            (duckdb-browse-mode)
            (setq-local duckdb-current-connection conn)
            (let ((inhibit-read-only t))
              (insert (propertize "test-table" 'duckdb-table-name "test-table")))
            (goto-char (point-min))
            ;; Mock duckdb--expand-table-columns to signal duckdb-error
            (cl-letf (((symbol-function 'duckdb--expand-table-columns)
                       (lambda (_) (signal 'duckdb-error '("Expand Columns Error")))))
              (let ((err (should-error (call-interactively #'duckdb-browse-toggle-columns))))
                (should (eq (car err) 'error))
                (should (string= (cadr err) "Expand Columns Error")))))
        (kill-buffer buf)))))

(ert-deftest duckdb-set-connection-error-test ()
  "Test that duckdb-set-connection handles duckdb-error."
  (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "/invalid/path"))
            ((symbol-function 'duckdb-open) (lambda (_) (signal 'duckdb-error '("Open Error")))))
    (let ((err (should-error (call-interactively #'duckdb-set-connection))))
      (should (eq (car err) 'error))
      (should (string= (cadr err) "Open Error")))))

(ert-deftest duckdb-list-tables-error-test ()
  "Test that duckdb-list-tables handles duckdb-error."
  (with-duckdb conn ":memory:"
    (cl-letf (((symbol-function 'duckdb-select-columns)
               (lambda (&rest _) (signal 'duckdb-error '("List Tables Error")))))
      (let ((err (should-error (duckdb-list-tables conn))))
        (should (eq (car err) 'error))
        (should (string= (cadr err) "List Tables Error"))))))

(ert-deftest duckdb-mode-open-file-error-test ()
  "Test that duckdb-mode-open-file handles duckdb-error."
  (cl-letf (((symbol-function 'read-string) (lambda (&rest _) "/invalid/path"))
            ((symbol-function 'duckdb-open) (lambda (_) (signal 'duckdb-error '("Open File Error")))))
    (let ((err (should-error (call-interactively #'duckdb-mode-open-file))))
      (should (eq (car err) 'error))
      (should (string= (cadr err) "Open File Error")))))

(ert-deftest duckdb-query-and-display-error-test ()
  "Test that duckdb-query-and-display handles duckdb-error."
  (with-duckdb conn ":memory:"
    (cl-letf (((symbol-function 'duckdb--query-and-display-internal)
               (lambda (&rest _) (signal 'duckdb-error '("Query Display Error")))))
      (let ((err (should-error (duckdb-query-and-display conn "SELECT 1"))))
        (should (eq (car err) 'error))
        (should (string= (cadr err) "Query Display Error"))))))

(ert-deftest duckdb-insert-buffer-error-test ()
  "Test that duckdb-insert-buffer handles duckdb-error."
  (with-duckdb conn ":memory:"
    (cl-letf (((symbol-function 'duckdb-get-tables)
               (lambda (&rest _) (signal 'duckdb-error '("Insert Buffer Error")))))
      (let ((err (should-error (duckdb-insert-buffer conn "test-table"))))
        (should (eq (car err) 'error))
        (should (string= (cadr err) "Insert Buffer Error"))))))

(ert-deftest duckdb-query-edit-run-error-test ()
  "Test that duckdb-query-edit-run handles duckdb-error during execution."
  (with-duckdb conn ":memory:"
    (let ((buf (get-buffer-create "*DuckDB Query: error-test*")))
      (unwind-protect
          (with-current-buffer buf
            (duckdb-query-edit-mode)
            (setq-local duckdb-current-connection conn)
            (insert "SELECT * FROM non_existent_table")
            ;; Mock duckdb-query-type to pass, but duckdb--query-execute to fail
            (cl-letf (((symbol-function 'duckdb-query-type) (lambda (&rest _) 'SELECT))
                      ((symbol-function 'duckdb--query-execute)
                       (lambda (&rest _) (signal 'duckdb-error '("Query Execution Error")))))
              (let ((err (should-error (call-interactively #'duckdb-query-edit-run))))
                (should (eq (car err) 'error))
                (should (string= (cadr err) "Query Execution Error")))))
        (kill-buffer buf)))))

(ert-deftest duckdb-query-results-fetch-more-error-test ()
  "Test that duckdb-query-results-fetch-more handles duckdb-error."
  (with-duckdb conn ":memory:"
    (let ((buf (get-buffer-create "*DuckDB Results: error-test*")))
      (unwind-protect
          (with-current-buffer buf
            (duckdb-query-results-mode)
            (setq-local duckdb-current-connection conn)
            (setq-local duckdb--query-sql "SELECT 1")
            (setq-local duckdb--query-offset 0)
            (cl-letf (((symbol-function 'duckdb--query-execute)
                       (lambda (&rest _) (signal 'duckdb-error '("Fetch More Error")))))
              (let ((err (should-error (call-interactively #'duckdb-query-results-fetch-more))))
                (should (eq (car err) 'error))
                (should (string= (cadr err) "Fetch More Error")))))
        (kill-buffer buf)))))

(ert-deftest duckdb-select-async-start-error-test ()
  "Test that duckdb-select-async handles startup errors gracefully."
  (with-duckdb conn ":memory:"
    (let ((err (should-error (duckdb-select-async conn "INVALID SQL" (lambda (_s _r) nil)))))
      (should (eq (car err) 'error))
      (should (string-match "syntax error" (cadr err))))))

(provide 'duckdb-interactive-error-tests)
