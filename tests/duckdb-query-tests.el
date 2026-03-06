;;; duckdb-query-tests.el --- Tests for DuckDB Querying -*- lexical-binding: t; -*-

(require 'ert)
(require 'duckdb)

(ert-deftest duckdb-query-type-test ()
  "Test duckdb-query-type for different SQL statements."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER)")
    (should (eq (duckdb-query-type conn "SELECT 1") 'SELECT))
    (should (eq (duckdb-query-type conn "EXPLAIN SELECT 1") 'EXPLAIN))
    (should (member (duckdb-query-type conn "PRAGMA version") '(PRAGMA SELECT)))
    (should (eq (duckdb-query-type conn "INSERT INTO test VALUES (1)") 'INSERT))
    (should (eq (duckdb-query-type conn "UPDATE test SET id = 2") 'UPDATE))
    (should (eq (duckdb-query-type conn "DELETE FROM test") 'DELETE))
    (should (eq (duckdb-query-type conn "DROP TABLE test") 'DROP))))

(ert-deftest duckdb-browse-query-interactive-test ()
  "Test interactive duckdb-browse-query (mocked window and input)."
  (with-duckdb conn ":memory:"
    (let ((query-buf nil))
      (cl-letf (((symbol-function 'pop-to-buffer) (lambda (buf &rest _) (setq query-buf buf)))
                ((symbol-function 'display-buffer) (lambda (buf &rest _) (setq popped-results-buf buf) (selected-window))))
        (let ((browse-buf (get-buffer-create "*DuckDB: :memory:*")))
          (with-current-buffer browse-buf
            (duckdb-browse-mode)
            (setq-local duckdb-current-connection conn)
            (duckdb-browse-query))
          
          (should (bufferp query-buf))
          (with-current-buffer query-buf
            (should (eq major-mode 'duckdb-query-edit-mode))
            (should (eq duckdb-current-connection conn))
            (should (string-match "SELECT" (buffer-string))))
          
          (kill-buffer query-buf)
          (kill-buffer browse-buf))))))

(ert-deftest duckdb-browse-mode-keys-test ()
  "Test keybindings in duckdb-browse-mode."
  (let ((buf (get-buffer-create "*DuckDB: test*")))
    (with-current-buffer buf
      (duckdb-browse-mode)
      (should (eq (lookup-key duckdb-browse-mode-map (kbd "Q")) 'duckdb-browse-query))
      (should (eq (lookup-key duckdb-browse-mode-map (kbd "q")) 'quit-window)))
    (kill-buffer buf)))

(ert-deftest duckdb-query-edit-run-strict-policy-test ()
  "Test that duckdb-query-edit-run enforces strict policy."
  (with-duckdb conn ":memory:"
    (let ((edit-buf (get-buffer-create "*DuckDB Query: memory*")))
      (with-current-buffer edit-buf
        (duckdb-query-edit-mode)
        (setq-local duckdb-current-connection conn)
        (erase-buffer)
        (insert "DELETE FROM test")
        (should-error (duckdb-query-edit-run) :type 'error)
        
        (erase-buffer)
        (insert "SELECT 1")
        ;; Mock duckdb--query-execute to avoid actual execution issues with missing tables
        (cl-letf (((symbol-function 'duckdb--query-execute) (lambda (&rest _) t)))
          (should (duckdb-query-edit-run))))
      (kill-buffer edit-buf))))

(provide 'duckdb-query-tests)
