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

(ert-deftest duckdb-query-results-mode-bindings-test ()
  "Test that global and local keybindings are accessible in duckdb-query-results-mode."
  (with-duckdb conn ":memory:"
    (let ((edit-buf (get-buffer-create "*DuckDB Edit Test*")))
      (with-current-buffer edit-buf (duckdb-query-edit-mode))
      ;; Simulate rendering results into the buffer
      (duckdb--query-render-results '("id") '(("1")) "SELECT 1" 0 nil edit-buf conn nil nil)
      (let ((results-buf (get-buffer "*DuckDB Query Results*")))
        (should (bufferp results-buf))
        (with-current-buffer results-buf
          ;; Verify major mode is preserved and not overwritten by tabulated-list-mode
          (should (eq major-mode 'duckdb-query-results-mode))
          
          ;; Check standard global/special-mode bindings (if present in environment)
          (let ((mx (key-binding (kbd "M-x")))
		(my (key-binding (kbd "M-y")))
                (q (key-binding (kbd "q"))))
            (should (eq mx 'execute-extended-command))
            (should (eq my 'yank-pop))
            ;; 'q' is bound in special-mode and overridden in our results mode
            (should (eq q 'duckdb-query-results-quit)))
          
          ;; Check mode-specific bindings that were reported as missing
          (should (eq (key-binding (kbd "e")) 'duckdb-query-results-edit))
          (should (eq (key-binding (kbd "m")) 'duckdb-query-results-fetch-more)))
        (kill-buffer results-buf)
        (kill-buffer edit-buf)))))

(ert-deftest duckdb-query-swap-logic-test ()
  "Test the buffer swapping logic between edit and results."
  (with-duckdb conn ":memory:"
    (let* ((edit-buf (get-buffer-create "*DuckDB Edit Test*"))
           (results-buf (get-buffer-create "*DuckDB Query Results*")))
      (with-current-buffer edit-buf
        (duckdb-query-edit-mode)
        (setq-local duckdb-current-connection conn))
      (with-current-buffer results-buf
        (duckdb-query-results-mode)
        (setq-local duckdb--query-edit-buffer edit-buf))
      
      ;; Test swap to edit
      (with-temp-buffer
        (let ((win (display-buffer results-buf)))
          (with-selected-window win
            (duckdb-query-results-edit)
            (should (eq (window-buffer win) edit-buf)))))
      
      ;; Test swap back to results (simulated via render call)
      (with-temp-buffer
        (let ((win (display-buffer edit-buf)))
          (with-selected-window win
            (duckdb--query-render-results '("col") '(("val")) "SELECT 1" 0 nil edit-buf conn nil nil)
            (should (eq (window-buffer win) (get-buffer "*DuckDB Query Results*"))))))
      
      (kill-buffer edit-buf)
      (kill-buffer results-buf))))

(provide 'duckdb-query-tests)
