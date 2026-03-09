;;; duckdb-leak-tests.el --- Tests for resource leak prevention -*- lexical-binding: t; -*-

(require 'ert)
(require 'duckdb)
(require 'cl-lib)

(ert-deftest duckdb-with-duckdb-leak-test ()
  "Test that with-duckdb closes db if connect fails."
  (let ((db-closed nil)
        (conn-disconnected nil))
    (cl-letf (((symbol-function 'duckdb-open) (lambda (_) :mock-db))
              ((symbol-function 'duckdb-connect) (lambda (_) (signal 'duckdb-error '("Connect Failed"))))
              ((symbol-function 'duckdb-close) (lambda (db) (when (eq db :mock-db) (setq db-closed t))))
              ((symbol-function 'duckdb-disconnect) (lambda (_) (setq conn-disconnected t))))
      (should-error (with-duckdb conn ":memory:" (message "Should not run")))
      (should db-closed)
      (should-not conn-disconnected))))

(ert-deftest duckdb-mode-open-file-leak-test ()
  "Test that duckdb-mode-open-file cleans up on failure."
  (let ((db-closed nil)
        (conn-disconnected nil)
        (buffer-killed nil)
        (real-kill-buffer (symbol-function 'kill-buffer)))
    (cl-letf (((symbol-function 'duckdb-open) (lambda (_) :mock-db))
              ((symbol-function 'duckdb-connect) (lambda (_) :mock-conn))
              ((symbol-function 'duckdb-browse-refresh) (lambda () (signal 'duckdb-error '("Refresh Failed"))))
              ((symbol-function 'duckdb-close) (lambda (db) (when (eq db :mock-db) (setq db-closed t))))
              ((symbol-function 'duckdb-disconnect) (lambda (conn) (when (eq conn :mock-conn) (setq conn-disconnected t))))
              ((symbol-function 'kill-buffer) (lambda (buf) 
                                                (when (and (bufferp buf) (string-match "\\*DuckDB:" (buffer-name buf)))
                                                  (setq buffer-killed t))
                                                (funcall real-kill-buffer buf))))
      (should-error (duckdb-mode-open-file ":memory:"))
      (should db-closed)
      (should conn-disconnected)
      (should buffer-killed))))

(ert-deftest duckdb-insert-buffer-leak-test ()
  "Test that duckdb-insert-buffer cleans up on failure."
  (let ((db-closed nil)
        (conn-disconnected nil))
    (cl-letf (((symbol-function 'duckdb-open) (lambda (_) :mock-db))
              ((symbol-function 'duckdb-connect) (lambda (_) :mock-conn))
              ((symbol-function 'duckdb-get-tables) (lambda (_) (signal 'duckdb-error '("Get Tables Failed"))))
              ((symbol-function 'duckdb-close) (lambda (db) (when (eq db :mock-db) (setq db-closed t))))
              ((symbol-function 'duckdb-disconnect) (lambda (conn) (when (eq conn :mock-conn) (setq conn-disconnected t)))))
      (should-error (duckdb-insert-buffer ":memory:" "test-table"))
      (should db-closed)
      (should conn-disconnected))))

(ert-deftest duckdb-browse-cleanup-brittle-test ()
  "Test that duckdb-browse-cleanup closes db even if disconnect fails."
  (let ((db-closed nil))
    (let ((buf (get-buffer-create "*DuckDB: cleanup-test*")))
      (unwind-protect
          (with-current-buffer buf
            (setq-local duckdb-current-connection :mock-conn)
            (setq-local duckdb--db-ptr :mock-db)
            (cl-letf (((symbol-function 'duckdb-disconnect) (lambda (_) (signal 'error '("Disconnect Failed"))))
                      ((symbol-function 'duckdb-close) (lambda (db) (when (eq db :mock-db) (setq db-closed t)))))
              (duckdb-browse-cleanup)
              (should db-closed)))
        (kill-buffer buf)))))

(provide 'duckdb-leak-tests)
