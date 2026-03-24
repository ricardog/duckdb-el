;;; ob-duckdb.el --- Org-babel support for DuckDB -*- lexical-binding: t; -*-

;; Copyright (C) 2026

;; Author: Emacs-DuckDB Contributors
;; Keywords: literate programming, reproducible research, data science, sql, duckdb
;; Package-Requires: ((emacs "28.1") (duckdb "1.0.0"))

;; This file is NOT part of GNU Emacs.

;;; Commentary:

;; This module provides Org-babel support for DuckDB, leveraging the high-performance
;; emacs-duckdb dynamic module. It supports:
;; - SQL execution within Org source blocks.
;; - Session management for stateful database interaction.
;; - Variable binding for simple values and Org-mode tables.
;; - Automatic conversion of DuckDB results to Org-mode tables.

;;; Code:

(require 'duckdb)
(require 'ob)
(require 'cl-lib)

(defgroup ob-duckdb nil
  "Org-babel DuckDB support."
  :group 'org-babel
  :group 'duckdb)

(defvar org-babel-default-header-args:duckdb
  '((:exports . "both") (:results . "table") (:db . ":memory:"))
  "Default arguments for duckdb source blocks.")

(defvar org-babel-duckdb-sessions (make-hash-table :test 'equal)
  "Active DuckDB sessions for org-babel.
Each value is a plist with :db and :conn.")

(defun org-babel-execute:duckdb (body params)
  "Execute a block of DuckDB SQL with org-babel.
BODY is the SQL query string.
PARAMS is a plist of header arguments."
  (let* ((db-path (or (cdr (assoc :db params)) ":memory:"))
         (session-name (cdr (assoc :session params)))
         (colnames (not (string= (cdr (assoc :colnames params)) "no")))
         (vars (org-babel--get-vars params))
         (is-session (and session-name (not (string= session-name "none"))))
         (conn nil)
         (temp-db nil)
         (results nil))
    
    (if is-session
        (setq conn (org-babel-duckdb-get-session-connection db-path session-name))
      ;; No session - open temporary db and connection
      (setq temp-db (duckdb-open db-path))
      (setq conn (duckdb-connect temp-db)))

    (unwind-protect
        (progn
          ;; Bind variables
          (dolist (var vars)
            (org-babel-duckdb-bind-var conn (car var) (cdr var)))
          ;; Execute query
          (condition-case err
              (let ((raw-results (duckdb-select-columns conn body)))
                (setq results (org-babel-duckdb-results-to-org-table raw-results colnames)))
            (duckdb-error
             (error "DuckDB error in org-babel: %s" (cadr err)))))
      ;; Cleanup if not a session
      (unless is-session
        (when conn (ignore-errors (duckdb-disconnect conn)))
        (when temp-db (ignore-errors (duckdb-close temp-db)))))
    results))

(defun org-babel-duckdb-get-session-connection (db-path session-name)
  "Get or create a DuckDB connection for DB-PATH and SESSION-NAME."
  (let ((session (gethash session-name org-babel-duckdb-sessions)))
    ;; Verify connection is still alive
    (unless (and session 
                 (plist-get session :conn)
                 (ignore-errors (duckdb-query-type (plist-get session :conn) "SELECT 1")))
      (let* ((db (duckdb-open db-path))
             (conn (duckdb-connect db)))
        (setq session (list :db db :conn conn))
        (puthash session-name session org-babel-duckdb-sessions)))
    (plist-get session :conn)))

(defun org-babel-duckdb-kill-session (session-name)
  "Kill the DuckDB session named SESSION-NAME."
  (interactive
   (list (completing-read "Kill session: " (hash-table-keys org-babel-duckdb-sessions))))
  (let ((session (gethash session-name org-babel-duckdb-sessions)))
    (when session
      (let ((conn (plist-get session :conn))
            (db (plist-get session :db)))
        (when conn (ignore-errors (duckdb-disconnect conn)))
        (when db (ignore-errors (duckdb-close db))))
      (remhash session-name org-babel-duckdb-sessions)
      (message "DuckDB session '%s' killed." session-name))))

(defun org-babel-duckdb--to-csv (table)
  "Convert a list of lists TABLE to a CSV string."
  (mapconcat
   (lambda (row)
     (mapconcat
      (lambda (cell)
        (let ((s (format "%s" cell)))
          (if (or (string-match-p "," s)
                  (string-match-p "\"" s)
                  (string-match-p "\n" s))
              (concat "\"" (replace-regexp-in-string "\"" "\"\"" s) "\"" )
            s)))
      row ","))
   table "\n"))

(defun org-babel-duckdb-bind-var (conn name value)
  "Bind NAME to VALUE in CONN.
If VALUE is a list of lists, it's treated as a table and bound via a TABLE.
Otherwise, it's bound as a simple value view."
  (if (and (listp value) (listp (car value))) ;; It's a table (list of lists)
      (let ((tmpfile (make-temp-file "duckdb-org-var-" nil ".csv")))
        (unwind-protect
            (progn
              (with-temp-file tmpfile
                (insert (org-babel-duckdb--to-csv value)))
              ;; Use TABLE instead of VIEW so we can delete the file immediately
              (duckdb-execute conn (format "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s')" (symbol-name name) tmpfile)))
          (when (file-exists-p tmpfile)
            (delete-file tmpfile))))
    ;; Simple value
    (let ((sql-val (cond
                  ((stringp value) (format "'%s'" (replace-regexp-in-string "'" "''" value)))
                  ((numberp value) (format "%s" value))
                  ((null value) "NULL")
                  (t (format "'%s'" (replace-regexp-in-string "'" "''" (prin1-to-string value)))))))
      (duckdb-execute conn (format "CREATE OR REPLACE VIEW %s AS SELECT %s AS val" (symbol-name name) sql-val)))))

(defun org-babel-duckdb-results-to-org-table (results colnames)
  "Convert duckdb-select-columns RESULTS to an Org table.
If COLNAMES is non-nil, include headers."
  (let* ((data-plist (plist-get results :data))
         (keys (cl-loop for (k v) on data-plist by 'cddr collect k))
         (cols (mapcar (lambda (k) (plist-get data-plist k)) keys))
         (num-rows (if cols (length (car cols)) 0))
         (rows nil))
    (when (and colnames keys)
      (push (mapcar (lambda (k) (substring (symbol-name k) 1)) keys) rows)
      (push 'hline rows))
    (cl-loop for r from 0 to (1- num-rows)
             do (push (mapcar (lambda (c) (aref c r)) cols) rows))
    (if (null rows)
        nil
      (nreverse rows))))

(provide 'ob-duckdb)
;;; ob-duckdb.el ends here
