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
      (setq duckdb--async-queries (delq ctx duckdb--async-queries)))))

(defun duckdb--handle-sigusr1 ()
  "Handle SIGUSR1 signal for async query completion."
  (interactive)
  (duckdb--async-poll-all))

;; Bind SIGUSR1 to our handler
(define-key special-event-map [sigusr1] #'duckdb--handle-sigusr1)

(defun duckdb-select-async (conn-ptr sql callback &optional params)
  "Execute SQL query asynchronously on CONN-PTR and call CALLBACK with results.
Optional PARAMS are bound to the query."
  (let ((ctx (duckdb--select-async conn-ptr sql callback params)))
    (when ctx
      (push ctx duckdb--async-queries)
      ctx)))

;;; High-level Wrapper

(defvar-local duckdb-current-connection nil
  "The current DuckDB connection for this buffer.")

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

(defun duckdb-get-tables (conn-ptr)
  "Return a list of all table names in the current database for CONN-PTR."
  (let ((results (duckdb-select conn-ptr "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")))
    (mapcar #'car results)))

(defun duckdb-get-columns (conn-ptr table)
  "Return a list of all column names for TABLE in CONN-PTR."
  (let ((results (duckdb-select conn-ptr "SELECT column_name FROM information_schema.columns WHERE table_name = ?" (list table))))
    (mapcar #'car results)))

;; Completion at point
(defun duckdb-completion-at-point ()
  "Completion at point for DuckDB SQL."
  (let* ((bounds (bounds-of-thing-at-point 'symbol))
         (start (or (car bounds) (point)))
         (end (or (cdr bounds) (point))))
    (when duckdb-current-connection
      (list start end
            (completion-table-dynamic
             (lambda (str)
               (let ((tables (duckdb-get-tables duckdb-current-connection)))
                 (if (member str tables)
                     (duckdb-get-columns duckdb-current-connection str)
                   tables))))
            :exclusive 'no))))

(define-derived-mode duckdb-sql-mode sql-mode "DuckDB SQL"
  "Major mode for editing DuckDB SQL."
  (add-hook 'completion-at-point-functions #'duckdb-completion-at-point nil t))

;; SQL-Scrubber (Interactive results)
(define-derived-mode duckdb-edit-mode tabulated-list-mode "DuckDB Edit"
  "Major mode for displaying DuckDB query results."
  (setq-local tabulated-list-padding 2)
  (tabulated-list-init-header))

(defun duckdb--render-results (columns rows)
  "Render COLUMNS and ROWS into a DuckDB Results buffer."
  (let ((buf (get-buffer-create "*DuckDB Results*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (if (and (fboundp 'vtable-insert) (featurep 'vtable))
            (vtable-insert :columns columns
                           :objects rows
                           :separator " | ")
          ;; Fallback to tabulated-list-mode
          (duckdb-edit-mode)
          (setq tabulated-list-format
                (vconcat (mapcar (lambda (col) (list col 20 t)) columns)))
          (setq tabulated-list-entries
                (cl-loop for row in rows
                         for i from 0
                         collect (list i (vconcat (mapcar (lambda (val) (format "%s" val)) row)))))
          (tabulated-list-print t))))
    (display-buffer buf)))

(defun duckdb-query-and-display (conn-ptr sql &optional params)
  "Execute SQL on CONN-PTR and display results."
  (interactive (list duckdb-current-connection (read-string "SQL: ")))
  (unless conn-ptr
    (error "No active DuckDB connection"))
  (let* ((results (duckdb-select-columns conn-ptr sql params))
         (keys (cl-loop for (k v) on results by 'cddr collect k))
         (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
         (data (cl-loop for (k v) on results by 'cddr collect v))
         (num-rows (if data (length (car data)) 0))
         (rows (cl-loop for r from 0 to (1- num-rows)
                        collect (mapcar (lambda (col-vec) (aref col-vec r)) data))))
    (duckdb--render-results columns rows)))

;; Data Ingestor
(defun duckdb-insert-buffer (conn-ptr table-name &optional buffer)
  "Insert the contents of BUFFER into TABLE-NAME in DuckDB via CONN-PTR.
If BUFFER is nil, use the current buffer."
  (interactive (list duckdb-current-connection (read-string "Table name: ")))
  (unless conn-ptr
    (error "No active DuckDB connection"))
  (let* ((buf (or buffer (current-buffer)))
         (file (buffer-file-name buf)))
    (if file
        (duckdb-execute conn-ptr (format "COPY %s FROM '%s' (AUTO_DETECT TRUE)" table-name file))
      ;; If not a file, write to a temp file
      (let ((temp-file (make-temp-file "duckdb-insert-")))
        (with-current-buffer buf
          (write-region (point-min) (point-max) temp-file))
        (unwind-protect
            (duckdb-execute conn-ptr (format "COPY %s FROM '%s' (AUTO_DETECT TRUE)" table-name temp-file))
          (delete-file temp-file))))))

(provide 'duckdb)
;;; duckdb.el ends here
