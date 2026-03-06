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

(defface duckdb-browse-header
  '((t :inherit header-line :background "grey10" :foreground "white" :weight bold))
  "Face for the header in DuckDB browse buffer."
  :group 'duckdb)

(defface duckdb-browse-table-name
  '((t :inherit default :weight bold))
  "Face for table names in DuckDB browse buffer."
  :group 'duckdb)

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
(declare-function duckdb-base64-encode "duckdb-core.so" (string))
(declare-function duckdb-base64-decode "duckdb-core.so" (string))
(declare-function duckdb-query-type "duckdb-core.so" (conn-ptr sql))

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

(defvar-local duckdb--db-ptr nil
  "The DuckDB database pointer for this buffer.")

(defvar-local duckdb--db-path nil
  "The path to the DuckDB database for this buffer.")

(defvar-local duckdb--expanded-table nil
  "The name of the currently expanded table.")

(defvar-local duckdb--expanded-overlay nil
  "The overlay for the currently expanded table data.")

(defvar duckdb-path-history nil
  "History of DuckDB database paths.")

(defun duckdb-browse-cleanup ()
  "Cleanup connection when buffer is killed."
  (when duckdb-current-connection
    (duckdb-disconnect duckdb-current-connection)
    (setq duckdb-current-connection nil))
  (when duckdb--db-ptr
    (duckdb-close duckdb--db-ptr)
    (setq duckdb--db-ptr nil)))

(define-derived-mode duckdb-browse-mode special-mode "DuckDB-Browse"
  "Major mode for browsing DuckDB tables."
  (setq truncate-lines t)
  (setq-local revert-buffer-function #'duckdb-browse-refresh)
  (add-hook 'kill-buffer-hook #'duckdb-browse-cleanup nil t))

(define-key duckdb-browse-mode-map (kbd "RET") #'duckdb-browse-toggle-table)
(define-key duckdb-browse-mode-map (kbd "c") #'duckdb-browse-toggle-columns)
(define-key duckdb-browse-mode-map (kbd "Q") #'duckdb-browse-query)

(defun duckdb-browse-refresh (&optional _ignore-auto _noconfirm)
  "Refresh the table list."
  (interactive)
  (let ((inhibit-read-only t)
        (pos (point)))
    (erase-buffer)
    (let ((header (format "%-20s %15s\n" "Table Name" "Number of Rows")))
      (insert (propertize header 'face 'duckdb-browse-header)))
    (insert (make-string 36 ?-) "\n")
    (let ((tables (duckdb--get-tables-with-counts duckdb-current-connection)))
      (dolist (table-info tables)
        (let ((name (car table-info))
              (count (cadr table-info))
              (start (point)))
          (insert (format "%-20s %15s\n" name count))
          (add-text-properties start (1- (point)) 
                               `(duckdb-table-name ,name 
                                 face duckdb-browse-table-name
                                 mouse-face highlight 
                                 help-echo "RET: toggle data, c: toggle columns, Q: query, q: quit")))))
    (setq duckdb--expanded-table nil)
    (setq duckdb--expanded-overlay nil)
    (goto-char (min pos (point-max)))
    (if (< (point) (point-min)) (goto-char (point-min)))))

(defun duckdb--get-tables-with-counts (conn)
  "Get all tables in CONN and their row counts, sorted by name."
  (let ((tables (duckdb-select conn "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name")))
    (mapcar (lambda (row)
              (let* ((table (car row))
                     (columns (duckdb-get-columns conn table))
                     (col (car columns))
                     (count-sql (if col
                                    (format "SELECT count(%s) FROM %s" col table)
                                  (format "SELECT count(*) FROM %s" table)))
                     (count-res (duckdb-select conn count-sql)))
                (list table (caar count-res))))
            tables)))

(defun duckdb-browse-toggle-table ()
  "Toggle the display of the table at point (data preview)."
  (interactive)
  (let ((table (get-text-property (point) 'duckdb-table-name)))
    (if (not table)
        (message "No table at point")
      (if (and (string= table duckdb--expanded-table)
               (eq (overlay-get duckdb--expanded-overlay 'duckdb-type) 'data))
          (duckdb--collapse-table)
        (duckdb--collapse-table) ; Collapse existing if any
        (duckdb--expand-table table)))))

(defun duckdb-browse-toggle-columns ()
  "Toggle the display of columns and types for the table at point."
  (interactive)
  (let ((table (get-text-property (point) 'duckdb-table-name)))
    (if (not table)
        (message "No table at point")
      (if (and (string= table duckdb--expanded-table)
               (eq (overlay-get duckdb--expanded-overlay 'duckdb-type) 'columns))
          (duckdb--collapse-table)
        (duckdb--collapse-table)
        (duckdb--expand-table-columns table)))))

(defun duckdb--collapse-table ()
  "Collapse currently expanded table."
  (when duckdb--expanded-overlay
    (let ((inhibit-read-only t))
      (delete-region (overlay-start duckdb--expanded-overlay)
                     (overlay-end duckdb--expanded-overlay))
      (delete-overlay duckdb--expanded-overlay)
      (setq duckdb--expanded-overlay nil)
      (setq duckdb--expanded-table nil))))

(defun duckdb--expand-table (table-name)
  "Expand TABLE-NAME with data preview."
  (let ((inhibit-read-only t))
    (save-excursion
      (forward-line 1)
      (let ((start (point))
            (data (duckdb--get-table-preview duckdb-current-connection table-name)))
        (insert data)
        (setq duckdb--expanded-table table-name)
        (setq duckdb--expanded-overlay (make-overlay start (point)))
        (overlay-put duckdb--expanded-overlay 'duckdb-type 'data)))))

(defun duckdb--expand-table-columns (table-name)
  "Expand TABLE-NAME with column information."
  (let ((inhibit-read-only t))
    (save-excursion
      (forward-line 1)
      (let ((start (point))
            (columns-info (duckdb--get-table-columns-info duckdb-current-connection table-name)))
        (insert "  " (propertize "Columns:" 'face 'duckdb-browse-header) "\n")
        (insert (format "    %-20s %s\n"
                        (propertize "Column Name" 'face 'duckdb-browse-header)
                        (propertize "Data Type" 'face 'duckdb-browse-header)))
        (dolist (info columns-info)
          (insert (format "    %-20s %s\n" (car info) (cadr info))))
        (setq duckdb--expanded-table table-name)
        (setq duckdb--expanded-overlay (make-overlay start (point)))
        (overlay-put duckdb--expanded-overlay 'duckdb-type 'columns)))))

(defun duckdb--get-table-columns-info (conn table)
  "Return a list of (column_name data_type) for TABLE in CONN."
  (duckdb-select conn "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position" (list table)))

(defun duckdb--format-value (val type)
  "Format VAL based on its DuckDB TYPE."
  (cond
   ((null val) "NULL")
   ((string= type "TIMESTAMP")
    (let ((secs (/ val 1000000))
          (micros (% val 1000000)))
      (format-time-string "%Y-%m-%d %H:%M:%S" (seconds-to-time secs))))
   ((and (stringp val) (not (multibyte-string-p val)))
    (if (> (length val) 1000)
        (format "<BLOB %d bytes>" (length val))
      (replace-regexp-in-string "\n" " " (prin1-to-string val))))
   (t (replace-regexp-in-string "\n" " " (format "%s" val)))))

(defun duckdb--get-table-preview (conn table)
  "Get first 100 rows of TABLE and format them."
  (let* ((sql (format "SELECT * FROM %s LIMIT 100" table))
         (results (duckdb-select-columns conn sql))
         (data-plist (plist-get results :data))
         (types-plist (plist-get results :types))
         (keys (cl-loop for (k v) on data-plist by 'cddr collect k))
         (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
         (data (cl-loop for (k v) on data-plist by 'cddr collect v))
         (types (cl-loop for (k v) on types-plist by 'cddr collect v))
         (num-rows (if data (length (car data)) 0))
         (rows (cl-loop for r from 0 to (1- num-rows)
                        collect (cl-loop for col-vec in data
                                         for type in types
                                         collect (duckdb--format-value (aref col-vec r) type)))))
    (duckdb--format-preview-data columns rows)))

(defun duckdb--format-preview-data (columns rows)
  "Format COLUMNS and ROWS for preview display."
  (if (null columns)
      "  (No columns)\n"
    (let* ((widths (mapcar #'string-width columns))
           (pad (lambda (s w)
                  (let ((sw (string-width s)))
                    (if (>= sw w) s
                      (concat s (make-string (- w sw) ?\s))))))
           (widths (cl-loop for row in rows
                            do (setq widths (cl-loop for val in row
                                                     for w in widths
                                                     collect (max w (string-width (if (stringp val) val (format "%s" val))))))
                            finally return widths))
           (out ""))
      ;; Header
      (setq out (concat out "  "))
      (cl-loop for col in columns
               for w in widths
               do (setq out (concat out (propertize (funcall pad col w) 'face 'duckdb-browse-header) "  ")))
      (setq out (concat out "\n"))
      ;; Rows
      (dolist (row rows)
        (setq out (concat out "  "))
        (cl-loop for val in row
                 for w in widths
                 do (let ((s (if (stringp val) val (format "%s" val))))
                      (setq out (concat out (funcall pad s w) "  "))))
        (setq out (concat out "\n")))
      out)))

(defun duckdb-blob (data)
  "Create a BLOB parameter from DATA (a unibyte string)."
  (list :blob data))

(defun duckdb-set-connection (path)
  "Set the current DuckDB connection for the buffer to PATH."
  (interactive (list (read-string "DuckDB Path: " ":memory:" 'duckdb-path-history ":memory:")))
  (let* ((db (duckdb-open path))
         (conn (duckdb-connect db)))
    (setq-local duckdb-current-connection conn)
    (message "Buffer connected to %s" path)))

(defun duckdb-list-tables (&optional conn-ptr)
  "List all tables in the database for CONN-PTR."
  (interactive (list duckdb-current-connection))
  (let ((conn (or conn-ptr duckdb-current-connection)))
    (unless conn
      (error "No active DuckDB connection"))
    (let* ((results (duckdb-select-columns conn "SELECT table_name, table_schema, table_type FROM information_schema.tables WHERE table_schema = 'main'"))
           (data-plist (plist-get results :data))
           (types-plist (plist-get results :types))
           (keys (cl-loop for (k v) on data-plist by 'cddr collect k))
           (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
           (data (cl-loop for (k v) on data-plist by 'cddr collect v))
           (types (cl-loop for (k v) on types-plist by 'cddr collect v))
           (num-rows (if data (length (car data)) 0))
           (rows (cl-loop for r from 0 to (1- num-rows)
                          collect (cl-loop for col-vec in data
                                           for type in types
                                           collect (duckdb--format-value (aref col-vec r) type)))))
      (duckdb--render-results columns rows))))

(defun duckdb-mode-open-file (path)
  "Open DuckDB database at PATH and browse its tables."
  (interactive (list (read-string "DuckDB Path: " ":memory:" 'duckdb-path-history ":memory:")))
  (let* ((db (duckdb-open path))
         (conn (duckdb-connect db))
         (buf (get-buffer-create (format "*DuckDB: %s*" path))))
    (with-current-buffer buf
      (duckdb-browse-mode)
      (setq-local duckdb-current-connection conn)
      (setq-local duckdb--db-ptr db)
      (setq-local duckdb--db-path path)
      (duckdb-browse-refresh))
    (switch-to-buffer buf)))

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

(defun duckdb--get-db-or-path ()
  "Get the current connection or prompt for a path."
  (or duckdb-current-connection
      (read-string "DuckDB Path: " ":memory:" 'duckdb-path-history ":memory:")))

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
(define-derived-mode duckdb-edit-mode special-mode "DuckDB Edit"
  "Major mode for displaying DuckDB query results."
  (setq-local tabulated-list-padding 2))

(defun duckdb--render-results (columns rows)
  "Render COLUMNS and ROWS into a DuckDB Results buffer."
  (let ((buf (get-buffer-create "*DuckDB Results*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (duckdb-edit-mode)
        (if (and (fboundp 'vtable-insert) (featurep 'vtable))
            (vtable-insert :columns columns
                           :objects (mapcar (lambda (row)
                                              (mapcar (lambda (val)
                                                        (replace-regexp-in-string "\n" " " (format "%s" val)))
                                                      row))
                                            rows)
                           :separator " | ")
          ;; Fallback to tabulated-list-mode
          (tabulated-list-mode)
          (setq tabulated-list-format
                (vconcat (mapcar (lambda (col) (list col 20 t)) columns)))
          (setq tabulated-list-entries
                (cl-loop for row in rows
                         for i from 0
                         collect (list i (vconcat (mapcar (lambda (val)
                                                           (replace-regexp-in-string "\n" " " (format "%s" val)))
                                                         row)))))
          (tabulated-list-init-header)
          (tabulated-list-print t))))
    (display-buffer buf)))

(defun duckdb--query-and-display-internal (conn-ptr sql &optional params)
  "Internal helper for query and display."
  (let* ((results (duckdb-select-columns conn-ptr sql params))
         (data-plist (plist-get results :data))
         (types-plist (plist-get results :types))
         (keys (cl-loop for (k v) on data-plist by 'cddr collect k))
         (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
         (data (cl-loop for (k v) on data-plist by 'cddr collect v))
         (types (cl-loop for (k v) on types-plist by 'cddr collect v))
         (num-rows (if data (length (car data)) 0))
         (rows (cl-loop for r from 0 to (1- num-rows)
                        collect (cl-loop for col-vec in data
                                         for type in types
                                         collect (duckdb--format-value (aref col-vec r) type)))))
    (duckdb--render-results columns rows)))

(defun duckdb-query-and-display (db-or-path sql &optional params)
  "Execute SQL on DB-OR-PATH and display results.
DB-OR-PATH can be a connection pointer or a string path."
  (interactive (list (duckdb--get-db-or-path) (read-string "SQL: ")))
  (if (stringp db-or-path)
      (with-duckdb conn db-or-path
        (duckdb--query-and-display-internal conn sql params))
    (duckdb--query-and-display-internal db-or-path sql params)))

;; Data Ingestor
(defun duckdb-insert-buffer (db-or-path table-name &optional buffer)
  "Insert the contents of BUFFER into TABLE-NAME in DuckDB via DB-OR-PATH.
DB-OR-PATH can be a connection pointer or a string path.
If BUFFER is nil, use the current buffer.
If the table does not exist, it will be created automatically using read_csv_auto.

Returns the connection object.
When called interactively, open a DuckDB Browser buffer for the database.
With a prefix argument, use an in-memory database and the buffer's basename
as the table name without prompting."
  (interactive
   (if current-prefix-arg
       (list ":memory:"
             (file-name-sans-extension (file-name-nondirectory (or (buffer-file-name) (buffer-name)))))
     (list (duckdb--get-db-or-path) (read-string "Table name: "))))
  (let* ((buf (or buffer (current-buffer)))
         (file (buffer-file-name buf))
         (db-ptr nil)
         (conn-ptr nil)
         (is-new-conn nil))
    (if (stringp db-or-path)
        (setq db-ptr (duckdb-open db-or-path)
              conn-ptr (duckdb-connect db-ptr)
              is-new-conn t)
      (setq conn-ptr db-or-path))
    (unwind-protect
        (let ((exists (member table-name (duckdb-get-tables conn-ptr))))
          (if file
              (if exists
                  (duckdb-execute conn-ptr (format "COPY %s FROM '%s' (AUTO_DETECT TRUE)" table-name file))
                (duckdb-execute conn-ptr (format "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')" table-name file)))
            ;; If not a file, write to a temp file
            (let ((temp-file (make-temp-file "duckdb-insert-")))
              (with-current-buffer buf
                (write-region (point-min) (point-max) temp-file))
              (unwind-protect
                  (if exists
                      (duckdb-execute conn-ptr (format "COPY %s FROM '%s' (AUTO_DETECT TRUE)" table-name temp-file))
                    (duckdb-execute conn-ptr (format "CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')" table-name temp-file)))
                (delete-file temp-file)))))
      ;; If an error occurred and we just opened the connection, we should probably close it
      ;; but if we are returning it, we keep it open.
      )
    (when (called-interactively-p 'any)
      ;; If we just opened a new database, or have the path, open a browser
      (let ((path (if (stringp db-or-path) db-or-path duckdb--db-path))
            (db (or db-ptr duckdb--db-ptr)))
        (if db
            (let* ((new-conn (duckdb-connect db))
                   (buf (get-buffer-create (format "*DuckDB: %s*" (or path "memory")))))
              (with-current-buffer buf
                (duckdb-browse-mode)
                (setq-local duckdb-current-connection new-conn)
                (setq-local duckdb--db-ptr db)
                (setq-local duckdb--db-path path)
                (duckdb-browse-refresh))
              (pop-to-buffer buf))
          (when (stringp path)
            (duckdb-mode-open-file path)))))
    conn-ptr))

;;; Interactive Querying

(defcustom duckdb-query-limit 1000
  "Default limit for queries from the browser."
  :type 'integer
  :group 'duckdb)

(defvar-local duckdb--query-sql nil)
(defvar-local duckdb--query-offset 0)
(defvar-local duckdb--query-window-config nil)
(defvar-local duckdb--query-edit-buffer nil)

(define-derived-mode duckdb-query-results-mode duckdb-edit-mode "DuckDB-Results"
  "Major mode for displaying DuckDB query results from the browser."
  (define-key duckdb-query-results-mode-map (kbd "q") #'duckdb-query-results-quit)
  (define-key duckdb-query-results-mode-map (kbd "ESC") #'duckdb-query-results-quit)
  (define-key duckdb-query-results-mode-map (kbd "e") #'duckdb-query-results-edit)
  (define-key duckdb-query-results-mode-map (kbd "m") #'duckdb-query-results-fetch-more))

(define-derived-mode duckdb-query-edit-mode duckdb-sql-mode "DuckDB-Query-Edit"
  "Major mode for editing DuckDB queries."
  (define-key duckdb-query-edit-mode-map (kbd "C-c C-c") #'duckdb-query-edit-run))

(defun duckdb-browse-query ()
  "Prompt for a SQL query and display results."
  (interactive)
  (unless duckdb-current-connection
    (error "No active DuckDB connection"))
  (let ((buf (get-buffer-create (format "*DuckDB Query: %s*" (or duckdb--db-path "memory"))))
        (conn duckdb-current-connection)
        (db-ptr duckdb--db-ptr)
        (db-path duckdb--db-path)
        (win-config (current-window-configuration)))
    (with-current-buffer buf
      (duckdb-query-edit-mode)
      (setq-local duckdb-current-connection conn)
      (setq-local duckdb--db-ptr db-ptr)
      (setq-local duckdb--db-path db-path)
      (setq-local duckdb--query-window-config win-config)
      (erase-buffer)
      (insert "-- Enter SQL query here (SELECT, DESCRIBE, EXPLAIN or PRAGMA only)\n")
      (insert "SELECT * FROM "))
    (pop-to-buffer buf)))

(defun duckdb-query-edit-run ()
  "Run the query in the current buffer."
  (interactive)
  (let ((sql (buffer-substring-no-properties (point-min) (point-max)))
        (conn duckdb-current-connection)
        (db-ptr duckdb--db-ptr)
        (db-path duckdb--db-path)
        (win-config duckdb--query-window-config)
        (edit-buf (current-buffer)))
    ;; Check query type
    (let ((type (duckdb-query-type conn sql)))
      (unless (member type '(SELECT DESCRIBE EXPLAIN PRAGMA))
        (error "Only SELECT, DESCRIBE, EXPLAIN and PRAGMA statements are allowed (got %s)" type)))
    
    (duckdb--query-execute conn sql 0 win-config edit-buf db-ptr db-path)))

(defun duckdb--query-execute (conn sql offset win-config edit-buf db-ptr db-path)
  "Execute SQL and display results."
  (let* ((limited-sql (format "%s LIMIT %d OFFSET %d" sql duckdb-query-limit offset))
         (results (duckdb-select-columns conn limited-sql))
         (data-plist (plist-get results :data))
         (types-plist (plist-get results :types))
         (keys (cl-loop for (k v) on data-plist by 'cddr collect k))
         (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
         (data (cl-loop for (k v) on data-plist by 'cddr collect v))
         (types (cl-loop for (k v) on types-plist by 'cddr collect v))
         (num-rows (if data (length (car data)) 0))
         (rows (cl-loop for r from 0 to (1- num-rows)
                        collect (cl-loop for col-vec in data
                                         for type in types
                                         collect (duckdb--format-value (aref col-vec r) type)))))
    (duckdb--query-render-results columns rows sql offset win-config edit-buf conn db-ptr db-path)))

(defun duckdb--query-render-results (columns rows sql offset win-config edit-buf conn db-ptr db-path)
  "Render query results."
  (let ((buf (get-buffer-create "*DuckDB Query Results*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (duckdb-query-results-mode)
        (setq-local duckdb-current-connection conn)
        (setq-local duckdb--db-ptr db-ptr)
        (setq-local duckdb--db-path db-path)
        (setq-local duckdb--query-sql sql)
        (setq-local duckdb--query-offset offset)
        (setq-local duckdb--query-window-config win-config)
        (setq-local duckdb--query-edit-buffer edit-buf)
        
        (if (and (fboundp 'vtable-insert) (featurep 'vtable))
            (vtable-insert :columns columns
                           :objects (mapcar (lambda (row)
                                              (mapcar (lambda (val)
                                                        (replace-regexp-in-string "\n" " " (format "%s" val)))
                                                      row))
                                            rows)
                           :separator " | ")
          ;; Fallback to tabulated-list-mode
          (setq tabulated-list-format
                (vconcat (mapcar (lambda (col) (list col 20 t)) columns)))
          (setq tabulated-list-entries
                (cl-loop for row in rows
                         for i from 0
                         collect (list (+ offset i) (vconcat (mapcar (lambda (val)
                                                                       (replace-regexp-in-string "\n" " " (format "%s" val)))
                                                                     row)))))
          (tabulated-list-init-header)
          (tabulated-list-print t))
        
        (let ((start (point)))
          (insert "\n")
          (insert (propertize "[Fetch More (m)]" 'face 'link 'help-echo "Click or press 'm' to fetch more rows" 'mouse-face 'highlight 'duckdb-action 'fetch-more))
          (insert "  ")
          (insert (propertize "[Edit Query (e)]" 'face 'link 'help-echo "Click or press 'e' to edit query" 'mouse-face 'highlight 'duckdb-action 'edit-query))
          (insert "  ")
          (insert (propertize "[Quit (q)]" 'face 'link 'help-echo "Click or press 'q' to close results" 'mouse-face 'highlight 'duckdb-action 'quit))
          (insert "\n\n")
          (insert "-- Query: " sql "\n")
          (insert "-- Showing rows " (number-to-string offset) " to " (number-to-string (+ offset (length rows))) "\n")
          (add-text-properties start (point) '(read-only t)))))
    
    (let ((win (display-buffer buf '(display-buffer-below-selected))))
      (select-window win))))

(defun duckdb-query-results-quit ()
  "Quit the results buffer and restore window configuration."
  (interactive)
  (let ((win-config duckdb--query-window-config)
        (edit-buf duckdb--query-edit-buffer))
    (kill-buffer (current-buffer))
    (when (buffer-live-p edit-buf)
      (kill-buffer edit-buf))
    (when win-config
      (set-window-configuration win-config))))

(defun duckdb-query-results-edit ()
  "Return to the query edit buffer."
  (interactive)
  (let ((edit-buf duckdb--query-edit-buffer))
    (if (buffer-live-p edit-buf)
        (pop-to-buffer edit-buf)
      (message "Edit buffer is gone"))))

(defun duckdb-query-results-fetch-more ()
  "Fetch more rows for the current query."
  (interactive)
  (let ((sql duckdb--query-sql)
        (offset (+ duckdb--query-offset duckdb-query-limit))
        (conn duckdb-current-connection)
        (db-ptr duckdb--db-ptr)
        (db-path duckdb--db-path)
        (win-config duckdb--query-window-config)
        (edit-buf duckdb--query-edit-buffer))
    (duckdb--query-execute conn sql offset win-config edit-buf db-ptr db-path)))

(provide 'duckdb)
;;; duckdb.el ends here
