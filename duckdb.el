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
                                 help-echo "RET to toggle rows")))))
    (setq duckdb--expanded-table nil)
    (setq duckdb--expanded-overlay nil)
    (goto-char (min pos (point-max)))
    (if (< (point) (point-min)) (goto-char (point-min)))))

(defun duckdb--get-tables-with-counts (conn)
  "Get all tables in CONN and their row counts."
  (let ((tables (duckdb-select conn "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")))
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
  "Toggle the display of the table at point."
  (interactive)
  (let ((table (get-text-property (point) 'duckdb-table-name)))
    (if (not table)
        (message "No table at point")
      (if (string= table duckdb--expanded-table)
          (duckdb--collapse-table)
        (duckdb--collapse-table) ; Collapse existing if any
        (duckdb--expand-table table)))))

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
        (overlay-put duckdb--expanded-overlay 'duckdb-expanded t)))))

(defun duckdb--get-table-preview (conn table)
  "Get first 100 rows of TABLE and format them."
  (let* ((sql (format "SELECT * FROM %s LIMIT 100" table))
         (results (duckdb-select-columns conn sql))
         (keys (cl-loop for (k v) on results by 'cddr collect k))
         (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
         (data (cl-loop for (k v) on results by 'cddr collect v))
         (num-rows (if data (length (car data)) 0))
         (rows (cl-loop for r from 0 to (1- num-rows)
                        collect (mapcar (lambda (col-vec) (aref col-vec r)) data))))
    (duckdb--format-preview-data columns rows)))

(defun duckdb--format-preview-data (columns rows)
  "Format COLUMNS and ROWS for preview display."
  (if (null columns)
      "  (No columns)\n"
    (let* ((widths (mapcar #'string-width columns))
           (val-to-string (lambda (v)
                            (let ((s (cond
                                      ((null v) "NULL")
                                      ((and (stringp v) (not (multibyte-string-p v)))
                                       (prin1-to-string v))
                                      (t (format "%s" v)))))
                              (replace-regexp-in-string "\n" " " s))))
           (pad (lambda (s w)
                  (let ((sw (string-width s)))
                    (if (>= sw w) s
                      (concat s (make-string (- w sw) ?\s))))))
           (widths (cl-loop for row in rows
                            do (setq widths (cl-loop for val in row
                                                     for w in widths
                                                     collect (max w (string-width (funcall val-to-string val)))))
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
                 do (let ((s (funcall val-to-string val)))
                      (setq out (concat out (funcall pad s w) "  "))))
        (setq out (concat out "\n")))
      out)))

(defun duckdb-blob (data)
  "Create a BLOB parameter from DATA (a unibyte string)."
  (list :blob (base64-encode-string data t)))

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
           (keys (cl-loop for (k v) on results by 'cddr collect k))
           (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
           (data (cl-loop for (k v) on results by 'cddr collect v))
           (num-rows (if data (length (car data)) 0))
           (rows (cl-loop for r from 0 to (1- num-rows)
                          collect (mapcar (lambda (col-vec) (aref col-vec r)) data))))
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
                           :objects (mapcar (lambda (row)
                                              (mapcar (lambda (val)
                                                        (replace-regexp-in-string "\n" " " (format "%s" val)))
                                                      row))
                                            rows)
                           :separator " | ")
          ;; Fallback to tabulated-list-mode
          (duckdb-edit-mode)
          (setq tabulated-list-format
                (vconcat (mapcar (lambda (col) (list col 20 t)) columns)))
          (setq tabulated-list-entries
                (cl-loop for row in rows
                         for i from 0
                         collect (list i (vconcat (mapcar (lambda (val)
                                                           (replace-regexp-in-string "\n" " " (format "%s" val)))
                                                         row)))))
          (tabulated-list-print t))))
    (display-buffer buf)))

(defun duckdb--query-and-display-internal (conn-ptr sql &optional params)
  "Internal helper for query and display."
  (let* ((results (duckdb-select-columns conn-ptr sql params))
         (keys (cl-loop for (k v) on results by 'cddr collect k))
         (columns (mapcar (lambda (k) (substring (symbol-name k) 1)) keys))
         (data (cl-loop for (k v) on results by 'cddr collect v))
         (num-rows (if data (length (car data)) 0))
         (rows (cl-loop for r from 0 to (1- num-rows)
                        collect (mapcar (lambda (col-vec) (aref col-vec r)) data))))
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
If BUFFER is nil, use the current buffer."
  (interactive (list (duckdb--get-db-or-path) (read-string "Table name: ")))
  (let* ((buf (or buffer (current-buffer)))
         (file (buffer-file-name buf)))
    (cl-flet ((do-insert (conn)
                (if file
                    (duckdb-execute conn (format "COPY %s FROM '%s' (AUTO_DETECT TRUE)" table-name file))
                  ;; If not a file, write to a temp file
                  (let ((temp-file (make-temp-file "duckdb-insert-")))
                    (with-current-buffer buf
                      (write-region (point-min) (point-max) temp-file))
                    (unwind-protect
                        (duckdb-execute conn (format "COPY %s FROM '%s' (AUTO_DETECT TRUE)" table-name temp-file))
                      (delete-file temp-file))))))
      (if (stringp db-or-path)
          (with-duckdb conn db-or-path (do-insert conn))
        (do-insert db-or-path)))))

(provide 'duckdb)
;;; duckdb.el ends here
