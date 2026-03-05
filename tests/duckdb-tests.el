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

(ert-deftest duckdb-connect-test ()
  "Test connecting to a database."
  (let* ((db (duckdb-open ":memory:"))
         (conn (duckdb-connect db)))
    (should (user-ptrp conn))
    (duckdb-disconnect conn)))

(ert-deftest duckdb-execute-test ()
  "Test executing a simple SQL query."
  (with-duckdb conn ":memory:"
    (let ((rows (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")))
      (should (equal rows 0)))
    (let ((rows (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');")))
      (should (equal rows 2)))))

(ert-deftest duckdb-select-test ()
  "Test selecting data from a table."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN);")
    (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice', 10.5, true), (2, 'Bob', 20.0, false), (3, NULL, NULL, NULL);")
    (let ((results (duckdb-select conn "SELECT * FROM test ORDER BY id;")))
      (should (equal results '((1 "Alice" 10.5 t)
                               (2 "Bob" 20.0 nil)
                               (3 nil nil nil)))))))

(ert-deftest duckdb-null-symbol-custom-test ()
  "Test that duckdb-null-symbol can be customized."
  (let ((duckdb-null-symbol :null))
    (with-duckdb conn ":memory:"
      (let ((results (duckdb-select conn "SELECT NULL;")))
        (should (equal results '((:null))))))))

(ert-deftest duckdb-execute-error ()
  "Test executing an invalid SQL query should signal duckdb-error."
  (with-duckdb conn ":memory:"
    (should-error (duckdb-execute conn "SELECT * FROM non_existent_table;")
                  :type 'duckdb-error)))

(ert-deftest duckdb-select-columns-test ()
  "Test selecting data in columnar format."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR, price DOUBLE);")
    (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.0), (3, NULL, NULL);")
    (let* ((results (duckdb-select-columns conn "SELECT * FROM test ORDER BY id;"))
           (data (plist-get results :data))
           (types (plist-get results :types)))
      (should (equal (plist-get data :id) [1 2 3]))
      (should (equal (plist-get data :name) ["Alice" "Bob" nil]))
      (should (equal (plist-get data :price) [10.5 20.0 nil]))
      (should (equal (plist-get types :id) "INTEGER"))
      (should (equal (plist-get types :name) "VARCHAR")))))

(ert-deftest duckdb-select-columns-null-test ()
  "Test selecting data in columnar format with custom null symbol."
  (let ((duckdb-null-symbol :null))
    (with-duckdb conn ":memory:"
      (duckdb-execute conn "CREATE TABLE test (val INTEGER);")
      (duckdb-execute conn "INSERT INTO test VALUES (NULL);")
      (let* ((results (duckdb-select-columns conn "SELECT * FROM test;"))
             (data (plist-get results :data)))
        (should (equal (plist-get data :val) [:null]))))))

(ert-deftest duckdb-parameterized-execute-test ()
  "Test duckdb-execute with parameters."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")
    (duckdb-execute conn "INSERT INTO test VALUES (?, ?);" '(1 "Alice"))
    (duckdb-execute conn "INSERT INTO test VALUES (?, ?);" '(2 "Bob"))
    (let ((results (duckdb-select conn "SELECT * FROM test ORDER BY id;")))
      (should (equal results '((1 "Alice") (2 "Bob")))))))

(ert-deftest duckdb-parameterized-select-test ()
  "Test duckdb-select with parameters."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")
    (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');")
    (let ((results (duckdb-select conn "SELECT name FROM test WHERE id = ?;" '(1))))
      (should (equal results '(("Alice")))))))

(ert-deftest duckdb-prepare-bind-step-test ()
  "Test manual prepare, bind, and step."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")
    (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');")
    (let ((stmt (duckdb-prepare conn "SELECT * FROM test WHERE id > ? ORDER BY id;")))
      (should (user-ptrp stmt))
      (duckdb-bind stmt '(1))
      (should (equal (duckdb-step stmt) '(2 "Bob")))
      (should (equal (duckdb-step stmt) '(3 "Charlie")))
      (should (equal (duckdb-step stmt) nil)))))

(ert-deftest duckdb-select-async-test ()
  "Test selecting data asynchronously."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")
    (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');")
    (let ((done nil)
          (results nil))
      (duckdb-select-async conn "SELECT * FROM test ORDER BY id;"
                           (lambda (res)
                             (setq results res)
                             (setq done t)))
      (let ((timeout 50)) ; 5 seconds total
        (while (and (not done) (> timeout 0))
          (read-event nil nil 0.1)
          (setq timeout (1- timeout))))
      (should done)
      (should (equal results '((1 "Alice") (2 "Bob")))))))

(ert-deftest duckdb-select-async-error-test ()
  "Test selecting data asynchronously with error."
  (with-duckdb conn ":memory:"
    (let ((done nil)
          (error-caught nil))
      (should-error (duckdb-select-async conn "SELECT * FROM non_existent_table;"
                                         (lambda (_) (setq done t)))
                    :type 'duckdb-error))))

(ert-deftest duckdb-get-tables-columns-test ()
  "Test duckdb-get-tables and duckdb-get-columns."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE users (id INTEGER, name VARCHAR);")
    (duckdb-execute conn "CREATE TABLE orders (id INTEGER, user_id INTEGER, total DOUBLE);")
    (let ((tables (duckdb-get-tables conn)))
      (should (member "users" tables))
      (should (member "orders" tables)))
    (let ((columns (duckdb-get-columns conn "users")))
      (should (member "id" columns))
      (should (member "name" columns)))))

(ert-deftest duckdb-insert-buffer-test ()
  "Test duckdb-insert-buffer with connection and path."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE users (id INTEGER, name VARCHAR);")
    (with-temp-buffer
      (insert "1,Alice\n2,Bob\n3,Charlie\n")
      (duckdb-insert-buffer conn "users"))
    (let ((results (duckdb-select conn "SELECT * FROM users ORDER BY id;")))
      (should (equal results '((1 "Alice") (2 "Bob") (3 "Charlie"))))))
  ;; Test with path
  (let ((tmp-db (make-temp-file "duckdb-test-")))
    (delete-file tmp-db) ;; DuckDB fails to open an empty file as a DB
    (unwind-protect
        (progn
          (with-duckdb conn tmp-db
            (duckdb-execute conn "CREATE TABLE users (id INTEGER, name VARCHAR);"))
          (with-temp-buffer
            (insert "4,David\n")
            (duckdb-insert-buffer tmp-db "users"))
          (with-duckdb conn tmp-db
            (let ((results (duckdb-select conn "SELECT * FROM users;")))
              (should (equal results '((4 "David")))))))
      (when (file-exists-p tmp-db) (delete-file tmp-db)))))

(ert-deftest duckdb-query-and-display-test ()
  "Test duckdb-query-and-display with connection and path."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")
    (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');")
    (duckdb-query-and-display conn "SELECT * FROM test ORDER BY id;"))
  ;; Test with path
  (duckdb-query-and-display ":memory:" "SELECT 1 as val;"))

(ert-deftest duckdb-select-timestamp-blob-test ()
  "Test selecting TIMESTAMP and BLOB types."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (ts TIMESTAMP, data BLOB);")
    (duckdb-execute conn "INSERT INTO test VALUES ('2023-01-01 12:00:00', '\\x01\\x02\\x03'::BLOB);")
    (let ((results (duckdb-select conn "SELECT * FROM test;")))
      (should (equal (length results) 1))
      (should (integerp (caar results)))
      (should (stringp (cadar results))))
    ;; Test columnar as well
    (let* ((results (duckdb-select-columns conn "SELECT * FROM test;"))
           (data (plist-get results :data))
           (types (plist-get results :types)))
      (should (integerp (aref (plist-get data :ts) 0)))
      (should (equal (plist-get types :ts) "TIMESTAMP"))
      (should (stringp (aref (plist-get data :data) 0))))))

(ert-deftest duckdb-select-varchar-invalid-utf8-test ()
  "Test selecting VARCHAR with invalid UTF-8 data."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (data VARCHAR);")
    ;; Use from_hex to get real binary data into a VARCHAR
    (duckdb-execute conn "INSERT INTO test SELECT from_hex('07832F')::VARCHAR;")
    
    (let* ((results (duckdb-select-columns conn "SELECT * FROM test;"))
           (data (plist-get results :data))
           (val (aref (plist-get data :data) 0)))
      (should (stringp val))
      ;; It seems in this environment DuckDB returns the escaped hex string "\x07\x83/"
      (should (member (length val) '(3 9)))
      (if (= (length val) 9)
          (should (string-prefix-p "\\x" val))
        (should (not (multibyte-string-p val)))))))

(ert-deftest duckdb-format-value-timestamp-test ()
  "Test duckdb--format-value with TIMESTAMP."
  ;; 1672574400000000 is 2023-01-01 12:00:00 UTC
  (should (string-prefix-p "2023-01-01" (duckdb--format-value 1672574400000000 "TIMESTAMP"))))

(ert-deftest duckdb-browse-format-preview-binary-test ()
  "Test duckdb--format-preview-data with binary data."
  (let* ((columns '("id" "data"))
         ;; 0x83 is non-UTF-8
         (binary (unibyte-string #x07 #x83 #x2F))
         (rows (list (list "1" (duckdb--format-value binary "BLOB"))))
         (formatted (duckdb--format-preview-data columns rows)))
    (should (string-match "id  data" formatted))
    (should (string-match "1   \"" formatted))))

(ert-deftest duckdb-browse-get-tables-with-counts-test ()
  "Test duckdb--get-tables-with-counts."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE t1 (id int)")
    (duckdb-execute conn "INSERT INTO t1 VALUES (1), (2), (3)")
    (duckdb-execute conn "CREATE TABLE t2 (name varchar)")
    (duckdb-execute conn "INSERT INTO t2 VALUES ('a')")
    
    (let ((tables (duckdb--get-tables-with-counts conn)))
      (should (equal (length tables) 2))
      (should (equal (nth 0 tables) '("t1" 3)))
      (should (equal (nth 1 tables) '("t2" 1))))))

(ert-deftest duckdb-browse-tables-sorting-test ()
  "Test that tables are sorted by name in DuckDB Browse mode."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE zzz (id int)")
    (duckdb-execute conn "CREATE TABLE aaa (id int)")
    (duckdb-execute conn "CREATE TABLE mmm (id int)")
    
    (let ((tables (duckdb--get-tables-with-counts conn)))
      (should (equal (mapcar #'car tables) '("aaa" "mmm" "zzz"))))))

(ert-deftest duckdb-browse-get-table-preview-test ()
  "Test duckdb--get-table-preview."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE t1 (id int, val varchar)")
    (duckdb-execute conn "INSERT INTO t1 VALUES (1, 'apple'), (2, 'banana')")
    
    (let ((preview (duckdb--get-table-preview conn "t1")))
      (should (string-match "id" preview))
      (should (string-match "val" preview))
      (should (string-match "1" preview))
      (should (string-match "apple" preview))
      (should (string-match "2" preview))
      (should (string-match "banana" preview)))))

(ert-deftest duckdb-browse-format-preview-data-test ()
  "Test duckdb--format-preview-data."
  (let* ((columns '("id" "name"))
         (rows (list (list "1" (duckdb--format-value "Alice" "VARCHAR"))
                     (list "2" (duckdb--format-value "Bob" "VARCHAR"))))
         (formatted (duckdb--format-preview-data columns rows)))
    (should (string-match "id  name" formatted))
    (should (string-match "1   \"Alice\"" formatted))
    (should (string-match "2   \"Bob\"" formatted))))

(ert-deftest duckdb-blob-bind-test ()
  "Test binding BLOB parameters."
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE test (data BLOB);")
    (let ((blob-data (unibyte-string #x00 #xFF #x07 #x83)))
      (duckdb-execute conn "INSERT INTO test VALUES (?);" (list (duckdb-blob blob-data)))
      (let* ((results (duckdb-select conn "SELECT data FROM test;"))
             (val (caar results)))
        (should (stringp val))
        (should (not (multibyte-string-p val)))
        (should (equal (length val) 4))
        (should (equal (aref val 0) #x00))
        (should (equal (aref val 1) #xFF))
        (should (equal (aref val 2) #x07))
        (should (equal (aref val 3) #x83))))))

(ert-deftest duckdb-browse-format-preview-newline-test ()
  "Test duckdb--format-preview-data with newlines in data."
  (let* ((columns '("id" "desc"))
         (rows (list (list "1" (duckdb--format-value "Line 1\nLine 2" "VARCHAR"))))
         (formatted (duckdb--format-preview-data columns rows)))
    (should (string-match "Line 1 Line 2" formatted))))

(ert-deftest duckdb-base64-test ()
  "Test duckdb-base64-encode and duckdb-base64-decode."
  (let* ((original "Hello, DuckDB!")
         (encoded (duckdb-base64-encode original))
         (decoded (duckdb-base64-decode encoded)))
    (should (string= encoded "SGVsbG8sIER1Y2tEQiE="))
    (should (string= decoded original)))
  ;; Test with binary data
  (let* ((binary (unibyte-string #x00 #xFF #x07 #x83))
         (encoded (duckdb-base64-encode binary))
         (decoded (duckdb-base64-decode encoded)))
    (should (string= encoded "AP8Hgw=="))
    (should (equal decoded binary))))

(ert-deftest duckdb-base64-large-test ()
  "Test duckdb-base64-encode and duckdb-base64-decode with large data (exercises SIMD)."
  (let* ((len 1024)
         (original (apply #'unibyte-string (cl-loop repeat len collect (random 256))))
         (encoded (duckdb-base64-encode original))
         (decoded (duckdb-base64-decode encoded)))
    (should (equal (length original) len))
    (should (equal decoded original))))

(ert-deftest duckdb-mode-open-file-test ()
  "Test duckdb-mode-open-file."
  (let ((buf (duckdb-mode-open-file ":memory:")))
    (unwind-protect
        (with-current-buffer buf
          (should (eq major-mode 'duckdb-browse-mode))
          (should (local-variable-p 'duckdb-current-connection))
          (should (local-variable-p 'duckdb--db-ptr))
          (should (string-match "Table Name" (buffer-string))))
      (kill-buffer buf))))

(ert-deftest duckdb-browse-toggle-columns-test ()
  "Test duckdb-browse-toggle-columns."
  (let ((buf (duckdb-mode-open-file ":memory:")))
    (unwind-protect
        (with-current-buffer buf
          (duckdb-execute duckdb-current-connection "CREATE TABLE test (id INTEGER, name VARCHAR);")
          (duckdb-browse-refresh)
          ;; Move to the "test" table line
          (goto-char (point-min))
          (search-forward "test")
          (forward-char -1)
          ;; Toggle columns
          (duckdb-browse-toggle-columns)
          (should (string= duckdb--expanded-table "test"))
          (should (eq (overlay-get duckdb--expanded-overlay 'duckdb-type) 'columns))
          (should (string-match "Columns:" (buffer-string)))
          (should (string-match "id" (buffer-string)))
          (should (string-match "INTEGER" (buffer-string)))
          ;; Toggle again to collapse
          (duckdb-browse-toggle-columns)
          (should-not duckdb--expanded-table)
          (should-not duckdb--expanded-overlay)
          ;; Toggle data
          (duckdb-browse-toggle-table)
          (should (string= duckdb--expanded-table "test"))
          (should (eq (overlay-get duckdb--expanded-overlay 'duckdb-type) 'data))
          ;; Toggle columns while data is shown
          (duckdb-browse-toggle-columns)
          (should (string= duckdb--expanded-table "test"))
          (should (eq (overlay-get duckdb--expanded-overlay 'duckdb-type) 'columns)))
      (kill-buffer buf))))

(ert-deftest duckdb-insert-iris-test ()
  "Test inserting iris.csv into a table and querying it."
  (let ((iris-file (expand-file-name "iris.csv" (or (and load-file-name (file-name-directory load-file-name))
                                                   (and buffer-file-name (file-name-directory buffer-file-name))
                                                   default-directory))))
    (with-duckdb conn ":memory:"
      (let ((buf (find-file-noselect iris-file)))
        (unwind-protect
            (duckdb-insert-buffer conn "iris" buf)
          (kill-buffer buf)))
      (let ((results (duckdb-select conn "SELECT count(*), avg(sepal_length) FROM iris;")))
        (should (equal (caar results) 150))
        (should (floatp (cadar results)))
        (should (> (cadar results) 5.8))
        (should (< (cadar results) 5.9))))))

(provide 'duckdb-tests)
;;; duckdb-tests.el ends here
