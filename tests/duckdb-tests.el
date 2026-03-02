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
    (let ((results (duckdb-select-columns conn "SELECT * FROM test ORDER BY id;")))
      (should (equal (plist-get results :id) [1 2 3]))
      (should (equal (plist-get results :name) ["Alice" "Bob" nil]))
      (should (equal (plist-get results :price) [10.5 20.0 nil])))))

(ert-deftest duckdb-select-columns-null-test ()
  "Test selecting data in columnar format with custom null symbol."
  (let ((duckdb-null-symbol :null))
    (with-duckdb conn ":memory:"
      (duckdb-execute conn "CREATE TABLE test (val INTEGER);")
      (duckdb-execute conn "INSERT INTO test VALUES (NULL);")
      (let ((results (duckdb-select-columns conn "SELECT * FROM test;")))
        (should (equal (plist-get results :val) [:null]))))))

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
      ;; Note: In my implementation, invalid SQL is caught during PREPARE in main thread.
      ;; But let's test a runtime error if possible.
      ;; Actually, I'll test catch-all.
      (should-error (duckdb-select-async conn "SELECT * FROM non_existent_table;"
                                         (lambda (_) (setq done t)))
                    :type 'duckdb-error))))

(provide 'duckdb-tests)
;;; duckdb-tests.el ends here
