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

(provide 'duckdb-tests)
;;; duckdb-tests.el ends here
