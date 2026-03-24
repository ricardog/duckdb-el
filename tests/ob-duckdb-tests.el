;;; ob-duckdb-tests.el --- Tests for Org-babel DuckDB support -*- lexical-binding: t; -*-

(require 'ert)
(require 'ob-duckdb)

(defmacro with-ob-duckdb-test-setup (&rest body)
  "Setup environment for ob-duckdb tests."
  `(let ((org-babel-duckdb-sessions (make-hash-table :test 'equal)))
     (add-to-list 'load-path (expand-file-name "build" default-directory))
     ,@body))

(ert-deftest ob-duckdb-simple-select-test ()
  "Test simple SELECT execution."
  (with-ob-duckdb-test-setup
   (let ((results (org-babel-execute:duckdb "SELECT 1 as a, 2 as b" '((:colnames . "yes")))))
     (should (equal results '(("a" "b") hline (1 2)))))
   (let ((results (org-babel-execute:duckdb "SELECT 1 as a, 2 as b" '((:colnames . "no")))))
     (should (equal results '((1 2)))))))

(ert-deftest ob-duckdb-variable-binding-test ()
  "Test variable binding for various types."
  (with-ob-duckdb-test-setup
   ;; Integer
   (let ((results (org-babel-execute:duckdb "SELECT * FROM myvar" '((:var . (myvar . 42)) (:colnames . "no")))))
     (should (equal results '((42)))))
   ;; String
   (let ((results (org-babel-execute:duckdb "SELECT * FROM mystr" '((:var . (mystr . "hello")) (:colnames . "no")))))
     (should (equal results '(("hello")))))
   ;; Table
   (let* ((table '((1 "alice") (2 "bob")))
          (results (org-babel-execute:duckdb "SELECT * FROM mytable" `((:var . (mytable . ,table)) (:colnames . "no")))))
     (should (equal results '((1 "alice") (2 "bob")))))))

(ert-deftest ob-duckdb-session-test ()
  "Test session persistence."
  (with-ob-duckdb-test-setup
   (org-babel-execute:duckdb "CREATE TABLE session_test (val TEXT)" '((:session . "s1")))
   (org-babel-execute:duckdb "INSERT INTO session_test VALUES ('persisted')" '((:session . "s1")))
   (let ((results (org-babel-execute:duckdb "SELECT * FROM session_test" '((:session . "s1") (:colnames . "no")))))
     (should (equal results '(("persisted")))))
   ;; Different session should not see the table
   (should-error (org-babel-execute:duckdb "SELECT * FROM session_test" '((:session . "s2"))))))

(ert-deftest ob-duckdb-kill-session-test ()
  "Test killing a session."
  (with-ob-duckdb-test-setup
   (org-babel-execute:duckdb "SELECT 1" '((:session . "to-kill")))
   (should (gethash "to-kill" org-babel-duckdb-sessions))
   (org-babel-duckdb-kill-session "to-kill")
   (should-not (gethash "to-kill" org-babel-duckdb-sessions))))

(ert-deftest ob-duckdb-cleanup-test ()
  "Test that temporary connections and databases are closed."
  (with-ob-duckdb-test-setup
   (let ((disconnect-called 0)
         (close-called 0))
     (cl-letf (((symbol-function 'duckdb-disconnect) 
                (lambda (_) (setq disconnect-called (1+ disconnect-called))))
               ((symbol-function 'duckdb-close) 
                (lambda (_) (setq close-called (1+ close-called)))))
       (org-babel-execute:duckdb "SELECT 1" '((:db . ":memory:"))))
     (should (= disconnect-called 1))
     (should (= close-called 1)))))

(ert-deftest ob-duckdb-table-cleanup-test ()
  "Test that table variables persist after temp file deletion."
  (with-ob-duckdb-test-setup
   (let* ((table '((1 "a")))
          (results (org-babel-execute:duckdb "SELECT * FROM mytable" `((:var . (mytable . ,table)) (:colnames . "no")))))
     (should (equal results '((1 "a")))))))

(ert-deftest ob-duckdb-error-handling-test ()
  "Test error handling for invalid SQL."
  (with-ob-duckdb-test-setup
   (should-error (org-babel-execute:duckdb "SELECT * FROM non_existent_table" nil)
                 :type 'error)))

(provide 'ob-duckdb-tests)
