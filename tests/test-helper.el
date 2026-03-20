;;; test-helper.el --- Setup for duckdb-tests -*- lexical-binding: t; -*-

(add-to-list 'load-path (file-name-directory (directory-file-name (file-name-directory (or load-file-name buffer-file-name)))))
(add-to-list 'load-path default-directory)

(require 'duckdb)
(require 'dired-aux)
