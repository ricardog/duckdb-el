;;; duckdb-benchmark.el --- Benchmark for emacs-duckdb -*- lexical-binding: t; -*-

(require 'duckdb)
(require 'cl-lib)

(defun duckdb-benchmark-blobs (num-rows blob-size)
  "Benchmark inserting and selecting NUM-ROWS of blobs of BLOB-SIZE."
  (message "Benchmarking %d rows of %d bytes each..." num-rows blob-size)
  (with-duckdb conn ":memory:"
    (duckdb-execute conn "CREATE TABLE benchmark (id INTEGER, data BLOB);")
    
    ;; Generation
    (let ((blobs (cl-loop repeat num-rows
                          collect (let ((str (make-string blob-size 0 nil)))
                                    (dotimes (i blob-size) (aset str i (random 256)))
                                    str))))
      
      ;; Insert benchmark
      (let ((start-time (float-time)))
        (cl-loop for i from 1 to num-rows
                 for blob in blobs
                 do (duckdb-execute conn "INSERT INTO benchmark VALUES (?, ?);" (list i (duckdb-blob blob))))
        (let ((elapsed (- (float-time) start-time)))
          (message "  Insert: %.4f seconds (%.2f MB/s)" 
                   elapsed 
                   (/ (* num-rows blob-size) (max 0.0001 (* elapsed 1024.0 1024.0))))))

      ;; Select benchmark
      (let ((start-time (float-time)))
        (let ((results (duckdb-select conn "SELECT data FROM benchmark;")))
          (let ((elapsed (- (float-time) start-time)))
            (message "  Select: %.4f seconds (%.2f MB/s)" 
                     elapsed 
                     (/ (* num-rows blob-size) (max 0.0001 (* elapsed 1024.0 1024.0))))))))))

(defun run-duckdb-benchmarks ()
  "Run a set of benchmarks."
  (duckdb-benchmark-blobs 100 1024)       ; 100 KB total
  (duckdb-benchmark-blobs 100 10240)      ; 1 MB total
  (duckdb-benchmark-blobs 100 102400)     ; 10 MB total
  (duckdb-benchmark-blobs 10 1048576))    ; 10 MB total (fewer rows, larger blobs)

(run-duckdb-benchmarks)
