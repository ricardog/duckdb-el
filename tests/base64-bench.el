;;; base64-bench.el --- Benchmark for base64 optimized primitives -*- lexical-binding: t; -*-

(require 'duckdb)
(require 'cl-lib)

(defun bench-base64 (size)
  (let ((data (make-string size 0 nil)))
    (dotimes (i size) (aset data i (random 256)))
    (message "Benchmarking size: %d bytes" size)
    
    ;; Built-in encode
    (let ((start (float-time)))
      (dotimes (_ 10) (base64-encode-string data t))
      (let ((elapsed (/ (- (float-time) start) 10.0)))
        (message "  Emacs built-in encode: %.6f s (%.2f MB/s)" elapsed (/ size (max 0.000001 (* elapsed 1024.0 1024.0))))))
    
    ;; Optimized encode
    (let ((start (float-time)))
      (dotimes (_ 10) (duckdb-base64-encode data))
      (let ((elapsed (/ (- (float-time) start) 10.0)))
        (message "  duckdb-core optimized encode:  %.6f s (%.2f MB/s)" elapsed (/ size (max 0.000001 (* elapsed 1024.0 1024.0))))))

    (let ((encoded (base64-encode-string data t)))
      ;; Built-in decode
      (let ((start (float-time)))
        (dotimes (_ 10) (base64-decode-string encoded))
        (let ((elapsed (/ (- (float-time) start) 10.0)))
          (message "  Emacs built-in decode: %.6f s (%.2f MB/s)" elapsed (/ size (max 0.000001 (* elapsed 1024.0 1024.0))))))

      ;; Optimized decode
      (let ((start (float-time)))
        (dotimes (_ 10) (duckdb-base64-decode encoded))
        (let ((elapsed (/ (- (float-time) start) 10.0)))
          (message "  duckdb-core optimized decode:  %.6f s (%.2f MB/s)" elapsed (/ size (max 0.000001 (* elapsed 1024.0 1024.0)))))))))

(bench-base64 102400) ; 100 KB
(bench-base64 1048576) ; 1 MB
(bench-base64 10485760) ; 10 MB
