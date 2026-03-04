# emacs-duckdb

An Emacs dynamic module for DuckDB.

## Features
- **Performance:** Columnar C API for fast data processing.
- **Stability:** Zero memory leaks with `user_ptr` finalizers.
- **Ergonomics:** Native Elisp API, `with-duckdb` macros, and interactive browser.
- **Asynchronous:** Support for non-blocking queries with callbacks.
- **Interactive:** Built-in table browser with data preview.

## Installation

### Prerequisites
- **DuckDB:** Ensure you have the DuckDB shared library installed.
  - **macOS:** `brew install duckdb`
  - **Linux:** Install via your package manager or download from [duckdb.org](https://duckdb.org/).
- **Emacs:** Emacs 28 or newer with dynamic module support.

### Building
Clone the repository and run `make`:

```bash
git clone https://github.com/youruser/emacs-duckdb
cd emacs-duckdb
make
```

This will produce `duckdb-core.so`.

## Usage

### Basic Example
```elisp
(require 'duckdb)

;; Simple query
(with-duckdb conn ":memory:"
  (duckdb-execute conn "CREATE TABLE test (id INTEGER, name VARCHAR);")
  (duckdb-execute conn "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob');")
  (duckdb-select conn "SELECT * FROM test;"))
;; => ((1 "Alice") (2 "Bob"))
```

### Parameterized Queries
Safely bind values to your SQL queries:
```elisp
(with-duckdb conn ":memory:"
  (duckdb-execute conn "INSERT INTO users (name, age) VALUES (?, ?);" '("Alice" 30)))
```

### Columnar Results
For large datasets, use `duckdb-select-columns` to get results in a high-performance columnar format (vectors):
```elisp
(let* ((results (duckdb-select-columns conn "SELECT * FROM large_table;"))
       (data (plist-get results :data))
       (types (plist-get results :types)))
  (message "Column names: %s" (cl-loop for (k v) on data by 'cddr collect k))
  (aref (plist-get data :some_column) 0)) ;; Access first row of 'some_column'
```

### Asynchronous Queries
Run long-running queries without freezing Emacs:
```elisp
(duckdb-select-async conn "SELECT count(*) FROM big_data;"
                     (lambda (results)
                       (message "Query finished! Count: %s" (caar results))))
```

### Interactive Browser
Open and browse a DuckDB database file interactively:
- `M-x duckdb-mode-open-file`: Select a database file to browse its tables.
- In the browse buffer:
  - `RET`: Toggle data preview for the table at point.
  - `g`: Refresh the table list.

## Development
Run tests with:
```bash
make test
```
To run tests with Address Sanitizer (requires `gcc` and `libasan`):
```bash
make asan-test
```

## License
GPL compatible.
