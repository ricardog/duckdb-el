# Specification: Emacs-DuckDB Dynamic Module API

**Version:** 1.0.0  
**Status:** Draft  
**Target:** Emacs 28+ (C Module API)  
**Dependencies:** libduckdb (C API)

---

## 1. Overview
The `emacs-duckdb` module provides a high-performance bridge between Emacs and the DuckDB analytical database. It prioritizes **binary compatibility**, **memory safety** via Garbage Collection integration, and **OLAP performance** through columnar data transfer.

---

## 2. Memory & Type Mapping

### 2.1 Opaque Pointers (user_ptr)
To ensure memory safety, the module uses Emacs `user_ptr` objects with finalizers.

| Lisp Object | C Underlying Type | Finalizer Action |
| :--- | :--- | :--- |
| `duckdb-db-ptr` | `duckdb_database` | `duckdb_close()` |
| `duckdb-conn-ptr` | `duckdb_connection` | `duckdb_disconnect()` |
| `duckdb-stmt-ptr` | `duckdb_prepared_statement` | `duckdb_destroy_prepare()` |

### 2.2 Data Conversion

| DuckDB Type | Emacs Lisp Type | Conversion Note |
| :--- | :--- | :--- |
| `BOOLEAN` | `t` / `nil` | |
| `INTEGER` / `BIGINT` | `integer` | Uses `make_integer` |
| `DOUBLE` / `FLOAT` | `float` | |
| `VARCHAR` | `string` | UTF-8 encoded |
| `NULL` | `nil` | Configurable via `duckdb-null-symbol` |
| `LIST` / `STRUCT` | `vector` / `plist` | Nested conversion |

---

## 3. Core Module Primitives (C API)

### 3.1 Connection Management
* `(duckdb-open path)`  
    Opens a database file. `path` can be a string or `:memory:`.  
    **Returns:** `duckdb-db-ptr`.
* `(duckdb-connect db-ptr)`  
    Creates a new connection session from a database pointer.  
    **Returns:** `duckdb-conn-ptr`.
* `(duckdb-disconnect conn-ptr)`  
    Manually closes a connection. Usually handled by GC.

### 3.2 Synchronous Querying
* `(duckdb-execute conn-ptr sql &optional params)`  
    Runs DDL/DML (e.g., CREATE, INSERT).  
    **Returns:** Number of rows affected.
* `(duckdb-select conn-ptr sql &optional params)`  
    Runs a query and returns results as a **List of Lists** (Row-major).  
    *Ideal for: Small result sets, basic Lisp processing.*
* `(duckdb-select-columns conn-ptr sql &optional params)`  
    Runs a query and returns a **Plist of Vectors** (Column-major).  
    *Example:* `(:id [1 2] :name ["Alice" "Bob"])`.  
    *Ideal for: High-performance data processing, vtable integration.*

### 3.3 Prepared Statements
* `(duckdb-prepare conn-ptr sql)`  
    **Returns:** `duckdb-stmt-ptr`.
* `(duckdb-bind stmt-ptr params)`  
    Binds a list of values to the `?` placeholders.
* `(duckdb-step stmt-ptr)`  
    Executes the statement. Returns a single row or `nil`.

---

## 4. Asynchronous Execution (Non-Blocking)

* `(duckdb-select-async conn-ptr sql callback &optional params)`  
    1. Spawns a background worker thread.
    2. Executes the query without blocking the Emacs UI.
    3. Upon completion, schedules the `callback` to run in the main thread with the result set.

---

## 5. High-Level Elisp Wrapper (Proposed)

### 5.1 Macros
```elisp
(defmacro with-duckdb (var path &rest body)
  "Open DuckDB at PATH, bind connection to VAR, and execute BODY."
  (declare (indent 2))
  `(let* ((db (duckdb-open ,path))
          (,var (duckdb-connect db)))
     (unwind-protect
         (progn ,@body)
       (duckdb-disconnect ,var))))
```

### 5.2 Interactive results (The "SQL-Scrubber")

The most common interactive use case is running a query and exploring the data.

* **Feature:** `duckdb-edit-mode`. A major mode (similar to `list-packages` or `tabulated-list-mode`) that renders query results into a buffer.
* **Implementation:** **Pure Elisp.** * The C module provides `duckdb-select` as a list of lists.
* The Elisp code formats these into a `tabulated-list-entries` structure.
* Support a read-only mode where the contents of the table can't be updated.
* *Bonus:* Use `vtable.el` (introduced in Emacs 29) to handle variable-width columns and alignment automatically.


### 5.3 "Insert from Buffer" (The Data Ingestor)

DuckDB is famous for its ability to "query anything." Users should be able to point at a CSV or JSON buffer and "suck" it into a table.

* **Feature:** `duckdb-insert-buffer`.
* **Implementation:** **Hybrid.**
* **Elisp:** Identify the file path of the current buffer or write the buffer to a temp file.
* **Module Primitives:** We need a robust `duckdb-execute` that can handle DuckDB's `COPY ... FROM ...` commands. If the data is only in a string (not a file), we would need a C primitive to stream Elisp string chunks into the DuckDB Appender API for performance.

### 5.4 Smart Completion (The "Capf")

* **Feature:** `completion-at-point` for table names and column names.
* **Implementation:** **Hybrid.**
* **Module Primitive:** We need a way to introspect the schema quickly.
```elisp
(duckdb-get-tables connection) ;; Returns ("users" "orders")
(duckdb-get-columns connection "users") ;; Returns ("id" "name" "email")

```

* **Elisp:** A `completion-at-point-function` that calls these primitives when the user is typing inside a `duckdb-sql-mode` buffer.

---

## 6. Error Handling

All C-level errors (Syntax errors, file IO) must signal an Emacs error of type duckdb-error.

```elisp
(condition-case err
    (duckdb-execute conn "BAD SQL")
  (duckdb-error (message "Caught DuckDB error: %s" (cdr err))))
```

