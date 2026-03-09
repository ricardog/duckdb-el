# Agent Guide: emacs-duckdb Development

This document serves as the authoritative guide for an AI agent to assist in the development of the `emacs-duckdb` dynamic module.

## 1. Project Goals
- **Performance:** Utilize DuckDB’s columnar C API to provide faster data processing than the built-in SQLite.
- **Stability:** Ensure zero memory leaks by leveraging Emacs `user_ptr` finalizers for all C-allocated structs.
- **Ergonomics:** Provide an Elisp API that feels native to Emacs (e.g., `vtable` integration and `with-` macros).
- **Portability:** Maintain compatibility across Emacs 28+ without version-specific branching where possible.

## 2. Directory Structure
Adhere to the standard layout for Emacs dynamic modules to ensure ease of compilation and packaging.

```text
emacs-duckdb/
├── CMakeLists.txt         # Build orchestration (compiles C, runs tests)
├── README.md             # Project overview
├── AGENTS.md             # This file (AI instructions)
├── duckdb-core.c         # Main C module entry point (emacs_module_init)
├── duckdb-api.h          # Internal C headers and helper macros
├── duckdb.el             # Elisp wrapper, high-level UI, and declarations
├── lib/                  # Local copies of duckdb.h if not system-wide
└── tests/
    ├── test-helper.el    # Test environment setup
    └── duckdb-tests.el   # ERT (Emacs Lisp Regression Tool) suites

```

## 3. Coding Conventions

### 3.1 C (Module) Conventions

* **Naming:** Prefix all internal C functions with `duckdb_` and Lisp-exposed functions with `Fduckdb_`.
* **GPL:** Every module must include `int plugin_is_GPL_compatible;`.
* **Error Handling:** Never use `exit()`. Use `env->non_local_exit_signal` to pass errors back to Elisp. Use the `SIGNAL_ERROR` macro from `duckdb-api.h`.
* **Memory:** Every `duckdb_database`, `duckdb_connection`, and `duckdb_result` must be wrapped in a `user_ptr` with a custom finalizer.
* **Type Safety:** Explicitly check types of `emacs_value` arguments using `env->type_of` before processing.

### 3.2 Emacs Lisp Conventions

* **Prefix:** All functions, variables, and faces must start with `duckdb-`.
* **Documentation:** Every function must have a docstring explaining arguments and return types.
* **Dependencies:** Use `(require 'cl-lib)` for modern list/struct manipulation.
* **Safety:** Use `unwind-protect` in macros to ensure resources are freed if Lisp code signals an error.

## 4. Key Implementation Details (For Future Agents)

### 4.1 Asynchronous Query Mechanism
* **C-side:** `duckdb-select-async` spawns a `pthread` worker. Upon completion, the worker uses `kill(getpid(), SIGUSR1)` to notify Emacs.
* **Elisp-side:** `duckdb.el` binds `SIGUSR1` in `special-event-map` to `duckdb--handle-sigusr1`. This handler calls `duckdb-async-poll` (C) which executes the callback with query results.
* **Note:** The `async_ctx` struct must be carefully managed with `make_global_ref` for callbacks and `user_ptr` finalizers for cleanup.

### 4.2 Columnar Result Format
* `duckdb-select-columns` returns a plist: `(:data (:col1 [v1 v2 ...] :col2 [v3 v4 ...]) :types (:col1 "VARCHAR" :col2 "INTEGER"))`.
* Data columns are Emacs vectors (`[v1 v2 ...]`). This format is optimized for `vtable` and large dataset processing.

### 4.3 Type Marshalling & BLOBs
* **TIMESTAMP:** Represented as an integer of microseconds.
* **BLOBs:** Transferred via base64 encoding/decoding during the C/Elisp bridge to handle binary safety. Use `(duckdb-blob unibyte-string)` for parameters.
* **VARCHAR:** If `make_string` fails (invalid UTF-8), it falls back to a blob-like transfer to prevent module crashes.

### 4.4 Parameter Binding
* Parameters are passed as a list: `(duckdb-execute conn "SELECT ?" '(1))`.
* Special handling for NULLs: Uses the configurable `duckdb-null-symbol` (defaults to `nil`).

## 5. Testing Strategy

We follow a "Lisp-Driven Testing" approach. Since the C code is an extension of the Lisp environment, we test it via Elisp.

* **Framework:** Use **ERT** (Emacs Lisp Regression Test).
* **Coverage:**
  * **Resource Lifecycle:** Verify that opening/closing databases doesn't leak memory.
  * **Type Fidelity:** Ensure large integers and floats retain precision across the C/Elisp boundary.
  * **Error States:** Pass malformed SQL to C and verify that a Lisp-level `duckdb-error` is signaled.
  * **Asynchronous:** Test that callbacks fire and results match the expected row-major format.

* **Address Sanitizer:** Use address sanitizer to ensure there are no memory bugs. Enable ASAN in CMake using `-DENABLE_ASAN=ON`, rebuild, and run the tests. Always run the asan tests before declaring work completed.

## 6. Development Workflow (Agent Protocol)

1. **Reproduction:** Before fixing a bug, add a test case to `tests/duckdb-tests.el` that fails.
2. **Build:** Use `cmake ..` and `make` (with `-DENABLE_ASAN=ON` if needed) to compile.
3. **Verification:** Ensure `ctest` passes and that no memory leaks are reported by ASAN.
4. **Documentation:** Update docstrings in `duckdb.el` and `README.md` if the public API changes.
