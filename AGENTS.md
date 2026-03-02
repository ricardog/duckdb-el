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
├── Makefile              # Build orchestration (compiles C, runs tests)
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
* **Error Handling:** Never use `exit()`. Use `env->non_local_exit_signal` to pass errors back to Elisp.
* **Memory:** Every `duckdb_database`, `duckdb_connection`, and `duckdb_result` must be wrapped in a `user_ptr` with a custom finalizer.
* **Type Safety:** Explicitly check types of `emacs_value` arguments using `env->type_of` before processing.

### 3.2 Emacs Lisp Conventions

* **Prefix:** All functions, variables, and faces must start with `duckdb-`.
* **Documentation:** Every function must have a docstring explaining arguments and return types.
* **Dependencies:** Use `(require 'cl-lib)` for modern list/struct manipulation.
* **Safety:** Use `unwind-protect` in macros to ensure resources are freed if Lisp code signals an error.

## 4. Implementation Strategy

### Phase 1: The "Thin" Layer

Implement `duckdb-open`, `duckdb-connect`, and a basic `duckdb-execute`. Focus on getting a `:memory:` database working and returning a simple confirmation string.

### Phase 2: Data Marshalling

Implement the logic to convert `duckdb_result` into Elisp lists. Handle mapping for:

* `DUCKDB_TYPE_BIGINT` -> `make_integer`
* `DUCKDB_TYPE_VARCHAR` -> `make_string`
* `DUCKDB_TYPE_DOUBLE` -> `make_float`

### Phase 3: Columnar Optimization

Implement `duckdb-select-columns`. This should bypass row-by-row iteration and use DuckDB's native chunks to populate Elisp vectors for significant performance gains in `vtable`.

## 5. Testing Strategy

We follow a "Lisp-Driven Testing" approach. Since the C code is an extension of the Lisp environment, we test it via Elisp.

* **Framework:** Use **ERT** (Emacs Lisp Regression Test).
* **Coverage:**
* **Resource Lifecycle:** Verify that opening/closing databases doesn't leak memory.
* **Type Fidelity:** Ensure large integers and floats retain precision across the C/Elisp boundary.
* **Error States:** Pass malformed SQL to C and verify that a Lisp-level `duckdb-error` is signaled.


* **CI Integration:** The `Makefile` should have a `test` target that runs:
`emacs -batch -l duckdb.el -l tests/duckdb-tests.el -f ert-run-tests-batch-and-exit`.

* **Address Sanitizer:** Use address sanitizer to ensure there are no
  memory bugs.  Add a target to the Makefile to build a version of the
  module with address sanitizer and then run the tests.


## 6. Agent Instructions for New Code

When generating code for this project:

1. Prioritize the use of `make_user_ptr` for any pointer that must persist across Lisp calls.
2. Ensure `emacs_module_init` performs all necessary `intern` and `fset` operations to register functions.
3. When writing Elisp, provide `(declare-function ...)` blocks so the byte-compiler is aware of C-defined primitives.

```
