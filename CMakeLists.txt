cmake_minimum_required(VERSION 3.10)
project(duckdb-emacs C)

set(CMAKE_C_STANDARD 11)

# Default build type to Release if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -fPIC")
set(CMAKE_C_FLAGS_RELEASE "-O2")
set(CMAKE_C_FLAGS_DEBUG "-g3 -O0")

# SIMD detection
if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    set(SIMD_FLAGS "-mavx2")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    set(SIMD_FLAGS "-march=armv8-a+simd")
endif()

# Find DuckDB
if(APPLE)
    execute_process(COMMAND brew --prefix duckdb
        OUTPUT_VARIABLE DUCKDB_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
endif()

if(DUCKDB_PREFIX)
    set(DUCKDB_INC "${DUCKDB_PREFIX}/include")
    set(DUCKDB_LIB_DIR "${DUCKDB_PREFIX}/lib")
else()
    set(DUCKDB_INC "${CMAKE_CURRENT_SOURCE_DIR}/lib")
    set(DUCKDB_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/lib")
endif()

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${DUCKDB_INC})

# Threads
find_package(Threads REQUIRED)

# The module
add_library(duckdb-core MODULE duckdb-core.c base64_simd.c)
set_target_properties(duckdb-core PROPERTIES PREFIX "")
set_target_properties(duckdb-core PROPERTIES SUFFIX ".so")

# SIMD flags for base64_simd.c
set_source_files_properties(base64_simd.c PROPERTIES COMPILE_FLAGS "${SIMD_FLAGS}")

# Copy iris.csv for tests
configure_file(iris.csv iris.csv COPYONLY)

# Linker flags
target_link_directories(duckdb-core PRIVATE ${DUCKDB_LIB_DIR})
target_link_libraries(duckdb-core PRIVATE duckdb Threads::Threads)

# RPATH configuration
if(APPLE)
    set_target_properties(duckdb-core PROPERTIES INSTALL_RPATH "@loader_path/lib")
    set_target_properties(duckdb-core PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE)
else()
    set_target_properties(duckdb-core PROPERTIES INSTALL_RPATH "$ORIGIN/lib")
    set_target_properties(duckdb-core PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE)
endif()

# ASAN support
option(ENABLE_ASAN "Enable AddressSanitizer" OFF)
if(ENABLE_ASAN)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
endif()

# Tests
enable_testing()
find_program(EMACS_EXECUTABLE emacs)

if(EMACS_EXECUTABLE)
    set(TEST_FILES
        duckdb-tests.el
        duckdb-query-tests.el
        duckdb-interactive-error-tests.el
        duckdb-leak-tests.el
    )

    foreach(TEST_FILE ${TEST_FILES})
        add_test(NAME ${TEST_FILE}
            COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${DUCKDB_LIB_DIR}:$ENV{LD_LIBRARY_PATH}"
            "${EMACS_EXECUTABLE}" -batch
            -L "${CMAKE_CURRENT_BINARY_DIR}"
            -L "${CMAKE_CURRENT_SOURCE_DIR}"
            -l "${CMAKE_CURRENT_SOURCE_DIR}/tests/test-helper.el"
            -l "${CMAKE_CURRENT_SOURCE_DIR}/tests/${TEST_FILE}"
            -f ert-run-tests-batch-and-exit
        )
    endforeach()
endif()
