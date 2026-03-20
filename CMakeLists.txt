cmake_minimum_required(VERSION 3.10)
project(duckdb-emacs C CXX)

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
    if(DUCKDB_PREFIX)
        set(DUCKDB_INC "${DUCKDB_PREFIX}/include")
        set(DUCKDB_LIB_DIR "${DUCKDB_PREFIX}/lib")
        find_library(DUCKDB_LIBRARY NAMES duckdb PATHS "${DUCKDB_LIB_DIR}" NO_DEFAULT_PATH)
    endif()
endif()

# Fallback: Search standard system paths (/usr/local, /usr)
if(NOT DUCKDB_INC OR NOT DUCKDB_LIBRARY)
    find_path(DUCKDB_INC_DIR NAMES duckdb.h)

    if(NOT APPLE)
        # On Linux, try to find the static library first to avoid runtime dependency issues (libstdc++, etc.)
        find_library(DUCKDB_LIBRARY_STATIC NAMES libduckdb.a duckdb_static)
        if(DUCKDB_LIBRARY_STATIC)
            set(DUCKDB_LIBRARY "${DUCKDB_LIBRARY_STATIC}")
        else()
            find_library(DUCKDB_LIBRARY NAMES duckdb)
        endif()
    else()
        find_library(DUCKDB_LIBRARY NAMES duckdb)
    endif()

    if(DUCKDB_INC_DIR AND DUCKDB_LIBRARY)
        set(DUCKDB_INC "${DUCKDB_INC_DIR}")
        # Get the directory containing the library for the RPATH/Linker
        get_filename_component(DUCKDB_LIB_DIR "${DUCKDB_LIBRARY}" DIRECTORY)
    else()
        message(FATAL_ERROR "DuckDB headers or library not found. Install duckdb first.")
    endif()
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
target_link_directories(duckdb-core PRIVATE "${DUCKDB_LIB_DIR}")

if(NOT APPLE)
    # On Linux, linking statically with libduckdb.a can lead to missing internal 
    # C++ symbols (like fmt) because they are often marked as unique/weak and 
    # not included in the static archive in a way that allows them to be 
    # exported by a shared module.
    # We prefer dynamic linking but ensure RPATH is set up correctly.

    # Check if we are linking against a static library (if user forced it)
    get_filename_component(DUCKDB_LIB_EXT "${DUCKDB_LIBRARY}" EXT)
    if(DUCKDB_LIB_EXT STREQUAL ".a")
        message(STATUS "Linking DuckDB statically")
        target_link_libraries(duckdb-core PRIVATE 
            -Wl,--whole-archive "${DUCKDB_LIBRARY}" -Wl,--no-whole-archive
            Threads::Threads m dl rt)
        target_link_options(duckdb-core PRIVATE "-static-libstdc++" "-static-libgcc")
    else()
        message(STATUS "Linking DuckDB dynamically")
        target_link_libraries(duckdb-core PRIVATE "${DUCKDB_LIBRARY}" Threads::Threads)
        # Ensure we look in the directory where the library was found at runtime
        set_target_properties(duckdb-core PROPERTIES INSTALL_RPATH "$ORIGIN;${DUCKDB_LIB_DIR}")
    endif()

    # Force C++ linker to ensure libstdc++ and other C++ requirements are handled correctly
    set_target_properties(duckdb-core PROPERTIES LINKER_LANGUAGE CXX)
else()    # On MacOS, we use the library found (should be .dylib or .a)
    target_link_libraries(duckdb-core PRIVATE "${DUCKDB_LIBRARY}" Threads::Threads)
endif()
# RPATH configuration
if(APPLE)
    set_target_properties(duckdb-core PROPERTIES INSTALL_RPATH "@loader_path;${DUCKDB_LIB_DIR}")
else()
    set_target_properties(duckdb-core PROPERTIES INSTALL_RPATH "$ORIGIN;${DUCKDB_LIB_DIR}")
endif()
set_target_properties(duckdb-core PROPERTIES BUILD_WITH_INSTALL_RPATH TRUE)

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
            COMMAND "${EMACS_EXECUTABLE}" -batch
            -L "${CMAKE_CURRENT_BINARY_DIR}"
            -L "${CMAKE_CURRENT_SOURCE_DIR}"
            -l "${CMAKE_CURRENT_SOURCE_DIR}/tests/test-helper.el"
            -l "${CMAKE_CURRENT_SOURCE_DIR}/tests/${TEST_FILE}"
            -f ert-run-tests-batch-and-exit
        )
    endforeach()
endif()
