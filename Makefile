ifeq ($(OS), Darwin)
DUCKDB_LIB := $(shell brew --prefix duckdb)/lib
DUCKDB_INC := $(shell brew --prefix duckdb)/include
else
DUCKDB_LIB := ./lib
DUCKDB_INC := ./lib
endif

CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -fPIC -pthread -I. -I$(DUCKDB_INC) -I./src
LDFLAGS = -pthread -L$(DUCKDB_LIB) -lduckdb -Wl,-rpath,'$$ORIGIN/lib'

EMACS   = emacs
RM      = rm -f

# The name of the module
MODULE  = duckdb-core.so

all: $(MODULE)

$(MODULE): duckdb-core.o base64_simd.o
	$(CC) -shared -o $@ $^ $(LDFLAGS)

duckdb-core.o: duckdb-core.c duckdb-api.h base64_simd.h
	$(CC) $(CFLAGS) -c $<

UNAME_M := $(shell uname -m)

ifeq ($(UNAME_M), x86_64)
    SIMD_FLAGS = -mavx2
else ifeq ($(UNAME_M), aarch64)
    SIMD_FLAGS = -march=armv8-a+simd
else ifeq ($(UNAME_M), arm64)
    SIMD_FLAGS = -march=armv8-a+simd
endif

base64_simd.o: base64_simd.c base64_simd.h
	$(CC) $(CFLAGS) $(SIMD_FLAGS) -c $<

clean:
	$(RM) *.o $(MODULE)

test: all
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-tests.el -f ert-run-tests-batch-and-exit
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-query-tests.el -f ert-run-tests-batch-and-exit
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-interactive-error-tests.el -f ert-run-tests-batch-and-exit
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-leak-tests.el -f ert-run-tests-batch-and-exit

asan-test: clean
	CFLAGS="$(CFLAGS) -fsanitize=address -g3" LDFLAGS="$(LDFLAGS) -fsanitize=address" $(MAKE) all
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH ASAN_OPTIONS=detect_leaks=1 $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-tests.el -f ert-run-tests-batch-and-exit
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH ASAN_OPTIONS=detect_leaks=1 $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-query-tests.el -f ert-run-tests-batch-and-exit
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH ASAN_OPTIONS=detect_leaks=1 $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-interactive-error-tests.el -f ert-run-tests-batch-and-exit
	LD_LIBRARY_PATH=$(DUCKDB_LIB):$$LD_LIBRARY_PATH ASAN_OPTIONS=detect_leaks=1 $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-leak-tests.el -f ert-run-tests-batch-and-exit

.PHONY: all clean test asan-test
