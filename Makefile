CC      = gcc
CFLAGS  = -Wall -Wextra -O2 -fPIC -I. -I./lib
LDFLAGS = -L./lib -lduckdb -Wl,-rpath,'$$ORIGIN/lib'

EMACS   = emacs
RM      = rm -f

# The name of the module
MODULE  = duckdb-core.so

all: $(MODULE)

$(MODULE): duckdb-core.o
	$(CC) -shared -o $@ $^ $(LDFLAGS)

duckdb-core.o: duckdb-core.c duckdb-api.h
	$(CC) $(CFLAGS) -c $<

clean:
	$(RM) *.o $(MODULE)

test: all
	LD_LIBRARY_PATH=./lib:$$LD_LIBRARY_PATH $(EMACS) -batch -l tests/test-helper.el -l tests/duckdb-tests.el -f ert-run-tests-batch-and-exit

.PHONY: all clean test
