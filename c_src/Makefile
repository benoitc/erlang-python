# Makefile for py_nif

# Erlang paths
ERL_INCLUDE = $(shell erl -noshell -eval 'io:format("~s", [code:root_dir()])' -s init stop)/usr/include
ERL_LIB = $(shell erl -noshell -eval 'io:format("~s", [code:lib_dir(erl_interface)])' -s init stop)/lib

# Python paths (use python3-config)
PYTHON_CONFIG ?= python3-config
PYTHON_CFLAGS = $(shell $(PYTHON_CONFIG) --cflags)
PYTHON_LDFLAGS = $(shell $(PYTHON_CONFIG) --ldflags --embed 2>/dev/null || $(PYTHON_CONFIG) --ldflags)

# Compiler settings
CC ?= cc
CFLAGS = -O2 -Wall -fPIC -I$(ERL_INCLUDE) $(PYTHON_CFLAGS)
LDFLAGS = -shared $(PYTHON_LDFLAGS)

# Platform-specific settings
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    LDFLAGS += -undefined dynamic_lookup -flat_namespace
    SO_EXT = .so
else ifeq ($(UNAME_S),Linux)
    # Export Python symbols for extension modules, set rpath for runtime
    LDFLAGS += -Wl,--export-dynamic -Wl,-rpath,'$$ORIGIN'
    SO_EXT = .so
else
    SO_EXT = .dll
endif

# Output
PRIV_DIR = ../priv
NIF_SO = $(PRIV_DIR)/py_nif$(SO_EXT)

# Sources
SOURCES = py_nif.c
OBJECTS = $(SOURCES:.c=.o)

.PHONY: all clean

all: $(PRIV_DIR) $(NIF_SO)

$(PRIV_DIR):
	mkdir -p $(PRIV_DIR)

$(NIF_SO): $(OBJECTS)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

clean:
	rm -f $(OBJECTS) $(NIF_SO)

# Debug target
debug: CFLAGS += -g -DDEBUG
debug: clean all

# Show configuration
info:
	@echo "ERL_INCLUDE: $(ERL_INCLUDE)"
	@echo "PYTHON_CFLAGS: $(PYTHON_CFLAGS)"
	@echo "PYTHON_LDFLAGS: $(PYTHON_LDFLAGS)"
	@echo "NIF_SO: $(NIF_SO)"
