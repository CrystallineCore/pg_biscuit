# Makefile for pg_biscuit PostgreSQL extension

EXTENSION = pg_biscuit
EXTVERSION = 0.9.0
MODULE_big = pg_biscuit
OBJS = src/pg_biscuit.o
DATA = sql/pg_biscuit--1.0.sql


PGFILEDESC = "LIKE pattern matching with bitmap indexing"

# PostgreSQL build system
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Compiler flags for stricter checking
override CFLAGS += -Wall -Wmissing-prototypes -Wpointer-arith -Werror=vla -Wendif-labels

# Default target: ensure versioned SQL is generated before build
all: sql/pg_biscuit--1.0.sql

# Build versioned SQL script from base SQL file if needed
sql/pg_biscuit--1.0.sql: sql/pg_biscuit.sql
	cp $< $@

# Clean up build artifacts
.PHONY: clean
clean:
	rm -f src/pg_biscuit.o src/pg_biscuit.bc pg_biscuit.so

# Manual install target (optional; PGXS normally handles this)
install: all
	$(INSTALL) -d $(DESTDIR)$(pkglibdir)
	$(INSTALL) -m 755 pg_biscuit.so $(DESTDIR)$(pkglibdir)/
	$(INSTALL) -d $(DESTDIR)$(datadir)/extension
	$(INSTALL) -m 644 pg_biscuit.control $(DESTDIR)$(datadir)/extension/
	$(INSTALL) -m 644 sql/pg_biscuit--1.0.sql $(DESTDIR)$(datadir)/extension/

dist:
	@echo "Creating distribution archive..."
	rm -rf dist
	mkdir dist
	cp -r $(shell ls | grep -v dist) dist/
	cd dist && zip -r ../$(EXTENSION)-$(EXTVERSION).zip .
	@echo "Created $(EXTENSION)-$(EXTVERSION).zip"