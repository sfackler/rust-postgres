RUSTC ?= rustc
RUSTFLAGS += -L.

.PHONY: all
all: sql.dummy postgres.dummy test_postgres sqlite3.dummy

sql.dummy: src/sql/lib.rs
	$(RUSTC) $(RUSTFLAGS) --lib $< -o $@
	touch $@

postgres.dummy: src/postgres/lib.rs sql.dummy
	$(RUSTC) $(RUSTFLAGS) --lib src/postgres/lib.rs -o $@
	touch $@

test_postgres: src/postgres/test.rs postgres.dummy
	$(RUSTC) $(RUSTFLAGS) --test src/postgres/test.rs -o $@

sqlite3.dummy: src/sqlite3/lib.rs sql.dummy
	$(RUSTC) $(RUSTFLAGS) --lib src/sqlite3/lib.rs -o $@
	touch $@

.PHONY: clean
clean:
	rm *.dummy *.so
