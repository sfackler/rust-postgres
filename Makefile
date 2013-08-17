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

.PHONY: check
check: check-postgres check-sqlite3

check-postgres: postgres.dummy src/postgres/test.rs
	$(RUSTC) $(RUSTFLAGS) --test src/postgres/test.rs -o $@
	./$@

check-sqlite3: sqlite3.dummy src/sqlite3/test.rs
	$(RUSTC) $(RUSTFLAGS) --test src/sqlite3/test.rs -o $@
	./$@

.PHONY: clean
clean:
	rm *.dummy *.so check-*
