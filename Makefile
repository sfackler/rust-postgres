RUSTC ?= rustc
RUSTFLAGS += -L.

.PHONY: all
all: postgres.dummy

postgres.dummy: src/lib.rs
	$(RUSTC) $(RUSTFLAGS) --lib src/lib.rs -o $@
	touch $@

.PHONY: check
check: check-postgres

check-postgres: postgres.dummy src/test.rs
	$(RUSTC) $(RUSTFLAGS) --test src/test.rs -o $@
	./$@

.PHONY: clean
clean:
	rm *.dummy *.so check-*
