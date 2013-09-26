RUSTC ?= rustc
RUSTFLAGS += -L. -Z debug-info

.PHONY: all
all: postgres.dummy

postgres.dummy: src/lib.rs src/message.rs src/types.rs src/error.rs src/pool/mod.rs
	$(RUSTC) $(RUSTFLAGS) --lib src/lib.rs --out-dir .
	touch $@

.PHONY: check
check: check-postgres check-pool

check-postgres: postgres.dummy src/test.rs
	$(RUSTC) $(RUSTFLAGS) --test src/test.rs -o $@
	./$@

check-pool: postgres.dummy src/pool/test.rs
	$(RUSTC) $(RUSTFLAGS) --test src/pool/test.rs -o $@
	./$@

.PHONY: clean
clean:
	git clean -dfx
