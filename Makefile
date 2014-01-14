RUSTPKG ?= rustpkg
RUSTC ?= rustc
RUST_FLAGS ?= -Z debug-info -O

all:
	$(RUSTPKG) $(RUST_FLAGS) install

test:
	$(RUSTC) $(RUST_FLAGS) --test lib.rs
	./rust-postgres

.PHONY: test

clean:
	rm -rf .rust rust-postgres rust-postgres.dSYM
