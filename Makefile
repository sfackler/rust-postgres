export RUSTC = rustc
BUILDDIR = build
export RUSTFLAGS = -O -Z debug-info

POSTGRES_LIB = lib.rs
POSTGRES = $(BUILDDIR)/$(shell $(RUSTC) --crate-file-name $(POSTGRES_LIB))
POSTGRES_TEST = $(BUILDDIR)/$(shell $(RUSTC) --test --crate-file-name $(POSTGRES_LIB))
OPENSSL_DIR = submodules/rust-openssl
OPENSSL = $(OPENSSL_DIR)/$(shell $(MAKE) -s -C $(OPENSSL_DIR) print-target)

all: $(POSTGRES)

-include $(BUILDDIR)/postgres.d
-include $(BUILDDIR)/postgres_test.d

$(BUILDDIR):
	mkdir -p $@

$(BUILDDIR)/rust-openssl-trigger: submodules/rust-openssl-trigger | $(BUILDDIR)
	git submodule init
	git submodule update
	touch $@

$(OPENSSL): $(BUILDDIR)/rust-openssl-trigger | $(BUILDDIR)
	$(MAKE) -C $(OPENSSL_DIR)

$(POSTGRES): $(POSTGRES_LIB) $(OPENSSL) | $(BUILDDIR)
	$(RUSTC) $(RUSTFLAGS) --dep-info $(@D)/postgres.d --out-dir $(@D) \
		-L $(dir $(OPENSSL)) $<

$(POSTGRES_TEST): $(POSTGRES_LIB) $(OPENSSL) | $(BUILDDIR)
	$(RUSTC) $(RUSTFLAGS) --dep-info $(@D)/postgres_test.d --out-dir $(@D) \
		-L $(dir $(OPENSSL)) --test $<

check: $(POSTGRES_TEST)
	$<

clean:
	rm -rf $(BUILDDIR)

.PHONY: all check clean
