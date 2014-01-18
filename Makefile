export RUSTC = rustc
BUILDDIR = build
export RUSTFLAGS = -O -Z debug-info

POSTGRES_LIB = lib.rs
POSTGRES = $(BUILDDIR)/$(shell $(RUSTC) --crate-file-name $(POSTGRES_LIB))
POSTGRES_TEST = $(BUILDDIR)/$(shell $(RUSTC) --test --crate-file-name $(POSTGRES_LIB))
OPENSSL_DIR = submodules/rust-openssl
OPENSSL = $(OPENSSL_DIR)/$(shell $(MAKE) -s -C $(OPENSSL_DIR) print-target)
PHF_DIR = submodules/rust-phf
PHF = $(PHF_DIR)/$(shell $(MAKE) -s -C $(PHF_DIR) print-targets)
LINK_ARGS = -L $(dir $(OPENSSL)) $(foreach file,$(PHF),-L $(dir $(file)))

all: $(POSTGRES)

-include $(BUILDDIR)/postgres.d
-include $(BUILDDIR)/postgres_test.d

$(BUILDDIR):
	mkdir -p $@

$(BUILDDIR)/submodule-trigger: submodules/submodule-trigger | $(BUILDDIR)
	git submodule init
	git submodule update
	touch $@

$(OPENSSL): $(BUILDDIR)/submodule-trigger | $(BUILDDIR)
	$(MAKE) -C $(OPENSSL_DIR)

$(PHF): $(BUILDDIR)/submodule-trigger | $(BUILDDIR)
	$(MAKE) -C $(PHF_DIR)

$(POSTGRES): $(POSTGRES_LIB) $(OPENSSL) $(PHF) | $(BUILDDIR)
	$(RUSTC) $(RUSTFLAGS) --dep-info $(@D)/postgres.d --out-dir $(@D) \
		$(LINK_ARGS) $<

$(POSTGRES_TEST): $(POSTGRES_LIB) $(OPENSSL) $(PHF) | $(BUILDDIR)
	$(RUSTC) $(RUSTFLAGS) --dep-info $(@D)/postgres_test.d --out-dir $(@D) \
		$(LINK_ARGS) --test $<

check: $(POSTGRES_TEST)
	$<

clean:
	rm -rf $(BUILDDIR)

doc: $(OPENSSL) $(PHF)
	rustdoc $(LINK_ARGS) $(POSTGRES_LIB)

.PHONY: all check clean
