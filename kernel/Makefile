RUNNING_KERNELRELEASE := $(shell uname -r)
KDIR ?= /lib/modules/$(RUNNING_KERNELRELEASE)/build
TARGET_KERNELRELEASE := $(shell cat $(KDIR)/include/config/kernel.release 2>/dev/null)
KERNELRELEASE ?= $(TARGET_KERNELRELEASE)
TARGET_ARCH := $(or $(strip $(shell sed -n \
	-e 's/^CONFIG_X86_64=y$$/x86_64/p' \
	-e 's/^CONFIG_ARM64=y$$/arm64/p' \
	$(KDIR)/include/config/auto.conf 2>/dev/null)),unknown)

# Kbuild has supported separate external-module output directories since Linux
# 6.13. Keep target-specific generated bindings and module products out of the
# source tree. Callers can request an in-place build by setting MO to an empty
# value.
DEFAULT_MO := $(abspath \
	$(CURDIR)/../target/kernel/$(TARGET_ARCH)/$(KERNELRELEASE))
MO ?= $(DEFAULT_MO)
MODULE_OUTPUT := $(if $(strip $(MO)),$(abspath $(MO)),$(CURDIR))
MODULE_PATH := $(MODULE_OUTPUT)/zerofs.ko
KBUILD_MO_ARG := $(if $(strip $(MO)),MO=$(MODULE_OUTPUT))

# Rust's kernel API is not stable across releases. Compatibility is determined
# from the exact target tree below and by compiling the module, not by a release
# allowlist that would unnecessarily exclude working distribution kernels.
TARGET_RUSTC_VERSION_TEXT := $(shell sed -n \
	's/^CONFIG_RUSTC_VERSION_TEXT=//p' \
	$(KDIR)/include/config/auto.conf 2>/dev/null)
TARGET_RUSTC_RELEASE := $(word 2,$(TARGET_RUSTC_VERSION_TEXT))
TARGET_RUSTC_SERIES := $(basename $(TARGET_RUSTC_RELEASE))
TARGET_BINDGEN_VERSION_TEXT := $(shell sed -n \
	's/^CONFIG_BINDGEN_VERSION_TEXT=//p' \
	$(KDIR)/include/config/auto.conf 2>/dev/null)
TARGET_BINDGEN_RELEASE := $(word 2,$(TARGET_BINDGEN_VERSION_TEXT))
TARGET_BINDGEN_SERIES := $(basename $(TARGET_BINDGEN_RELEASE))

# Distribution kernels build their Rust metadata with a distribution rustc.
# Prefer the target's versioned distribution binary, then the distribution
# default, over a rustup toolchain in PATH so the metadata versions match.
RUSTC ?= $(strip $(if $(wildcard /usr/bin/rustc-$(TARGET_RUSTC_SERIES)),\
	/usr/bin/rustc-$(TARGET_RUSTC_SERIES),\
	$(if $(wildcard /usr/bin/rustc),/usr/bin/rustc,rustc)))
BINDGEN ?= $(strip $(if $(wildcard /usr/bin/bindgen-$(TARGET_BINDGEN_SERIES)),\
	/usr/bin/bindgen-$(TARGET_BINDGEN_SERIES),\
	$(if $(wildcard /usr/bin/bindgen),/usr/bin/bindgen,bindgen)))
CLANG ?= clang
LLVM_LINK ?= llvm-link
LLVM_OPT ?= opt
LLVM_NM ?= llvm-nm
TARGET_IS_CLANG := $(shell grep -qx 'CONFIG_CC_IS_CLANG=y' \
	$(KDIR)/include/config/auto.conf 2>/dev/null && printf y)
TARGET_CONFIG_CC := $(shell sed -n \
	's/^CONFIG_CC_VERSION_TEXT=\([^ ]*\).*/\1/p' \
	$(KDIR)/include/config/auto.conf 2>/dev/null)
TARGET_GCC := $(or $(shell command -v "$(TARGET_CONFIG_CC)" 2>/dev/null),gcc)
TARGET_CC ?= $(if $(TARGET_IS_CLANG),$(CLANG),$(TARGET_GCC))
TARGET_LD ?=
TARGET_AR ?=
TARGET_NM ?=
TARGET_OBJCOPY ?=
TARGET_OBJDUMP ?=
TARGET_READELF ?=
TARGET_STRIP ?=
KERNEL_SRC ?=
SELF_CONTAINED_OUTPUT ?= $(abspath \
	$(CURDIR)/../target/kernel-support/$(TARGET_ARCH)/$(KERNELRELEASE))

KBUILD_COMMON_ARGS := \
	KERNELRELEASE=$(KERNELRELEASE) \
	RUSTC=$(RUSTC) \
	BINDGEN=$(BINDGEN)
KBUILD_ARGS := \
	$(KBUILD_COMMON_ARGS) \
	$(KBUILD_MO_ARG)

.PHONY: all modules check clean install module-path self-contained
.PHONY: test test-protocol test-tags

all modules: check
	$(MAKE) -C $(KDIR) M=$(CURDIR) $(KBUILD_ARGS) modules

self-contained:
	@test -n "$(KERNEL_SRC)" || { \
		echo "KERNEL_SRC must name the exact full kernel source tree" >&2; \
		exit 1; \
	}
	$(CURDIR)/self_contained/build.sh \
		"$(KERNEL_SRC)" "$(KDIR)" "$(CURDIR)" "$(MODULE_OUTPUT)" \
		"$(SELF_CONTAINED_OUTPUT)" "$(RUSTC)" "$(BINDGEN)" \
		"$(TARGET_CC)" "$(TARGET_LD)" "$(TARGET_AR)" "$(TARGET_NM)" \
		"$(TARGET_OBJCOPY)" "$(TARGET_OBJDUMP)" "$(TARGET_READELF)" \
		"$(TARGET_STRIP)" "$(CLANG)" "$(LLVM_LINK)" "$(LLVM_OPT)" \
		"$(LLVM_NM)"

check:
	@test -d "$(KDIR)" || { \
		echo "kernel build tree not found: $(KDIR)" >&2; \
		exit 1; \
	}
	@test "$(KERNELRELEASE)" = "$(TARGET_KERNELRELEASE)" || { \
		echo "KERNELRELEASE ($(KERNELRELEASE)) does not match KDIR ($(TARGET_KERNELRELEASE))" >&2; \
		exit 1; \
	}
	@grep -qx 'CONFIG_MODULES=y' "$(KDIR)/include/config/auto.conf" || { \
		echo "target kernel does not enable CONFIG_MODULES" >&2; \
		exit 1; \
	}
	@grep -qx 'CONFIG_RUST=y' "$(KDIR)/include/config/auto.conf" || { \
		echo "target kernel does not enable CONFIG_RUST" >&2; \
		exit 1; \
	}
	@grep -Eq '^CONFIG_NETFS_SUPPORT=(y|m)$$' \
		"$(KDIR)/include/config/auto.conf" || { \
		echo "target kernel does not enable CONFIG_NETFS_SUPPORT" >&2; \
		exit 1; \
	}
	@grep -Eq '^CONFIG_UNIX=(y|m)$$' \
		"$(KDIR)/include/config/auto.conf" || { \
		echo "target kernel does not enable CONFIG_UNIX" >&2; \
		exit 1; \
	}
	@grep -qx 'CONFIG_FILE_LOCKING=y' \
		"$(KDIR)/include/config/auto.conf" || { \
		echo "target kernel does not enable CONFIG_FILE_LOCKING" >&2; \
		exit 1; \
	}
	@grep -Eq '^CONFIG_(X86_64|ARM64)=y$$' \
		"$(KDIR)/include/config/auto.conf" || { \
		echo "supported architectures are x86_64 and arm64" >&2; \
		exit 1; \
	}
	@! grep -qx 'CONFIG_CPU_BIG_ENDIAN=y' \
		"$(KDIR)/include/config/auto.conf" || { \
		echo "big-endian targets are not supported" >&2; \
		exit 1; \
	}
	@! grep -Eq '^CONFIG_(GCC_PLUGIN_)?RANDSTRUCT=y$$' \
		"$(KDIR)/include/config/auto.conf" || { \
		echo "target kernel randomizes private VFS layouts" >&2; \
		exit 1; \
	}
	@test -s "$(KDIR)/rust/libkernel.rmeta" || { \
		echo "prebuilt Rust kernel metadata not found below $(KDIR)/rust" >&2; \
		echo "target compiler: $(TARGET_RUSTC_VERSION_TEXT)" >&2; \
		echo "install or build the Rust metadata for $(TARGET_KERNELRELEASE)" >&2; \
		exit 1; \
	}
	@command -v "$(RUSTC)" >/dev/null || { \
		echo "Rust compiler not found: $(RUSTC)" >&2; \
		exit 1; \
	}
	@command -v "$(BINDGEN)" >/dev/null || { \
		echo "bindgen not found: $(BINDGEN)" >&2; \
		exit 1; \
	}
	@kernel_rustc="$(TARGET_RUSTC_VERSION_TEXT)"; \
	host_rustc="$$("$(RUSTC)" --version)"; \
	if test -n "$$kernel_rustc" && test "$$kernel_rustc" != "$$host_rustc"; then \
		echo "Rust metadata/compiler mismatch:" >&2; \
		echo "  kernel: $$kernel_rustc" >&2; \
		echo "  module: $$host_rustc" >&2; \
		exit 1; \
	fi
	@if grep -qx 'CONFIG_MODVERSIONS=y' \
		"$(KDIR)/include/config/auto.conf"; then \
		grep -qx 'CONFIG_EXTENDED_MODVERSIONS=y' \
			"$(KDIR)/include/config/auto.conf" || { \
			echo "CONFIG_EXTENDED_MODVERSIONS is required for Rust modules" >&2; \
			exit 1; \
		}; \
		test -s "$(KDIR)/Module.symvers" || { \
			echo "Module.symvers is required when CONFIG_MODVERSIONS=y" >&2; \
			exit 1; \
		}; \
	fi

clean:
ifneq ($(MODULE_OUTPUT),$(CURDIR))
	$(MAKE) -C $(KDIR) M=$(CURDIR) $(KBUILD_COMMON_ARGS) MO= clean
endif
	$(MAKE) -C $(KDIR) M=$(CURDIR) $(KBUILD_ARGS) clean

install: all
	$(MAKE) -C $(KDIR) M=$(CURDIR) $(KBUILD_ARGS) modules_install

module-path:
	@printf '%s\n' "$(MODULE_PATH)"

test: test-protocol test-tags

test-protocol:
	$(RUSTC) --edition=2024 --test -D warnings --cfg MODULE \
		$(CURDIR)/protocol.rs -o /tmp/zerofs-protocol-tests
	/tmp/zerofs-protocol-tests

test-tags:
	$(RUSTC) --edition=2024 --test -D warnings \
		$(CURDIR)/tag_space_tests.rs -o /tmp/zerofs-tag-space-tests
	/tmp/zerofs-tag-space-tests
