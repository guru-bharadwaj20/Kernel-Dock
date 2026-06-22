obj-m += monitor.o

KDIR  := /lib/modules/$(shell uname -r)/build
PWD   := $(shell pwd)

# ── Compiler flag sets ────────────────────────────────────────────────
COMMON_FLAGS  := -Wall -Wextra -Wpedantic
RELEASE_FLAGS := $(COMMON_FLAGS) -O2
DEBUG_FLAGS   := $(COMMON_FLAGS) -O0 -g \
                 -fsanitize=address,undefined \
                 -fno-omit-frame-pointer
CI_FLAGS      := $(COMMON_FLAGS) -O2

# Default: release build (no sanitizers, optimized)
CFLAGS ?= $(RELEASE_FLAGS)

WORKLOAD_LDFLAGS ?= -static

USER_TARGETS := engine memory_hog cpu_hog io_pulse

# ── Top-level targets ─────────────────────────────────────────────────
.PHONY: all release debug ci module clean

all: release module

release: CFLAGS = $(RELEASE_FLAGS)
release: $(USER_TARGETS)
	@echo "Release build complete."

debug: CFLAGS = $(DEBUG_FLAGS)
debug: WORKLOAD_LDFLAGS =
debug: $(USER_TARGETS)
	@echo "Debug build complete (ASan + UBSan enabled)."

ci: CFLAGS = $(CI_FLAGS)
ci: WORKLOAD_LDFLAGS =
ci: $(USER_TARGETS)
	@echo "CI build complete."

module: monitor.ko

# ── Per-binary rules ──────────────────────────────────────────────────
engine: engine.c monitor_ioctl.h
	gcc $(CFLAGS) -o $@ engine.c -lpthread

memory_hog: memory_hog.c
	gcc $(CFLAGS) $(WORKLOAD_LDFLAGS) -o $@ memory_hog.c

cpu_hog: cpu_hog.c
	gcc $(CFLAGS) $(WORKLOAD_LDFLAGS) -o $@ cpu_hog.c

io_pulse: io_pulse.c
	gcc $(CFLAGS) $(WORKLOAD_LDFLAGS) -o $@ io_pulse.c

monitor.ko: monitor.c monitor_ioctl.h
	$(MAKE) -C $(KDIR) M=$(PWD) modules

# ── Clean ─────────────────────────────────────────────────────────────
clean:
	if [ -d "$(KDIR)" ]; then $(MAKE) -C $(KDIR) M=$(PWD) clean; fi
	rm -f $(USER_TARGETS)
	rm -f *.o *.mod *.mod.c *.symvers *.order
	rm -f *.log
	rm -rf logs
	rm -f /tmp/mini_runtime.sock
	@echo "Clean complete."
