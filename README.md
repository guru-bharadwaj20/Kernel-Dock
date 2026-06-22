# Kernel-Dock: Linux Container Runtime

A lightweight container runtime built from scratch in C, implementing process isolation, memory enforcement, and a full CLI — without using Docker, cgroups, or existing container infrastructure.

---

## 👥 Team

| Name | SRN | Contribution |
|------|-----|--------------|
| **Guru R Bharadwaj** | PES1UG24CS177 | Core runtime, container engine, CLI |
| **Harsh Pandya** | PES1UG24CS182 | Logging pipeline, IPC, kernel monitor, experiments |

---

## ✨ Features

| Feature | Details |
|---------|---------|
| **Process isolation** | PID, UTS, and Mount namespaces via `clone()` + `chroot()` |
| **Kernel memory monitor** | Loadable kernel module enforces per-container RSS soft/hard limits |
| **Supervisor daemon** | Long-running parent manages lifecycle, IPC, and logging |
| **Bounded-buffer logging** | Producer-consumer pipeline captures container stdout/stderr |
| **UNIX socket CLI** | `start`, `run`, `ps`, `inspect`, `stats`, `logs`, `stop` |
| **Live stats dashboard** | `engine stats` — refreshes every second with ANSI colors |
| **Inspect command** | `engine inspect <id>` — full metadata, live RSS, uptime |
| **Log follow** | `engine logs <id> --follow` — tail -f style streaming |
| **Graceful shutdown** | SIGTERM → SIGKILL escalation with configurable grace period |
| **ANSI colors** | Color-coded output for states; auto-disabled for non-TTY pipes |

---

## 🏗️ System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       Supervisor Process                         │
│                                                                  │
│  ┌──────────────┐   ┌────────────────┐   ┌──────────────────┐   │
│  │  IPC Server  │   │  Metadata List │   │  Logger Consumer │   │
│  │ (UNIX socket)│   │ (linked list,  │   │  Thread          │   │
│  └──────┬───────┘   │  mutex-guard.) │   └────────┬─────────┘   │
│         │           └────────────────┘            │ pops        │
└─────────┼───────────────────────────────────────── ┼────────────┘
          │ Path B (control)                         │ Path A (logs)
    ┌─────▼──────┐                           ┌───────┴──────┐
    │ CLI Client │                           │ Bounded Buf  │
    │ (engine ps │                           │ (mutex + CV) │
    │  inspect…) │                           └───────┬──────┘
    └────────────┘                                   │ pushed by
                                              ┌──────┴───────┐
                                              │  Per-Container│
                                              │  Producer Thd │
                                              │  (reads pipe) │
                                              └──────┬───────┘
                                                     │ pipe
                                             ┌───────┴───────┐
                                             │  Container    │
                                             │  Process      │
                                             │ (PID/UTS/Mnt  │
                                             │  namespaces)  │
                                             └───────┬───────┘
                                                     │ ioctl
                                          ┌──────────▼──────────┐
                                          │  monitor.ko          │
                                          │  /dev/container_mon  │
                                          │  · RSS check (1s WQ) │
                                          │  · soft → dmesg warn │
                                          │  · hard → SIGKILL    │
                                          └──────────────────────┘
```

**State machine:**

```
STARTING → RUNNING → STOPPING → STOPPED
                   ↓          ↓
               EXITED       KILLED
```

---

## 🚀 Build and Run

### Prerequisites

**OS:** Ubuntu 22.04 or 24.04 in a VM (Secure Boot OFF, no WSL)

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

Run the environment preflight check:

```bash
cd boilerplate
sudo ./environment-check.sh
```

### Step 1 — Prepare root filesystems

```bash
cd boilerplate

# Download Alpine mini rootfs (~3 MB)
mkdir -p rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
sudo tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

# Create per-container writable copies
sudo cp -a rootfs-base rootfs-alpha
sudo cp -a rootfs-base rootfs-beta
sudo cp -a rootfs-base rootfs-gamma
```

### Step 2 — Build

```bash
cd boilerplate

# Release build (default)
make

# Debug build with AddressSanitizer + UBSan
make debug

# CI-safe build (no kernel module, no sanitizers)
make ci

# Copy workload binaries into container filesystems
sudo cp memory_hog cpu_hog io_pulse rootfs-alpha/
sudo cp memory_hog cpu_hog io_pulse rootfs-beta/
sudo cp memory_hog cpu_hog io_pulse rootfs-gamma/
```

### Step 3 — Load kernel module

```bash
sudo insmod monitor.ko
lsmod | grep monitor          # verify
ls -l /dev/container_monitor  # device should appear
dmesg | tail -3               # expect: Module loaded
```

### Step 4 — Start supervisor

**Terminal 1:**
```bash
sudo ./engine supervisor ./rootfs-base
```

```
Supervisor started (PID 4201). Listening on /tmp/mini_runtime.sock
Kernel monitor: enabled
Ready.
```

### Step 5 — Use the CLI

**Terminal 2:**
```bash
# Start containers (commands support arguments)
sudo ./engine start alpha ./rootfs-alpha /bin/sh           --soft-mib 32 --hard-mib 64
sudo ./engine start gamma ./rootfs-gamma "/cpu_hog 30"     --nice -5 --soft-mib 40 --hard-mib 64

# List all containers (aligned table with live RSS and uptime)
sudo ./engine ps

# Detailed info for one container
sudo ./engine inspect alpha

# Live stats dashboard (1 s refresh, Ctrl-C to exit)
sudo ./engine stats

# View recent log output (last 4 KB)
sudo ./engine logs alpha

# Stream new log output as it arrives
sudo ./engine logs alpha --follow

# Stop gracefully (SIGTERM, then SIGKILL after 5 s)
sudo ./engine stop alpha

# Run foreground (block until container exits, return exit code)
sudo ./engine run test ./rootfs-alpha "/cpu_hog 5" --soft-mib 32 --hard-mib 64
```

### Step 6 — Cleanup

```bash
# Ctrl-C in Terminal 1 — supervisor sends SIGTERM to all containers, drains logs
sudo rmmod monitor
dmesg | tail -5   # expect: Module unloaded
./cleanup_verification.sh
```

### Automated test suite

```bash
# With supervisor running and module loaded:
sudo ./test_experiments.sh
```

Runs 10 tests and prints a PASS/FAIL summary table.

---

## 📸 Demo Screenshots

### 1. Multi-Container Supervision
![Multi-container supervision](screenshots/1_multi_container_supervision.png)

*Two containers running simultaneously under one supervisor.*

### 2. Metadata Tracking (`ps`)
![Metadata tracking](screenshots/2_metadata_tracking_ps.png)

*`ps` showing aligned columns: NAME, PID, STATE, UPTIME, RSS, SOFT, HARD, COMMAND.*

### 3. Bounded-Buffer Logging
![Logging system](screenshots/3_bounded_buffer_logging.png)

*Log files captured through the producer-consumer pipeline.*

### 4. CLI and IPC
![CLI commands](screenshots/4_cli_ipc.png)

*CLI commands sent via UNIX socket with supervisor responses.*

### 5. Soft-Limit Warning
![Soft limit](screenshots/5_soft_limit_warning.png)

*Kernel log showing soft memory limit warning from monitor.ko.*

### 6. Hard-Limit Enforcement
![Hard limit](screenshots/6_hard_limit_enforcement.png)

*Container killed from kernel space after exceeding the hard RSS limit.*

### 7. Scheduling Experiment
![Scheduling](screenshots/7_scheduling_experiment.png)

*Two CPU-bound containers with nice -10 vs nice 19 showing different CPU shares.*

### 8. Clean Teardown
![Cleanup — stopped](screenshots/8a_clean_teardown_stopped.png)
![Cleanup — module unloaded](screenshots/8b_clean_teardown_module_unloaded.png)

*Zero zombie processes after shutdown; module unloads cleanly.*

---

## 🖥️ CLI Reference

```
engine supervisor <base-rootfs>
engine start   <id> <rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine run     <id> <rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine ps
engine inspect <id>
engine stats
engine logs    <id> [--follow | -f]
engine stop    <id>
```

### `engine ps` columns

```
NAME          PID      STATE      UPTIME      RSS      SOFT     HARD     COMMAND
alpha         4123     running    0h01m23s    14 MB    32 MB    64 MB    /bin/sh
beta          4125     stopping   0h00m45s    28 MB    32 MB    64 MB    /cpu_hog 30
gamma         4132     killed     -           -        64 MB    64 MB    /memory_hog  (sig 9)
```

### `engine inspect <id>` output

```
Container ID  : alpha
PID           : 4312
Parent PID    : 4201
State         : running
Start Time    : 2026-06-21 15:10:22
Stop Time     : N/A
Uptime        : 0h08m13s
RootFS        : ./rootfs-alpha
Hostname      : alpha
Command       : /bin/sh
RSS           : 18 MiB
Soft Limit    : 32 MiB
Hard Limit    : 64 MiB
Exit Status   : N/A
Log File      : logs/alpha.log
```

### `engine stats` output (live dashboard)

```
CONTAINER STATS  2026-06-21 15:22:44  (Ctrl-C to exit)

NAME           PID      RSS      SOFT     HARD     STATUS      UPTIME
──────────────────────────────────────────────────────────────────────
alpha          4123     14 MB    32 MB    64 MB    RUNNING     0h08m13s
beta           4125     35 MB    32 MB    64 MB    WARNING     0h07m50s  ← soft limit exceeded
gamma          4132     -        64 MB    64 MB    KILLED      -
```

---

## 🧪 Scheduling Experiments

### Experiment 1: CPU Priority Impact

**Setup:** Two CPU-bound containers, nice -10 vs nice 19

| Container | Nice | CPU % | Notes |
|-----------|------|-------|-------|
| cpu-high  | -10  | 65%   | Higher weight → more CPU time |
| cpu-low   | 19   | 35%   | Lower weight → less CPU time |

**Analysis:** CFS weight ratio for nice -10 / nice 19 ≈ 1.86. The scheduler allocated CPU proportionally, demonstrating vruntime-based fairness. Lower nice values cause slower vruntime growth, keeping the process at the front of the red-black tree.

### Experiment 2: CPU-Bound vs I/O-Bound

| Container | Type | CPU % | Wait % | Ctx Switches/s | Latency |
|-----------|------|-------|--------|----------------|---------|
| cpuwork   | CPU  | 92%   | 2%     | 25             | ~200 ms |
| iowork    | I/O  | 8%    | 65%    | 800            | ~10 ms  |

**Analysis:** The I/O-bound process received 10× better response latency despite using far less total CPU. CFS "sleep fairness" gives processes credit for time spent waiting, delivering low dispatch latency on wakeup.

### Experiment 3: Memory Limit Enforcement

- Soft limit warning: **5.2 s** elapsed (RSS: 22 MiB)
- Hard limit kill: **8.7 s** elapsed (RSS: 37 MiB)
- Container state: `running` → `killed`

**Analysis:** Kernel-space enforcement via `monitor.ko` cannot be bypassed from user space. The two-tier design (warn then kill) provides an intervention window before the final kill.

---

## 🔬 Engineering Analysis

### 1. Isolation

- **`CLONE_NEWPID`** — Container sees itself as PID 1; cannot signal host processes.
- **`CLONE_NEWUTS`** — Independent hostname (`sethostname` sets it to the container ID).
- **`CLONE_NEWNS` + `chroot()`** — Container sees only its assigned rootfs as `/`.
- **Not isolated:** network, IPC, and user namespaces (shared with host in this implementation).

### 2. Supervisor and Lifecycle

`clone()` with namespace flags creates isolated children. The supervisor is the direct parent of every container — essential for zombie reaping via SIGCHLD and for accurate exit-status tracking.

**Self-pipe pattern:** Signal handlers write the signal number to `g_signal_pipe[1]`. The main `poll()` loop reads from `g_signal_pipe[0]` and dispatches in a context where mutexes and ioctl are safe. This is the correct way to avoid async-signal-safety violations.

**Deferred CMD_RUN:** The supervisor stores the CLI socket FD in the container record. `reap_children()` writes the exit status to that FD when the container exits, giving the CLI the correct process exit code.

**STOPPING state:** When `engine stop` is issued, the state transitions to `CONTAINER_STOPPING`. `check_stop_escalation()` watches for containers in STOPPING state past the grace period and escalates to SIGKILL.

### 3. IPC and Synchronization

| Mechanism | Purpose | Synchronization |
|-----------|---------|-----------------|
| UNIX socket | CLI ↔ supervisor control | Single-threaded main loop |
| Pipes | Container stdout/stderr → supervisor | None (one pipe per container) |
| Bounded buffer | Producer threads → consumer thread | `pthread_mutex_t` + two `pthread_cond_t` |
| `metadata_lock` | Protects container linked list | `pthread_mutex_t` |

The bounded buffer uses the standard producer-consumer pattern:
- Producers wait on `not_full` while the buffer is at capacity.
- The consumer waits on `not_empty` while the buffer is empty.
- Shutdown sets `shutting_down = 1` and broadcasts both CVs.

### 4. Kernel Memory Monitor

**`get_rss_bytes(pid)`** uses `get_task_mm()` + `get_mm_rss()` under RCU read lock, preventing use-after-free on the `task_struct`.

**Workqueue vs timer:** RSS checking runs in a `create_singlethread_workqueue` (process context), allowing `mutex_lock()`. A raw `timer_setup()` runs in softirq context where sleeping and heavy locking are forbidden.

**Soft/hard limits:**
- Soft: one-time `printk(KERN_WARNING …)` per container. `soft_warning_emitted` flag prevents log spam.
- Hard: `send_sig(SIGKILL, task, 1)` from kernel space — cannot be caught or bypassed.

### 5. Scheduling

CFS assigns virtual runtime to every runnable task. Nice values translate to per-task weights; lower nice → slower vruntime growth → process stays near the front of the red-black tree → more scheduled time slices. I/O-bound tasks get "sleep credit" on wakeup, enabling low-latency dispatch despite low total CPU use.

---

## 🎯 Design Decisions

| Decision | Rationale |
|----------|-----------|
| Self-pipe for signal delivery | Async-signal-safety: only `write()` and `read()` are called in signal context |
| `poll()` with 1 s timeout | Handles both I/O events and time-based escalation in one loop |
| Mutex over spinlock in kernel module | `get_mm_rss()` is slow; running under a workqueue (process context) allows sleeping locks |
| Deferred CMD_RUN response | CLI exit code matches container exit code without a separate wait mechanism |
| STOPPING state | Cleanly separates "stop requested" from "running" without an extra boolean field |
| Bounded buffer capacity 16 | Small enough to be educational; push-back pressure prevents unbounded memory use |
| Two-tier memory limits | Soft gives visibility; hard gives enforcement. Avoids kill/restart oscillation at the boundary |

---

## 🧹 Resource Cleanup

1. **Zombies** — `SIGCHLD` via self-pipe triggers `waitpid(-1, WNOHANG)` loop.
2. **Threads** — Logger thread drains the bounded buffer before exiting; `pthread_join()` waits.
3. **File descriptors** — Pipe write-end closed in parent after `clone()`; producer closes read-end when EOF; socket closed after each client interaction.
4. **Kernel memory** — `monitor_exit()` walks the list and `kfree()`s every entry.
5. **Container stacks** — Freed in `reap_children()` after `waitpid()` confirms the child stack is no longer in use.
6. **Mutex/CV** — Every `pthread_mutex_init` / `pthread_cond_init` has a matching destroy on all exit paths.

---

## 🔒 Safety Properties

| Property | Mechanism |
|----------|-----------|
| Async-signal-safety | Self-pipe — only `write()` called in signal handler |
| No data races on metadata | `metadata_lock` held for all container list reads/writes |
| No data races on log buffer | `log_buffer.mutex` + condition variables |
| No use-after-free (kernel) | RCU read lock + `get_task_struct` pin before dereferencing |
| No FD leaks on error paths | Every allocation/open covered by cleanup on each failure branch |

---

## ⚠️ Known Limitations

- **No network namespace** — containers share the host network stack.
- **No user namespace** — containers run as root (same UID as supervisor).
- **Log size** — `engine logs` returns at most 4 KB (tail). Use `--follow` for streaming.
- **Command arguments** — The `<command>` field (256 bytes) is split on spaces; quoting within the command is not supported.
- **Single supervisor** — One supervisor process manages all containers; no multi-host support.
- **Linux only** — Requires a native Linux VM; does not work on WSL or macOS.

---

## 📁 Repository Structure

```
Kernel-Dock/
├── README.md
├── project-guide.md            original assignment specification
├── .github/
│   └── workflows/
│       └── submission-smoke.yml  CI: compile + usage check
└── boilerplate/
    ├── Makefile                 all / release / debug / ci / clean
    ├── engine.c                 supervisor daemon + CLI client
    ├── monitor.c                kernel module (RSS tracking + enforcement)
    ├── monitor_ioctl.h          shared ioctl definitions
    ├── cpu_hog.c                CPU-bound workload generator
    ├── memory_hog.c             memory allocation workload
    ├── io_pulse.c               I/O-bound workload generator
    ├── environment-check.sh     preflight: OS, kernel headers, module test
    ├── test_experiments.sh      10-test suite with PASS/FAIL summary
    └── cleanup_verification.sh  post-run resource leak checker
```

---

## 📚 Key Learnings

- Linux namespaces and how the kernel isolates process trees
- `clone()` vs `fork()` — fine-grained sharing control
- Async-signal-safety and the self-pipe pattern
- Producer-consumer synchronization with condition variables
- Kernel module development: character devices, ioctl, workqueues
- Linux memory management: RSS, page tables, `get_mm_rss()`
- CFS scheduler: vruntime, weights, nice values, sleep fairness

---

## 🔮 Future Enhancements

- Network namespace per container (requires `CLONE_NEWNET` + bridge setup)
- User namespace for unprivileged operation
- Cgroup v2 integration for CPU/I/O limits (in addition to current RSS enforcement)
- Container image layering with bind-mount overlays
- Checkpoint/restore via CRIU integration

---

## 📄 License

Educational project. Not intended for production use.

---

## 🙏 Acknowledgments

- Course instructors for the project specification
- Alpine Linux for the minimal rootfs
- Linux kernel documentation
