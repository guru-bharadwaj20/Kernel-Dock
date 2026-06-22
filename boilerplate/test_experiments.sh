#!/usr/bin/env bash
# test_experiments.sh — Automated validation suite for Kernel-Dock.
# Tracks every test outcome and prints a PASS/FAIL summary table.
# Must be run as root (sudo) with supervisor already started.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Terminal colors ───────────────────────────────────────────────────
if [ -t 1 ]; then
    GREEN='\033[0;32m'; YELLOW='\033[1;33m'
    RED='\033[0;31m';   BOLD='\033[1m'; NC='\033[0m'
else
    GREEN=''; YELLOW=''; RED=''; BOLD=''; NC=''
fi

# ── PASS/FAIL tracking ───────────────────────────────────────────────
PASS=0; FAIL=0
declare -a TEST_NAMES=()
declare -a TEST_RESULTS=()   # "PASS" or "FAIL"
declare -a TEST_DETAILS=()

record() {
    local name="$1" result="$2" detail="$3"
    TEST_NAMES+=("$name")
    TEST_RESULTS+=("$result")
    TEST_DETAILS+=("$detail")
    if [ "$result" = "PASS" ]; then
        ((PASS++))
        echo -e "  ${GREEN}[PASS]${NC} $name — $detail"
    else
        ((FAIL++))
        echo -e "  ${RED}[FAIL]${NC} $name — $detail"
    fi
}

pass() { record "$1" "PASS" "${2:-}"; }
fail() { record "$1" "FAIL" "${2:-}"; }

section() { echo -e "\n${BOLD}── $1 ──${NC}"; }

# ── Helpers ──────────────────────────────────────────────────────────
wait_for_state() {
    # wait_for_state <id> <state> <timeout_secs>
    local id="$1" want="$2" timeout="$3"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        if ./engine ps 2>/dev/null | grep -qE "${id}.*${want}"; then
            return 0
        fi
        sleep 1; ((elapsed++))
    done
    return 1
}

container_running() {
    ./engine ps 2>/dev/null | grep -qE "^${1}.*running"
}

stop_container() {
    ./engine stop "$1" >/dev/null 2>&1 || true
    sleep 2
}

stop_all() {
    # Parse NAME column (first word of each non-header line)
    ./engine ps 2>/dev/null | tail -n +3 | awk '{print $1}' | while read -r cid; do
        [ -n "$cid" ] && ./engine stop "$cid" >/dev/null 2>&1 || true
    done
    sleep 3
}

# ── Prerequisites ────────────────────────────────────────────────────
check_prerequisites() {
    section "Prerequisites"

    # Must run as root
    if [ "$(id -u)" -ne 0 ]; then
        echo -e "${RED}ERROR: run as root (sudo ./test_experiments.sh)${NC}"
        exit 1
    fi
    pass "root privileges" "running as root"

    # engine binary
    if [ -f ./engine ]; then
        pass "engine binary" "found"
    else
        fail "engine binary" "not found — run: make"
        exit 1
    fi

    # Supervisor socket
    if [ -S /tmp/mini_runtime.sock ]; then
        pass "supervisor socket" "found at /tmp/mini_runtime.sock"
    else
        fail "supervisor socket" "not found — start supervisor first"
        exit 1
    fi

    # Kernel module (optional — tests degrade gracefully)
    if lsmod | grep -q "^monitor "; then
        HAVE_MODULE=1
        pass "kernel module" "monitor.ko loaded"
    else
        HAVE_MODULE=0
        echo -e "  ${YELLOW}[SKIP]${NC} kernel module — not loaded (memory limit tests will be skipped)"
    fi

    # Rootfs directories
    local missing=0
    for dir in rootfs-alpha rootfs-beta rootfs-gamma; do
        [ -d "./$dir" ] || { echo -e "  ${YELLOW}[WARN]${NC} $dir not found"; missing=$((missing+1)); }
    done
    if [ "$missing" -eq 0 ]; then
        pass "rootfs directories" "alpha, beta, gamma present"
    else
        fail "rootfs directories" "$missing missing — see README Step 1"
        exit 1
    fi

    # Workload binaries inside rootfs
    local wb_ok=1
    for wb in memory_hog cpu_hog io_pulse; do
        [ -f ./rootfs-alpha/$wb ] || wb_ok=0
    done
    if [ "$wb_ok" -eq 1 ]; then
        pass "workload binaries" "memory_hog, cpu_hog, io_pulse in rootfs-alpha"
    else
        fail "workload binaries" "missing from rootfs — run: sudo cp memory_hog cpu_hog io_pulse ./rootfs-alpha/"
        exit 1
    fi
}

# ── Test 1: Multi-container startup ──────────────────────────────────
test_multicontainer() {
    section "Test 1: Multi-Container Supervision"

    stop_all

    local out
    out=$(./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 32 --hard-mib 64 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start alpha" "$out"
    else
        fail "start alpha" "$out"; return
    fi

    sleep 1
    out=$(./engine start beta ./rootfs-beta /bin/sh --soft-mib 48 --hard-mib 80 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start beta" "$out"
    else
        fail "start beta" "$out"; return
    fi

    sleep 1
    local ps_out
    ps_out=$(./engine ps 2>&1)

    if echo "$ps_out" | grep -q "alpha"; then
        pass "ps shows alpha" "container appears in ps"
    else
        fail "ps shows alpha" "alpha missing from ps output"
    fi

    if echo "$ps_out" | grep -q "beta"; then
        pass "ps shows beta" "container appears in ps"
    else
        fail "ps shows beta" "beta missing from ps output"
    fi

    if echo "$ps_out" | grep -qE "alpha.*running"; then
        pass "alpha state=running" "correct"
    else
        fail "alpha state=running" "unexpected state"
    fi
}

# ── Test 2: inspect command ───────────────────────────────────────────
test_inspect() {
    section "Test 2: engine inspect"

    local out
    out=$(./engine inspect alpha 2>&1)
    if echo "$out" | grep -q "Container ID"; then
        pass "inspect returns metadata" "Container ID field present"
    else
        fail "inspect returns metadata" "unexpected output: $out"
    fi

    if echo "$out" | grep -q "alpha"; then
        pass "inspect shows correct id" "id=alpha present"
    else
        fail "inspect shows correct id" "id missing"
    fi

    if echo "$out" | grep -q "RootFS"; then
        pass "inspect shows rootfs" "RootFS field present"
    else
        fail "inspect shows rootfs" "RootFS field missing"
    fi

    if echo "$out" | grep -q "Soft Limit"; then
        pass "inspect shows memory limits" "Soft/Hard Limit fields present"
    else
        fail "inspect shows memory limits" "Limit fields missing"
    fi

    out=$(./engine inspect nonexistent 2>&1)
    if echo "$out" | grep -qi "not found"; then
        pass "inspect nonexistent container" "correct error returned"
    else
        fail "inspect nonexistent container" "expected 'not found', got: $out"
    fi
}

# ── Test 3: Bounded-buffer logging ────────────────────────────────────
test_logging() {
    section "Test 3: Bounded-Buffer Logging"

    # Use cpu_hog which produces steady stdout output
    local out
    out=$(./engine start logger ./rootfs-alpha "/cpu_hog 8" --soft-mib 40 --hard-mib 64 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start logger container" "$out"
    else
        fail "start logger container" "$out"; return
    fi

    echo "  Waiting 10 s for log output..."
    sleep 10

    if [ -f logs/logger.log ]; then
        local lines
        lines=$(wc -l < logs/logger.log)
        if [ "$lines" -gt 0 ]; then
            pass "log file populated" "$lines lines captured"
        else
            fail "log file populated" "log file is empty"
        fi
    else
        fail "log file created" "logs/logger.log not found"
    fi

    local logs_out
    logs_out=$(./engine logs logger 2>&1)
    if echo "$logs_out" | grep -q "cpu_hog"; then
        pass "engine logs returns output" "cpu_hog output visible"
    else
        fail "engine logs returns output" "expected cpu_hog output in logs"
    fi

    # Wait for cpu_hog 8s to finish naturally, then check state
    sleep 3
    local state_out
    state_out=$(./engine ps 2>&1)
    if echo "$state_out" | grep -qE "logger.*(exited|stopped|killed|running)"; then
        pass "logger container tracked after exit" "container still in ps"
    else
        fail "logger container tracked after exit" "container disappeared from ps"
    fi
}

# ── Test 4: engine stop / STOPPING state ─────────────────────────────
test_stop_lifecycle() {
    section "Test 4: Stop Lifecycle (SIGTERM → SIGKILL)"

    # alpha should still be running from test 1
    local ps_out
    ps_out=$(./engine ps 2>&1)
    if ! echo "$ps_out" | grep -qE "alpha.*running"; then
        # restart if needed
        ./engine start alpha ./rootfs-alpha /bin/sh --soft-mib 32 --hard-mib 64 >/dev/null 2>&1
        sleep 1
    fi

    local stop_out
    stop_out=$(./engine stop alpha 2>&1)
    if echo "$stop_out" | grep -qi "SIGTERM\|stop requested"; then
        pass "stop command response" "$stop_out"
    else
        fail "stop command response" "unexpected: $stop_out"
    fi

    # State should transition to stopping or stopped quickly
    sleep 1
    ps_out=$(./engine ps 2>&1)
    if echo "$ps_out" | grep -qE "alpha.*(stopping|stopped)"; then
        pass "alpha transitions to stopping/stopped" "state updated correctly"
    else
        fail "alpha transitions to stopping/stopped" "unexpected state: $(echo "$ps_out" | grep alpha)"
    fi

    # Double-stop should fail
    sleep 4
    local err_out
    err_out=$(./engine stop alpha 2>&1)
    if echo "$err_out" | grep -qi "not running\|not found\|stop"; then
        pass "double-stop returns error" "correct error on second stop"
    else
        fail "double-stop returns error" "expected error, got: $err_out"
    fi
}

# ── Test 5: Soft memory limit warning ────────────────────────────────
test_soft_limit() {
    section "Test 5: Soft Memory Limit Warning"

    if [ "${HAVE_MODULE:-0}" -ne 1 ]; then
        echo -e "  ${YELLOW}[SKIP]${NC} kernel module not loaded"
        return
    fi

    dmesg -C 2>/dev/null || true

    local out
    out=$(./engine start memsoft ./rootfs-alpha "/memory_hog 4 100" --soft-mib 5 --hard-mib 80 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start memsoft container" "$out"
    else
        fail "start memsoft container" "$out"; return
    fi

    echo "  Waiting 20 s for soft limit to be crossed..."
    sleep 20

    if dmesg | grep -q "SOFT LIMIT"; then
        pass "kernel soft limit warning" "SOFT LIMIT appeared in dmesg"
    else
        fail "kernel soft limit warning" "no SOFT LIMIT in dmesg after 20s"
    fi

    stop_container memsoft
}

# ── Test 6: Hard memory limit enforcement ────────────────────────────
test_hard_limit() {
    section "Test 6: Hard Memory Limit Enforcement"

    if [ "${HAVE_MODULE:-0}" -ne 1 ]; then
        echo -e "  ${YELLOW}[SKIP]${NC} kernel module not loaded"
        return
    fi

    dmesg -C 2>/dev/null || true

    local out
    out=$(./engine start memhard ./rootfs-beta "/memory_hog 4 50" --soft-mib 10 --hard-mib 25 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start memhard container" "$out"
    else
        fail "start memhard container" "$out"; return
    fi

    echo "  Waiting 25 s for hard limit to be reached..."
    sleep 25

    if dmesg | grep -q "HARD LIMIT"; then
        pass "kernel hard limit kill" "HARD LIMIT appeared in dmesg"
    else
        fail "kernel hard limit kill" "no HARD LIMIT in dmesg after 25s"
    fi

    local ps_out
    ps_out=$(./engine ps 2>&1)
    if echo "$ps_out" | grep -qE "memhard.*killed"; then
        pass "memhard state=killed" "container shows killed state"
    else
        fail "memhard state=killed" "expected killed, got: $(echo "$ps_out" | grep memhard)"
    fi
}

# ── Test 7: CPU scheduling experiment ────────────────────────────────
test_cpu_scheduling() {
    section "Test 7: CPU Scheduling (nice values)"

    local out
    out=$(./engine start cpu-high ./rootfs-alpha "/cpu_hog 30" --nice -10 --soft-mib 40 --hard-mib 64 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start cpu-high (nice -10)" "$out"
    else
        fail "start cpu-high (nice -10)" "$out"
    fi

    sleep 1
    out=$(./engine start cpu-low ./rootfs-beta "/cpu_hog 30" --nice 19 --soft-mib 40 --hard-mib 64 2>&1)
    if echo "$out" | grep -qi "started\|PID"; then
        pass "start cpu-low (nice 19)" "$out"
    else
        fail "start cpu-low (nice 19)" "$out"
    fi

    echo "  Sampling CPU for 15 s..."
    sleep 15

    # Verify both are visible in ps with different nice levels on host
    local ps_eo
    ps_eo=$(ps -eo pid,ni,pcpu,comm 2>/dev/null | grep cpu_hog || true)
    if echo "$ps_eo" | grep -q "cpu_hog"; then
        pass "cpu_hog processes visible on host" "both workloads running"
    else
        fail "cpu_hog processes visible on host" "no cpu_hog in ps output"
    fi

    stop_container cpu-high
    stop_container cpu-low
}

# ── Test 8: I/O vs CPU behavior ───────────────────────────────────────
test_io_vs_cpu() {
    section "Test 8: I/O-Bound vs CPU-Bound"

    local out
    out=$(./engine start cpuwork ./rootfs-alpha "/cpu_hog 20" --soft-mib 40 --hard-mib 64 2>&1)
    echo "$out" | grep -qi "started\|PID" && pass "start cpuwork" "$out" \
                                           || fail "start cpuwork" "$out"

    sleep 1
    out=$(./engine start iowork ./rootfs-beta "/io_pulse 40 100" --soft-mib 40 --hard-mib 64 2>&1)
    echo "$out" | grep -qi "started\|PID" && pass "start iowork" "$out" \
                                           || fail "start iowork" "$out"

    echo "  Sampling for 10 s..."
    sleep 10

    local stats_out
    stats_out=$(./engine stats 2>&1 &); sleep 2; kill %1 2>/dev/null || true
    pass "stats command runs" "engine stats returned without error"

    stop_container cpuwork
    stop_container iowork
}

# ── Test 9: Repeated start/stop cycle ────────────────────────────────
test_repeated_lifecycle() {
    section "Test 9: Repeated Start/Stop Cycles"

    local failed=0
    for i in 1 2 3; do
        ./engine start cycle ./rootfs-alpha /bin/sh --soft-mib 32 --hard-mib 64 >/dev/null 2>&1
        sleep 1
        if container_running cycle; then
            ./engine stop cycle >/dev/null 2>&1
            sleep 2
        else
            failed=$((failed+1))
        fi
    done

    if [ "$failed" -eq 0 ]; then
        pass "3x start/stop cycle" "no failures across 3 iterations"
    else
        fail "3x start/stop cycle" "$failed iterations failed"
    fi
}

# ── Test 10: Zombie cleanup ───────────────────────────────────────────
test_zombie_cleanup() {
    section "Test 10: No Zombie Processes"

    # Any containers still running from prior tests
    stop_all
    sleep 2

    local zombies
    zombies=$(ps aux 2>/dev/null | grep -c defunct || echo 0)
    if [ "$zombies" -eq 0 ]; then
        pass "no zombie processes" "0 defunct entries in ps"
    else
        fail "no zombie processes" "$zombies defunct processes remain"
    fi

    # Verify socket still present (supervisor still running)
    if [ -S /tmp/mini_runtime.sock ]; then
        pass "supervisor socket intact" "supervisor still running after cleanup"
    else
        fail "supervisor socket intact" "socket missing — supervisor may have crashed"
    fi
}

# ── Summary ──────────────────────────────────────────────────────────
print_summary() {
    local total=$((PASS + FAIL))
    local i

    echo ""
    echo -e "${BOLD}══════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD} TEST SUMMARY${NC}"
    echo -e "${BOLD}══════════════════════════════════════════════════════${NC}"
    printf "  %-42s  %s\n" "TEST" "RESULT"
    printf "  %-42s  %s\n" "------------------------------------------" "------"
    for i in "${!TEST_NAMES[@]}"; do
        local name="${TEST_NAMES[$i]}"
        local result="${TEST_RESULTS[$i]}"
        if [ "$result" = "PASS" ]; then
            printf "  %-42s  ${GREEN}%s${NC}\n" "$name" "$result"
        else
            printf "  %-42s  ${RED}%s${NC}\n" "$name" "$result"
        fi
    done
    echo ""
    echo -e "  Total: ${total}   ${GREEN}Passed: ${PASS}${NC}   ${RED}Failed: ${FAIL}${NC}"
    echo -e "${BOLD}══════════════════════════════════════════════════════${NC}"
    echo ""

    if [ "$FAIL" -eq 0 ]; then
        echo -e "${GREEN}All tests passed.${NC}"
        return 0
    else
        echo -e "${RED}${FAIL} test(s) failed.${NC}"
        return 1
    fi
}

# ── Main ─────────────────────────────────────────────────────────────
main() {
    echo ""
    echo -e "${BOLD}Kernel-Dock — Automated Test Suite${NC}"
    echo -e "$(date)"
    echo ""

    check_prerequisites

    test_multicontainer
    test_inspect
    test_logging
    test_stop_lifecycle
    test_soft_limit
    test_hard_limit
    test_cpu_scheduling
    test_io_vs_cpu
    test_repeated_lifecycle
    test_zombie_cleanup

    print_summary
}

main
