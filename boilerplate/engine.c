#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ── Constants ──────────────────────────────────────────────────────── */

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)
#define STOP_GRACE_SECONDS  5

/* ANSI escape sequences — emitted only when stdout is a TTY. */
#define ANSI_RESET   "\033[0m"
#define ANSI_BOLD    "\033[1m"
#define ANSI_DIM     "\033[2m"
#define ANSI_GREEN   "\033[32m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_RED     "\033[31m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_CLEAR   "\033[2J\033[H"

/* ── Types ──────────────────────────────────────────────────────────── */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP,
    CMD_INSPECT,
    CMD_STATS
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0, /* created, not yet scheduled */
    CONTAINER_RUNNING,
    CONTAINER_STOPPING,     /* SIGTERM sent, grace period active */
    CONTAINER_STOPPED,      /* exited after stop request */
    CONTAINER_KILLED,       /* killed by hard memory limit or SIGKILL */
    CONTAINER_EXITED        /* natural exit */
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    time_t stopped_at;      /* set when container leaves RUNNING/STOPPING */
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int stop_requested;
    time_t stop_requested_at;
    int run_client_fd;      /* -1 when no CMD_RUN client is waiting */
    char *child_stack;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* ── Globals ────────────────────────────────────────────────────────── */

static supervisor_ctx_t global_ctx;

/* Self-pipe: signal handlers write the signal number here; the main loop
 * reads and dispatches where it is safe to lock, call ioctl, etc. */
static int g_signal_pipe[2] = {-1, -1};

/* Client-side globals for CMD_RUN SIGINT/SIGTERM forwarding. */
static volatile sig_atomic_t run_signal_pending;
static char run_signal_container_id[CONTAINER_ID_LEN];

/* Set to 1 when stdout is a TTY so ANSI sequences are safe to emit. */
static int g_use_color = 0;

/* ── ANSI color helper ──────────────────────────────────────────────── */

/* Return the ANSI escape code when color is enabled, else empty string. */
static const char *col(const char *code)
{
    return g_use_color ? code : "";
}

/* Return a colorized state label for terminal output. */
static const char *state_colored(container_state_t state)
{
    if (!g_use_color) return "";
    switch (state) {
    case CONTAINER_RUNNING:  return ANSI_GREEN;
    case CONTAINER_STOPPING: return ANSI_YELLOW;
    case CONTAINER_KILLED:   return ANSI_RED;
    default:                 return ANSI_DIM;
    }
}

/* ── Async-signal-safe signal handler ──────────────────────────────── */

static void unified_signal_handler(int sig)
{
    int saved_errno = errno;
    unsigned char s = (unsigned char)sig;
    (void)write(g_signal_pipe[1], &s, 1);
    errno = saved_errno;
}

/* ── Usage ──────────────────────────────────────────────────────────── */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start   <id> <rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run     <id> <rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s inspect <id>\n"
            "  %s stats\n"
            "  %s logs    <id> [--follow]\n"
            "  %s stop    <id>\n",
            prog, prog, prog, prog, prog, prog, prog, prog);
}

/* ── Argument parsing ───────────────────────────────────────────────── */

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

/* ── State helpers ──────────────────────────────────────────────────── */

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:  return "starting";
    case CONTAINER_RUNNING:   return "running";
    case CONTAINER_STOPPING:  return "stopping";
    case CONTAINER_STOPPED:   return "stopped";
    case CONTAINER_KILLED:    return "killed";
    case CONTAINER_EXITED:    return "exited";
    default:                  return "unknown";
    }
}

/* Format seconds as hh:mm:ss into buf. */
static void fmt_uptime(long secs, char *buf, size_t bufsz)
{
    if (secs < 0) secs = 0;
    snprintf(buf, bufsz, "%ldh%02ldm%02lds",
             secs / 3600, (secs % 3600) / 60, secs % 60);
}

/* ── RSS helper ─────────────────────────────────────────────────────── */

/* Read Resident Set Size (in kB) from /proc/<pid>/status.
 * Returns -1 if the process is unavailable or the file cannot be read. */
static long read_proc_rss_kb(pid_t pid)
{
    char path[64];
    char line[256];
    long rss = -1;
    FILE *f;

    snprintf(path, sizeof(path), "/proc/%d/status", (int)pid);
    f = fopen(path, "r");
    if (!f)
        return -1;

    while (fgets(line, sizeof(line), f)) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            sscanf(line + 6, " %ld", &rss);
            break;
        }
    }
    fclose(f);
    return rss; /* kB */
}

/* ── Bounded buffer ─────────────────────────────────────────────────── */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    memcpy(&buffer->items[buffer->tail], item, sizeof(log_item_t));
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    if (buffer->shutting_down && buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }
    memcpy(item, &buffer->items[buffer->head], sizeof(log_item_t));
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ── Logging threads ────────────────────────────────────────────────── */

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *curr = ctx->containers;
        const char *log_path = NULL;
        while (curr) {
            if (strcmp(curr->id, item.container_id) == 0) {
                log_path = curr->log_path;
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path && log_path[0] != '\0') {
            FILE *f = fopen(log_path, "a");
            if (f) {
                fwrite(item.data, 1, item.length, f);
                fclose(f);
            }
        }
    }
    return NULL;
}

typedef struct {
    supervisor_ctx_t *ctx;
    int read_fd;
    char container_id[CONTAINER_ID_LEN];
} producer_args_t;

static void *producer_thread(void *arg)
{
    producer_args_t *args = (producer_args_t *)arg;
    char buffer[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(args->read_fd, buffer, sizeof(buffer))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, args->container_id, CONTAINER_ID_LEN - 1);
        item.length = (size_t)n;
        memcpy(item.data, buffer, (size_t)n);
        if (bounded_buffer_push(&args->ctx->log_buffer, &item) != 0)
            break;
    }

    close(args->read_fd);
    free(args);
    return NULL;
}

/* ── Container child function ───────────────────────────────────────── */

/* Configure namespaces and rootfs, then exec the requested command.
 * The command string is split on spaces so arguments work (e.g. "/cpu_hog 30"). */
static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    char cmd_copy[CHILD_COMMAND_LEN];
    char *exec_argv[64];
    int exec_argc = 0;
    char *tok;

    sethostname(cfg->id, strlen(cfg->id));

    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc");
        return 1;
    }
    if (cfg->log_write_fd >= 0) {
        if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
            dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
            perror("dup2");
            return 1;
        }
        close(cfg->log_write_fd);
    }
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    strncpy(cmd_copy, cfg->command, CHILD_COMMAND_LEN - 1);
    cmd_copy[CHILD_COMMAND_LEN - 1] = '\0';
    tok = strtok(cmd_copy, " ");
    while (tok && exec_argc < 63) {
        exec_argv[exec_argc++] = tok;
        tok = strtok(NULL, " ");
    }
    exec_argv[exec_argc] = NULL;

    if (exec_argc == 0) {
        fprintf(stderr, "engine: empty command\n");
        return 1;
    }

    execvp(exec_argv[0], exec_argv);
    fprintf(stderr, "engine: exec '%s': %s\n", exec_argv[0], strerror(errno));
    return 1;
}

/* ── Kernel monitor integration ─────────────────────────────────────── */

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    return ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0 ? -1 : 0;
}

static int unregister_from_monitor(int monitor_fd,
                                   const char *container_id,
                                   pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    return ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0 ? -1 : 0;
}

/* ── Container list operations ──────────────────────────────────────── */
/* All list functions require the caller to hold ctx->metadata_lock.    */

static void add_container(supervisor_ctx_t *ctx, container_record_t *rec)
{
    rec->next = ctx->containers;
    ctx->containers = rec;
}

static container_record_t *find_container_by_id(supervisor_ctx_t *ctx,
                                                 const char *id)
{
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (strcmp(curr->id, id) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx,
                                                  pid_t pid)
{
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (curr->host_pid == pid)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

/* ── Container lifecycle ────────────────────────────────────────────── */

/* Spawn a container process, track metadata, and wire up log capture. */
static int create_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    int pipefd[2];

    if (pipe(pipefd) != 0) {
        perror("pipe");
        return -1;
    }

    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
    memset(cfg, 0, sizeof(*cfg));
    cfg->log_write_fd = -1;
    strncpy(cfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(cfg->command, req->command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        free(cfg);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    pid_t pid = clone(child_fn,
                      stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      cfg);
    if (pid < 0) {
        perror("clone");
        free(cfg);
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    /* Parent no longer writes to the pipe. */
    close(pipefd[1]);

    container_record_t *rec = malloc(sizeof(container_record_t));
    if (!rec) {
        close(pipefd[0]);
        kill(pid, SIGKILL);
        waitpid(pid, NULL, 0);
        free(stack);
        free(cfg);
        return -1;
    }

    memset(rec, 0, sizeof(*rec));
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(rec->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(rec->command, req->command, CHILD_COMMAND_LEN - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->stopped_at = 0;
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->stop_requested = 0;
    rec->stop_requested_at = 0;
    rec->run_client_fd = -1;
    rec->child_stack = stack;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    add_container(ctx, rec);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0) {
        if (register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                                  req->soft_limit_bytes,
                                  req->hard_limit_bytes) < 0) {
            /* Non-fatal: supervisor continues without kernel enforcement. */
            fprintf(stderr,
                    "Warning: failed to register %s with kernel monitor: %s\n",
                    req->container_id, strerror(errno));
        }
    }

    producer_args_t *prod_args = malloc(sizeof(producer_args_t));
    if (prod_args) {
        prod_args->ctx = ctx;
        prod_args->read_fd = pipefd[0];
        strncpy(prod_args->container_id, req->container_id, CONTAINER_ID_LEN - 1);

        pthread_t prod_thread;
        if (pthread_create(&prod_thread, NULL, producer_thread, prod_args) != 0) {
            /* Failed to start producer — close fd to avoid leak. */
            close(pipefd[0]);
            free(prod_args);
        } else {
            pthread_detach(prod_thread);
        }
    } else {
        close(pipefd[0]);
    }

    free(cfg);
    return pid;
}

/* Reap exited children and update container metadata.
 * Called from the main event loop — safe to lock, call ioctl, etc. */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);

        container_record_t *rec = find_container_by_pid(ctx, pid);
        if (rec) {
            rec->stopped_at = time(NULL);

            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->state = rec->stop_requested ? CONTAINER_STOPPED
                                                  : CONTAINER_EXITED;
            } else if (WIFSIGNALED(status)) {
                rec->exit_signal = WTERMSIG(status);
                if (rec->stop_requested) {
                    rec->state = CONTAINER_STOPPED;
                } else if (rec->exit_signal == SIGKILL) {
                    rec->state = CONTAINER_KILLED;
                } else {
                    rec->state = CONTAINER_EXITED;
                }
            }

            if (rec->child_stack) {
                free(rec->child_stack);
                rec->child_stack = NULL;
            }

            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, rec->id, pid);

            /* Notify any attached CMD_RUN client with the exit status. */
            if (rec->run_client_fd >= 0) {
                control_response_t run_resp;
                memset(&run_resp, 0, sizeof(run_resp));
                if (WIFEXITED(status)) {
                    run_resp.status = WEXITSTATUS(status);
                    snprintf(run_resp.message, CONTROL_MESSAGE_LEN,
                             "Container %s exited with code %d",
                             rec->id, WEXITSTATUS(status));
                } else if (WIFSIGNALED(status)) {
                    run_resp.status = 128 + WTERMSIG(status);
                    snprintf(run_resp.message, CONTROL_MESSAGE_LEN,
                             "Container %s killed by signal %d",
                             rec->id, WTERMSIG(status));
                }
                (void)write(rec->run_client_fd, &run_resp, sizeof(run_resp));
                close(rec->run_client_fd);
                rec->run_client_fd = -1;
            }
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* Escalate pending stop requests from SIGTERM to SIGKILL after timeout. */
static void check_stop_escalation(supervisor_ctx_t *ctx)
{
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (curr->state == CONTAINER_STOPPING &&
            curr->stop_requested_at > 0 &&
            (now - curr->stop_requested_at) >= STOP_GRACE_SECONDS) {
            kill(curr->host_pid, SIGKILL);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

/* ── Command handlers ───────────────────────────────────────────────── */

static void handle_cmd_start(supervisor_ctx_t *ctx,
                              const control_request_t *req,
                              control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Container '%s' already exists", req->container_id);
        return;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    int pid = create_container(ctx, req);
    if (pid < 0) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Failed to create container '%s'", req->container_id);
        return;
    }

    resp->status = 0;
    snprintf(resp->message, CONTROL_MESSAGE_LEN,
             "Container '%s' started (PID %d)", req->container_id, pid);
}

/* Handle CMD_RUN: start container, defer response until exit.
 * Returns 1 to send immediately (error), 0 to defer (success). */
static int handle_cmd_run(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           control_response_t *resp,
                           int client_fd)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Container '%s' already exists", req->container_id);
        return 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    int pid = create_container(ctx, req);
    if (pid < 0) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Failed to create container '%s'", req->container_id);
        return 1;
    }

    /* Store the client FD in the record so reap_children() can respond. */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (rec)
        rec->run_client_fd = client_fd;
    pthread_mutex_unlock(&ctx->metadata_lock);

    return 0; /* response deferred */
}

/* Build an aligned table of all known containers with live RSS. */
static void handle_cmd_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    int off = 0;
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *curr = ctx->containers;
    if (!curr) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "No containers.\n");
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 0;
        return;
    }

    /* Header */
    off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                    "%-14s %-7s %-10s %-10s %-8s %-8s %-8s %s\n",
                    "NAME", "PID", "STATE", "UPTIME",
                    "RSS", "SOFT", "HARD", "COMMAND");
    off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                    "%-14s %-7s %-10s %-10s %-8s %-8s %-8s %s\n",
                    "──────────────", "───────", "──────────",
                    "──────────", "────────", "────────",
                    "────────", "───────────────────────────");

    while (curr && off < CONTROL_MESSAGE_LEN - 256) {
        char uptime_buf[32] = "-";
        char rss_buf[16]    = "-";

        if (curr->state == CONTAINER_RUNNING ||
            curr->state == CONTAINER_STOPPING) {
            long up = (long)(now - curr->started_at);
            fmt_uptime(up, uptime_buf, sizeof(uptime_buf));

            long rss_kb = read_proc_rss_kb(curr->host_pid);
            if (rss_kb >= 0)
                snprintf(rss_buf, sizeof(rss_buf), "%ld MB", rss_kb / 1024);
        }

        /* Truncate command to fit column. */
        char cmd_short[40];
        strncpy(cmd_short, curr->command, sizeof(cmd_short) - 1);
        cmd_short[sizeof(cmd_short) - 1] = '\0';

        off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                        "%-14s %-7d %-10s %-10s %-8s %-8lu %-8lu %s",
                        curr->id,
                        (int)curr->host_pid,
                        state_to_string(curr->state),
                        uptime_buf,
                        rss_buf,
                        curr->soft_limit_bytes >> 20,
                        curr->hard_limit_bytes >> 20,
                        cmd_short);

        /* Append exit info for terminated containers. */
        if (curr->state == CONTAINER_EXITED) {
            off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                            "  (exit %d)", curr->exit_code);
        } else if (curr->state == CONTAINER_KILLED) {
            off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                            "  (sig %d)", curr->exit_signal);
        } else if (curr->state == CONTAINER_STOPPED) {
            if (curr->exit_signal > 0)
                off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                                "  (sig %d)", curr->exit_signal);
            else
                off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                                "  (exit %d)", curr->exit_code);
        }

        off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off, "\n");
        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = 0;
}

/* Return detailed metadata and live RSS for a single container. */
static void handle_cmd_inspect(supervisor_ctx_t *ctx,
                                const control_request_t *req,
                                control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Container '%s' not found", req->container_id);
        return;
    }

    /* Snapshot fields under lock. */
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    char log_path[PATH_MAX];
    pid_t host_pid    = rec->host_pid;
    time_t started_at = rec->started_at;
    time_t stopped_at = rec->stopped_at;
    container_state_t state = rec->state;
    unsigned long soft = rec->soft_limit_bytes;
    unsigned long hard = rec->hard_limit_bytes;
    int exit_code     = rec->exit_code;
    int exit_signal   = rec->exit_signal;

    strncpy(id,       rec->id,       sizeof(id) - 1);       id[sizeof(id)-1] = '\0';
    strncpy(rootfs,   rec->rootfs,   sizeof(rootfs) - 1);   rootfs[sizeof(rootfs)-1] = '\0';
    strncpy(command,  rec->command,  sizeof(command) - 1);  command[sizeof(command)-1] = '\0';
    strncpy(log_path, rec->log_path, sizeof(log_path) - 1); log_path[sizeof(log_path)-1] = '\0';

    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Compute live RSS and uptime. */
    long rss_kb = -1;
    if (state == CONTAINER_RUNNING || state == CONTAINER_STOPPING)
        rss_kb = read_proc_rss_kb(host_pid);

    time_t now = time(NULL);
    char uptime_buf[32] = "N/A";
    char started_str[64], stopped_str[64];
    struct tm *tm_info;

    tm_info = localtime(&started_at);
    strftime(started_str, sizeof(started_str), "%Y-%m-%d %H:%M:%S", tm_info);

    if (stopped_at > 0) {
        tm_info = localtime(&stopped_at);
        strftime(stopped_str, sizeof(stopped_str), "%Y-%m-%d %H:%M:%S", tm_info);
    } else {
        snprintf(stopped_str, sizeof(stopped_str), "N/A");
    }

    if (state == CONTAINER_RUNNING || state == CONTAINER_STOPPING) {
        long up = (long)(now - started_at);
        fmt_uptime(up, uptime_buf, sizeof(uptime_buf));
    }

    char rss_str[32];
    if (rss_kb >= 0)
        snprintf(rss_str, sizeof(rss_str), "%ld MiB", rss_kb / 1024);
    else
        snprintf(rss_str, sizeof(rss_str), "N/A");

    char exit_str[32];
    if (state == CONTAINER_EXITED || state == CONTAINER_STOPPED)
        snprintf(exit_str, sizeof(exit_str),
                 exit_signal > 0 ? "signal %d" : "%d",
                 exit_signal > 0 ? exit_signal : exit_code);
    else
        snprintf(exit_str, sizeof(exit_str), "N/A");

    snprintf(resp->message, CONTROL_MESSAGE_LEN,
             "Container ID  : %s\n"
             "PID           : %d\n"
             "Parent PID    : %d\n"
             "State         : %s\n"
             "Start Time    : %s\n"
             "Stop Time     : %s\n"
             "Uptime        : %s\n"
             "RootFS        : %s\n"
             "Hostname      : %s\n"
             "Command       : %s\n"
             "RSS           : %s\n"
             "Soft Limit    : %lu MiB\n"
             "Hard Limit    : %lu MiB\n"
             "Exit Status   : %s\n"
             "Log File      : %s\n",
             id,
             (int)host_pid,
             (int)getpid(),
             state_to_string(state),
             started_str,
             stopped_str,
             uptime_buf,
             rootfs,
             id,     /* hostname = container id set via sethostname */
             command,
             rss_str,
             soft >> 20,
             hard >> 20,
             exit_str,
             log_path);

    resp->status = 0;
}

/* Build a live stats snapshot for all containers. */
static void handle_cmd_stats(supervisor_ctx_t *ctx, control_response_t *resp)
{
    int off = 0;
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *curr = ctx->containers;
    if (!curr) {
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "No containers running.\n");
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = 0;
        return;
    }

    off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                    "%-14s %-7s %-8s %-8s %-8s %-10s\n",
                    "NAME", "PID", "RSS", "SOFT", "HARD", "STATUS");
    off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                    "%-14s %-7s %-8s %-8s %-8s %-10s\n",
                    "──────────────", "───────", "────────",
                    "────────", "────────", "──────────");

    while (curr && off < CONTROL_MESSAGE_LEN - 128) {
        char rss_buf[16]    = "-";
        char status_buf[24] = "";
        char uptime_buf[24] = "";

        long rss_kb = -1;
        if (curr->state == CONTAINER_RUNNING ||
            curr->state == CONTAINER_STOPPING) {
            rss_kb = read_proc_rss_kb(curr->host_pid);
            if (rss_kb >= 0)
                snprintf(rss_buf, sizeof(rss_buf), "%ld MB", rss_kb / 1024);

            long up = (long)(now - curr->started_at);
            fmt_uptime(up, uptime_buf, sizeof(uptime_buf));
        }

        /* Derive status label with warning when RSS exceeds soft limit. */
        if (curr->state == CONTAINER_RUNNING) {
            if (rss_kb >= 0 &&
                (unsigned long)(rss_kb * 1024) > curr->soft_limit_bytes)
                snprintf(status_buf, sizeof(status_buf), "WARNING");
            else
                snprintf(status_buf, sizeof(status_buf), "RUNNING");
        } else {
            const char *s = state_to_string(curr->state);
            /* uppercase */
            size_t k;
            for (k = 0; s[k] && k < sizeof(status_buf) - 1; k++)
                status_buf[k] = (char)(s[k] >= 'a' && s[k] <= 'z'
                                       ? s[k] - 32 : s[k]);
            status_buf[k] = '\0';
        }

        off += snprintf(resp->message + off, CONTROL_MESSAGE_LEN - off,
                        "%-14s %-7d %-8s %-8lu %-8lu %-10s  %s\n",
                        curr->id,
                        (int)curr->host_pid,
                        rss_buf,
                        curr->soft_limit_bytes >> 20,
                        curr->hard_limit_bytes >> 20,
                        status_buf,
                        uptime_buf);

        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = 0;
}

static void handle_cmd_stop(supervisor_ctx_t *ctx,
                             const control_request_t *req,
                             control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Container '%s' not found", req->container_id);
        return;
    }
    if (rec->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Container '%s' is not running (state: %s)",
                 req->container_id, state_to_string(rec->state));
        return;
    }

    rec->stop_requested = 1;
    rec->stop_requested_at = time(NULL);
    rec->state = CONTAINER_STOPPING;
    kill(rec->host_pid, SIGTERM);

    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    snprintf(resp->message, CONTROL_MESSAGE_LEN,
             "Container '%s': SIGTERM sent, SIGKILL in %ds if needed",
             req->container_id, STOP_GRACE_SECONDS);
}

/* Return the most-recent captured log output for a container.
 * For files larger than the response buffer, the tail is returned. */
static void handle_cmd_logs(supervisor_ctx_t *ctx,
                             const control_request_t *req,
                             control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "Container '%s' not found", req->container_id);
        return;
    }
    char log_path[PATH_MAX];
    strncpy(log_path, rec->log_path, PATH_MAX - 1);
    log_path[PATH_MAX - 1] = '\0';
    pthread_mutex_unlock(&ctx->metadata_lock);

    FILE *f = fopen(log_path, "r");
    if (!f) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                 "No logs for '%s' (log file not found)", req->container_id);
        return;
    }

    /* Tail semantics: show the last CONTROL_MESSAGE_LEN-1 bytes. */
    if (fseek(f, 0, SEEK_END) == 0) {
        long file_sz   = ftell(f);
        long max_bytes = (long)(CONTROL_MESSAGE_LEN - 1);
        if (file_sz > max_bytes)
            fseek(f, -max_bytes, SEEK_END);
        else
            rewind(f);
    }

    size_t n = fread(resp->message, 1, CONTROL_MESSAGE_LEN - 1, f);
    resp->message[n] = '\0';
    fclose(f);
    resp->status = 0;
}

/* ── Signal-pipe drain ──────────────────────────────────────────────── */

static void drain_signal_pipe(supervisor_ctx_t *ctx)
{
    unsigned char sig;
    while (read(g_signal_pipe[0], &sig, 1) > 0) {
        if (sig == SIGCHLD) {
            reap_children(ctx);
        } else if (sig == SIGINT || sig == SIGTERM) {
            printf("\nReceived signal %d, initiating shutdown...\n", (int)sig);
            ctx->should_stop = 1;
        }
    }
}

/* ── Client connection handler ──────────────────────────────────────── */

static void handle_client_connection(supervisor_ctx_t *ctx)
{
    int client_fd = accept(ctx->server_fd, NULL, NULL);
    if (client_fd < 0)
        return;

    control_request_t req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    ssize_t n = read(client_fd, &req, sizeof(req));
    if (n != (ssize_t)sizeof(req)) {
        close(client_fd);
        return;
    }

    int should_respond = 1;

    switch (req.kind) {
    case CMD_START:
        handle_cmd_start(ctx, &req, &resp);
        break;
    case CMD_RUN:
        should_respond = handle_cmd_run(ctx, &req, &resp, client_fd);
        break;
    case CMD_PS:
        handle_cmd_ps(ctx, &resp);
        break;
    case CMD_STOP:
        handle_cmd_stop(ctx, &req, &resp);
        break;
    case CMD_LOGS:
        handle_cmd_logs(ctx, &req, &resp);
        break;
    case CMD_INSPECT:
        handle_cmd_inspect(ctx, &req, &resp);
        break;
    case CMD_STATS:
        handle_cmd_stats(ctx, &resp);
        break;
    default:
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN,
                 "Unknown command kind %d", (int)req.kind);
        break;
    }

    if (should_respond) {
        (void)write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }
    /* For a deferred CMD_RUN, client_fd lives inside the container record. */
}

/* ── Supervisor main ────────────────────────────────────────────────── */

static int run_supervisor(const char *rootfs)
{
    (void)rootfs; /* base rootfs documented but not used by supervisor */
    struct sigaction sa_chld, sa_shutdown;

    memset(&global_ctx, 0, sizeof(global_ctx));
    global_ctx.server_fd  = -1;
    global_ctx.monitor_fd = -1;

    if (pthread_mutex_init(&global_ctx.metadata_lock, NULL) != 0) {
        perror("pthread_mutex_init");
        return 1;
    }

    if (mkdir(LOG_DIR, 0755) < 0 && errno != EEXIST) {
        fprintf(stderr, "Warning: could not create '%s': %s\n",
                LOG_DIR, strerror(errno));
    }

    if (bounded_buffer_init(&global_ctx.log_buffer) != 0) {
        fprintf(stderr, "Failed to initialize log buffer\n");
        pthread_mutex_destroy(&global_ctx.metadata_lock);
        return 1;
    }

    global_ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (global_ctx.monitor_fd < 0)
        fprintf(stderr, "Warning: /dev/container_monitor unavailable — "
                        "kernel memory enforcement disabled\n");

    if (pipe(g_signal_pipe) != 0) {
        perror("pipe(signal)");
        bounded_buffer_destroy(&global_ctx.log_buffer);
        pthread_mutex_destroy(&global_ctx.metadata_lock);
        return 1;
    }
    fcntl(g_signal_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(g_signal_pipe[1], F_SETFL, O_NONBLOCK);

    /* All signal handlers funnel through the self-pipe. */
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = unified_signal_handler;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_shutdown, 0, sizeof(sa_shutdown));
    sa_shutdown.sa_handler = unified_signal_handler;
    sa_shutdown.sa_flags   = SA_RESTART;
    sigaction(SIGINT,  &sa_shutdown, NULL);
    sigaction(SIGTERM, &sa_shutdown, NULL);

    if (pthread_create(&global_ctx.logger_thread, NULL,
                       logging_thread, &global_ctx) != 0) {
        fprintf(stderr, "Failed to create logger thread\n");
        bounded_buffer_destroy(&global_ctx.log_buffer);
        pthread_mutex_destroy(&global_ctx.metadata_lock);
        return 1;
    }

    global_ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (global_ctx.server_fd < 0) {
        perror("socket");
        bounded_buffer_begin_shutdown(&global_ctx.log_buffer);
        pthread_join(global_ctx.logger_thread, NULL);
        bounded_buffer_destroy(&global_ctx.log_buffer);
        pthread_mutex_destroy(&global_ctx.metadata_lock);
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);

    if (bind(global_ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(global_ctx.server_fd);
        bounded_buffer_begin_shutdown(&global_ctx.log_buffer);
        pthread_join(global_ctx.logger_thread, NULL);
        bounded_buffer_destroy(&global_ctx.log_buffer);
        pthread_mutex_destroy(&global_ctx.metadata_lock);
        return 1;
    }

    if (listen(global_ctx.server_fd, 10) < 0) {
        perror("listen");
        close(global_ctx.server_fd);
        unlink(CONTROL_PATH);
        bounded_buffer_begin_shutdown(&global_ctx.log_buffer);
        pthread_join(global_ctx.logger_thread, NULL);
        bounded_buffer_destroy(&global_ctx.log_buffer);
        pthread_mutex_destroy(&global_ctx.metadata_lock);
        return 1;
    }

    printf("Supervisor started (PID %d). Listening on %s\n",
           (int)getpid(), CONTROL_PATH);
    printf("Kernel monitor: %s\n",
           global_ctx.monitor_fd >= 0 ? "enabled" : "disabled (no module)");
    printf("Ready.\n");

    /* ── Event loop ─────────────────────────────────────────────────── */

    struct pollfd fds[2];
    fds[0].fd = global_ctx.server_fd;
    fds[0].events = POLLIN;
    fds[1].fd = g_signal_pipe[0];
    fds[1].events = POLLIN;

    while (!global_ctx.should_stop) {
        int ret = poll(fds, 2, 1000); /* 1 s timeout for escalation checks */

        if (ret < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }

        if (fds[1].revents & POLLIN)
            drain_signal_pipe(&global_ctx);

        check_stop_escalation(&global_ctx);

        if (!global_ctx.should_stop && (fds[0].revents & POLLIN))
            handle_client_connection(&global_ctx);
    }

    /* ── Orderly shutdown ───────────────────────────────────────────── */

    printf("\nShutting down supervisor...\n");

    pthread_mutex_lock(&global_ctx.metadata_lock);
    container_record_t *curr = global_ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING && !curr->stop_requested) {
            curr->stop_requested    = 1;
            curr->stop_requested_at = time(NULL);
            curr->state             = CONTAINER_STOPPING;
            kill(curr->host_pid, SIGTERM);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&global_ctx.metadata_lock);

    /* Wait for graceful exits. */
    for (int i = 0; i < STOP_GRACE_SECONDS; i++) {
        sleep(1);
        reap_children(&global_ctx);
        int all_done = 1;
        pthread_mutex_lock(&global_ctx.metadata_lock);
        curr = global_ctx.containers;
        while (curr) {
            if (curr->state == CONTAINER_RUNNING ||
                curr->state == CONTAINER_STOPPING) {
                all_done = 0;
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&global_ctx.metadata_lock);
        if (all_done)
            break;
    }

    /* Force-kill any survivors. */
    pthread_mutex_lock(&global_ctx.metadata_lock);
    curr = global_ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING ||
            curr->state == CONTAINER_STOPPING)
            kill(curr->host_pid, SIGKILL);
        curr = curr->next;
    }
    pthread_mutex_unlock(&global_ctx.metadata_lock);

    usleep(500000);
    reap_children(&global_ctx);

    /* Notify pending CMD_RUN clients. */
    pthread_mutex_lock(&global_ctx.metadata_lock);
    curr = global_ctx.containers;
    while (curr) {
        if (curr->run_client_fd >= 0) {
            control_response_t sr;
            memset(&sr, 0, sizeof(sr));
            sr.status = -1;
            snprintf(sr.message, CONTROL_MESSAGE_LEN, "Supervisor shutting down");
            (void)write(curr->run_client_fd, &sr, sizeof(sr));
            close(curr->run_client_fd);
            curr->run_client_fd = -1;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&global_ctx.metadata_lock);

    bounded_buffer_begin_shutdown(&global_ctx.log_buffer);
    pthread_join(global_ctx.logger_thread, NULL);
    bounded_buffer_destroy(&global_ctx.log_buffer);

    if (global_ctx.monitor_fd >= 0)
        close(global_ctx.monitor_fd);
    close(global_ctx.server_fd);
    unlink(CONTROL_PATH);
    if (g_signal_pipe[0] >= 0) close(g_signal_pipe[0]);
    if (g_signal_pipe[1] >= 0) close(g_signal_pipe[1]);

    curr = global_ctx.containers;
    while (curr) {
        container_record_t *next = curr->next;
        if (curr->child_stack)
            free(curr->child_stack);
        free(curr);
        curr = next;
    }
    global_ctx.containers = NULL;

    pthread_mutex_destroy(&global_ctx.metadata_lock);
    printf("Supervisor shutdown complete.\n");
    return 0;
}

/* ── Client-side helpers ────────────────────────────────────────────── */

/* Send req, read the full response into resp_out.  Returns 0 on success. */
static int send_control_request_raw(const control_request_t *req,
                                    control_response_t *resp_out)
{
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "connect: %s  (is supervisor running?)\n",
                strerror(errno));
        close(sock_fd);
        return -1;
    }
    if (write(sock_fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(sock_fd);
        return -1;
    }

    memset(resp_out, 0, sizeof(*resp_out));
    {
        char *buf   = (char *)resp_out;
        size_t total = 0;
        while (total < sizeof(*resp_out)) {
            ssize_t n = read(sock_fd, buf + total, sizeof(*resp_out) - total);
            if (n > 0) {
                total += (size_t)n;
                continue;
            }
            if (n < 0 && errno == EINTR) {
                /* For CMD_RUN: forward pending SIGINT/SIGTERM as stop. */
                if (req->kind == CMD_RUN &&
                    run_signal_pending &&
                    run_signal_container_id[0] != '\0') {
                    control_request_t stop_req;
                    control_response_t stop_resp;
                    memset(&stop_req, 0, sizeof(stop_req));
                    stop_req.kind = CMD_STOP;
                    strncpy(stop_req.container_id,
                            run_signal_container_id,
                            sizeof(stop_req.container_id) - 1);
                    run_signal_pending = 0;
                    send_control_request_raw(&stop_req, &stop_resp);
                }
                continue;
            }
            if (n == 0)
                fprintf(stderr, "Supervisor closed connection unexpectedly\n");
            else
                perror("read");
            close(sock_fd);
            return -1;
        }
    }

    close(sock_fd);
    return 0;
}

/* Send request, print message, and return status code. */
static int send_control_request(const control_request_t *req)
{
    control_response_t resp;
    if (send_control_request_raw(req, &resp) != 0)
        return 1;
    if (resp.message[0] != '\0')
        printf("%s\n", resp.message);
    return resp.status;
}

static void run_client_signal_handler(int sig)
{
    (void)sig;
    run_signal_pending = 1;
}

/* ── Client subcommand implementations ─────────────────────────────── */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    struct sigaction sa, old_int, old_term;
    int rc;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    run_signal_pending = 0;
    memset(run_signal_container_id, 0, sizeof(run_signal_container_id));
    strncpy(run_signal_container_id, req.container_id,
            sizeof(run_signal_container_id) - 1);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = run_client_signal_handler;
    sigemptyset(&sa.sa_mask);
    /* No SA_RESTART: read() must return EINTR so we can forward stop. */
    if (sigaction(SIGINT,  &sa, &old_int)  != 0) { perror("sigaction"); return 1; }
    if (sigaction(SIGTERM, &sa, &old_term) != 0) {
        perror("sigaction");
        sigaction(SIGINT, &old_int, NULL);
        return 1;
    }

    rc = send_control_request(&req);

    sigaction(SIGINT,  &old_int,  NULL);
    sigaction(SIGTERM, &old_term, NULL);
    run_signal_pending         = 0;
    run_signal_container_id[0] = '\0';
    return rc;
}

static int cmd_ps(void)
{
    g_use_color = isatty(STDOUT_FILENO);
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_inspect(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s inspect <id>\n", argv[0]);
        return 1;
    }
    g_use_color = isatty(STDOUT_FILENO);
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_INSPECT;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* Live stats dashboard — refreshes every second until Ctrl-C. */
static int cmd_stats(void)
{
    g_use_color = isatty(STDOUT_FILENO);

    /* Install Ctrl-C handler so we exit cleanly. */
    struct sigaction sa_int;
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = run_client_signal_handler;
    sigemptyset(&sa_int.sa_mask);
    sigaction(SIGINT, &sa_int, NULL);

    run_signal_pending = 0;

    while (!run_signal_pending) {
        control_request_t req;
        control_response_t resp;
        memset(&req, 0, sizeof(req));
        req.kind = CMD_STATS;

        if (send_control_request_raw(&req, &resp) != 0)
            return 1;

        /* Clear screen and redraw. */
        if (g_use_color)
            printf("%s", ANSI_CLEAR);
        else
            printf("\n--- stats ---\n");

        /* Print timestamp header. */
        time_t now = time(NULL);
        char ts[32];
        struct tm *tm_info = localtime(&now);
        strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm_info);
        printf("%sCONTAINER STATS%s  %s%s  (Ctrl-C to exit)\n\n",
               col(ANSI_BOLD), col(ANSI_RESET),
               col(ANSI_DIM), ts);

        /* Print state with color indicators. */
        char *line = resp.message;
        int header_done = 0;
        while (*line) {
            char *nl = strchr(line, '\n');
            size_t len = nl ? (size_t)(nl - line) : strlen(line);

            /* Colorize status tokens in data rows (after 2 header rows). */
            if (header_done && g_use_color) {
                char row[512];
                if (len >= sizeof(row)) len = sizeof(row) - 1;
                memcpy(row, line, len);
                row[len] = '\0';

                if (strstr(row, "RUNNING"))
                    printf("%s%s%s\n", ANSI_GREEN, row, ANSI_RESET);
                else if (strstr(row, "WARNING"))
                    printf("%s%s%s\n", ANSI_YELLOW, row, ANSI_RESET);
                else if (strstr(row, "KILLED"))
                    printf("%s%s%s\n", ANSI_RED, row, ANSI_RESET);
                else if (strstr(row, "STOPPING"))
                    printf("%s%s%s\n", ANSI_YELLOW, row, ANSI_RESET);
                else
                    printf("%s%s%s\n", ANSI_DIM, row, ANSI_RESET);
            } else {
                /* Header lines */
                if (g_use_color && !header_done)
                    printf("%s%.*s%s\n", ANSI_BOLD, (int)len, line, ANSI_RESET);
                else
                    printf("%.*s\n", (int)len, line);

                /* Count header rows (2: column names + separator). */
                if (!header_done && nl) {
                    char *next_nl = strchr(nl + 1, '\n');
                    if (next_nl) header_done = 1;
                }
            }

            if (!nl) break;
            line = nl + 1;
        }

        fflush(stdout);
        sleep(1);
    }

    if (g_use_color) printf("%s", ANSI_RESET);
    printf("\n");
    return 0;
}

static int cmd_logs(int argc, char *argv[])
{
    int follow = 0;
    int i;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id> [--follow|-f]\n", argv[0]);
        return 1;
    }

    for (i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--follow") == 0 || strcmp(argv[i], "-f") == 0)
            follow = 1;
    }

    /* Always print the current log snapshot first. */
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    int rc = send_control_request(&req);
    if (rc != 0 || !follow)
        return rc;

    /* Follow mode: open the log file directly and tail new data. */
    char log_path[PATH_MAX];
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, argv[2]);

    FILE *f = fopen(log_path, "r");
    if (!f) {
        fprintf(stderr, "Cannot open '%s' for follow mode: %s\n",
                log_path, strerror(errno));
        return 1;
    }
    fseek(f, 0, SEEK_END); /* start from end (snapshot already printed) */

    /* Install Ctrl-C handler. */
    struct sigaction sa_int;
    memset(&sa_int, 0, sizeof(sa_int));
    sa_int.sa_handler = run_client_signal_handler;
    sigemptyset(&sa_int.sa_mask);
    sigaction(SIGINT, &sa_int, NULL);
    run_signal_pending = 0;

    while (!run_signal_pending) {
        char buf[4096];
        size_t n = fread(buf, 1, sizeof(buf), f);
        if (n > 0) {
            fwrite(buf, 1, n, stdout);
            fflush(stdout);
        } else {
            usleep(200000); /* 200 ms poll */
        }
    }
    fclose(f);
    return 0;
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ── Entry point ────────────────────────────────────────────────────── */

int main(int argc, char *argv[])
{
    g_use_color = isatty(STDOUT_FILENO);

    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start")   == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")     == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")      == 0) return cmd_ps();
    if (strcmp(argv[1], "inspect") == 0) return cmd_inspect(argc, argv);
    if (strcmp(argv[1], "stats")   == 0) return cmd_stats();
    if (strcmp(argv[1], "logs")    == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")    == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
