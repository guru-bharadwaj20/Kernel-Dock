

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

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 4096
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define STOP_GRACE_SECONDS 5

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    int stop_requested;
    time_t stop_requested_at;
    int run_client_fd;       /* -1 when no CMD_RUN client is waiting */
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

/* Self-pipe for async-signal-safe notification from signal handlers.
 * Signal handlers write the signal number; the main loop reads and
 * dispatches in a context where it is safe to lock, call ioctl, etc. */
static int g_signal_pipe[2] = {-1, -1};

/* Client-side globals for CMD_RUN SIGINT/SIGTERM forwarding */
static volatile sig_atomic_t run_signal_pending;
static char run_signal_container_id[CONTAINER_ID_LEN];

/* ── Async-signal-safe signal handler ──────────────────────────────── */

/* Notify the main loop about incoming signals through the self-pipe. */
static void unified_signal_handler(int sig)
{
    int saved_errno = errno;
    unsigned char s = (unsigned char)sig;
    (void)write(g_signal_pipe[1], &s, 1);
    errno = saved_errno;
}

/* ── Usage ──────────────────────────────────────────────────────────── */

/* Print command-line usage for supervisor and client subcommands. */
static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

/* ── Argument parsing ───────────────────────────────────────────────── */

/* Parse a MiB flag value and store it as bytes in the target field. */
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

/* Parse optional runtime flags such as memory limits and nice value. */
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

/* Convert container state enum values to user-facing text labels. */
static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* ── Bounded buffer ─────────────────────────────────────────────────── */

/* Initialize bounded buffer synchronization primitives and counters. */
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

/* Destroy all synchronization primitives owned by the bounded buffer. */
static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

/* Mark the bounded buffer as shutting down and wake blocked threads. */
static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/* Push one log item into the buffer, blocking while it is full. */
static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }
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

/* Pop one log item from the buffer, blocking while it is empty. */
static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }
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

/* Consume buffered log chunks and append them to per-container log files. */
static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        int ret = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (ret != 0)
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

/* Read container stdout/stderr from a pipe and enqueue log chunks. */
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
        memcpy(item.data, buffer, n);

        if (bounded_buffer_push(&args->ctx->log_buffer, &item) != 0) {
            break;
        }
    }

    close(args->read_fd);
    free(args);
    return NULL;
}

/* ── Container child function ───────────────────────────────────────── */

/* Configure container namespaces/rootfs, then execute the requested command. */
static int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;
    sethostname(cfg->id, strlen(cfg->id));
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot failed");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir failed");
        return 1;
    }
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount /proc failed");
        return 1;
    }
    if (cfg->log_write_fd > 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);
    execlp(cfg->command, cfg->command, NULL);

    perror("exec failed");
    return 1;
}

/* ── Kernel monitor integration ─────────────────────────────────────── */

/* Register a container PID and memory limits with the kernel monitor. */
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

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

/* Unregister a container PID from the kernel monitor device. */
static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ── Container list operations ──────────────────────────────────────── */
/* All list functions require the caller to hold ctx->metadata_lock.    */

/* Insert a container record at the head of the in-memory list. */
static void add_container(supervisor_ctx_t *ctx, container_record_t *rec)
{
    rec->next = ctx->containers;
    ctx->containers = rec;
}

/* Find a container record by its logical container identifier. */
static container_record_t *find_container_by_id(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (strcmp(curr->id, id) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

/* Find a container record by host PID. */
static container_record_t *find_container_by_pid(supervisor_ctx_t *ctx, pid_t pid)
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
        return -1;
    }
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    memset(cfg, 0, sizeof(*cfg));
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
        free(cfg);
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }
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
    rec->host_pid = pid;
    rec->started_at = time(NULL);
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
        register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                            req->soft_limit_bytes, req->hard_limit_bytes);
    }
    producer_args_t *prod_args = malloc(sizeof(producer_args_t));
    if (prod_args) {
        prod_args->ctx = ctx;
        prod_args->read_fd = pipefd[0];
        strncpy(prod_args->container_id, req->container_id, CONTAINER_ID_LEN - 1);

        pthread_t prod_thread;
        pthread_create(&prod_thread, NULL, producer_thread, prod_args);
        pthread_detach(prod_thread);
    } else {
        /* Producer allocation failed — close the pipe read end to avoid
         * leaking the file descriptor.  The container will still run but
         * its stdout/stderr output will not be captured in the log file. */
        close(pipefd[0]);
    }

    free(cfg);

    return pid;
}

/* Reap exited children and update container metadata.
 * Called from the main event loop — safe to lock, call ioctl, etc. */
/* Process any exited child processes and finalize their container state. */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);

        container_record_t *rec = find_container_by_pid(ctx, pid);
        if (rec) {
            if (WIFEXITED(status)) {
                rec->exit_code = WEXITSTATUS(status);
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
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
            if (ctx->monitor_fd >= 0) {
                unregister_from_monitor(ctx->monitor_fd, rec->id, pid);
            }

            /* Notify any attached CMD_RUN client with the exit status */
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
                write(rec->run_client_fd, &run_resp, sizeof(run_resp));
                close(rec->run_client_fd);
                rec->run_client_fd = -1;
            }
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* Send SIGKILL to containers whose stop grace period has expired. */
/* Escalate pending stop requests from SIGTERM to SIGKILL after timeout. */
static void check_stop_escalation(supervisor_ctx_t *ctx)
{
    time_t now = time(NULL);

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *curr = ctx->containers;
    while (curr) {
        if (curr->stop_requested &&
            curr->state == CONTAINER_RUNNING &&
            curr->stop_requested_at > 0 &&
            (now - curr->stop_requested_at) >= STOP_GRACE_SECONDS) {
            kill(curr->host_pid, SIGKILL);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

/* ── Command handlers ───────────────────────────────────────────────── */

/* Handle the START command by creating a new background container. */
static void handle_cmd_start(supervisor_ctx_t *ctx, const control_request_t *req,
                             control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                "Container %s already exists", req->container_id);
        return;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    int pid = create_container(ctx, req);
    if (pid < 0) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "Failed to create container");
        return;
    }

    resp->status = 0;
    snprintf(resp->message, CONTROL_MESSAGE_LEN,
            "Container %s started with PID %d", req->container_id, pid);
}

/* Returns 1 if the response should be sent immediately (error path),
 * or 0 if the response is deferred until the container exits (success). */
/* Handle the RUN command and optionally defer the response until exit. */
static int handle_cmd_run(supervisor_ctx_t *ctx, const control_request_t *req,
                          control_response_t *resp, int client_fd)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id(ctx, req->container_id)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                "Container %s already exists", req->container_id);
        return 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    int pid = create_container(ctx, req);
    if (pid < 0) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "Failed to create container");
        return 1;
    }

    /* Attach the CLI socket to the container record.  The response will
     * be sent by reap_children() when the container exits. */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (rec) {
        rec->run_client_fd = client_fd;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return 0;  /* do not send response yet */
}

/* Build a text table of known containers and their lifecycle details. */
static void handle_cmd_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    int offset = 0;

    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *curr = ctx->containers;
    if (!curr) {
        offset += snprintf(resp->message + offset,
                          CONTROL_MESSAGE_LEN - offset,
                          "No containers\n");
    }
    while (curr && offset < CONTROL_MESSAGE_LEN - 256) {
        char time_str[64];
        struct tm *tm_info = localtime(&curr->started_at);
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", tm_info);

        offset += snprintf(resp->message + offset,
                          CONTROL_MESSAGE_LEN - offset,
                          "ID: %-12s | PID: %-6d | State: %-8s | "
                          "Soft: %lu MB | Hard: %lu MB | Started: %s",
                          curr->id, curr->host_pid,
                          state_to_string(curr->state),
                          curr->soft_limit_bytes / (1UL << 20),
                          curr->hard_limit_bytes / (1UL << 20),
                          time_str);

        /* Show exit information for terminated containers */
        if (curr->state == CONTAINER_EXITED) {
            offset += snprintf(resp->message + offset,
                              CONTROL_MESSAGE_LEN - offset,
                              " | Exit: code %d", curr->exit_code);
        } else if (curr->state == CONTAINER_KILLED) {
            offset += snprintf(resp->message + offset,
                              CONTROL_MESSAGE_LEN - offset,
                              " | Exit: signal %d (hard_limit_killed)",
                              curr->exit_signal);
        } else if (curr->state == CONTAINER_STOPPED) {
            if (curr->exit_signal > 0) {
                offset += snprintf(resp->message + offset,
                                  CONTROL_MESSAGE_LEN - offset,
                                  " | Exit: signal %d", curr->exit_signal);
            } else {
                offset += snprintf(resp->message + offset,
                                  CONTROL_MESSAGE_LEN - offset,
                                  " | Exit: code %d", curr->exit_code);
            }
        }

        offset += snprintf(resp->message + offset,
                          CONTROL_MESSAGE_LEN - offset, "\n");
        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
}

/* Handle STOP by requesting graceful termination of a running container. */
static void handle_cmd_stop(supervisor_ctx_t *ctx, const control_request_t *req,
                            control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                "Container %s not found", req->container_id);
        return;
    }

    if (rec->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                "Container %s is not running", req->container_id);
        return;
    }

    rec->stop_requested = 1;
    rec->stop_requested_at = time(NULL);
    kill(rec->host_pid, SIGTERM);

    pthread_mutex_unlock(&ctx->metadata_lock);

    resp->status = 0;
    snprintf(resp->message, CONTROL_MESSAGE_LEN,
            "Container %s stop requested (SIGTERM sent, SIGKILL in %ds if needed)",
            req->container_id, STOP_GRACE_SECONDS);
}

/* Return the captured log output for a specific container. */
static void handle_cmd_logs(supervisor_ctx_t *ctx, const control_request_t *req,
                            control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);

    container_record_t *rec = find_container_by_id(ctx, req->container_id);
    if (!rec) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN,
                "Container %s not found", req->container_id);
        return;
    }

    char log_path[PATH_MAX];
    strncpy(log_path, rec->log_path, PATH_MAX - 1);
    log_path[PATH_MAX - 1] = '\0';

    pthread_mutex_unlock(&ctx->metadata_lock);

    FILE *f = fopen(log_path, "r");
    if (!f) {
        resp->status = -1;
        snprintf(resp->message, CONTROL_MESSAGE_LEN, "No logs for %s", req->container_id);
        return;
    }

    size_t n = fread(resp->message, 1, CONTROL_MESSAGE_LEN - 1, f);
    resp->message[n] = '\0';
    fclose(f);

    resp->status = 0;
}

/* ── Signal-pipe drain and dispatch ─────────────────────────────────── */

/* Drain pending signal bytes and dispatch to the proper handler path. */
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

/* Read one client request, execute it, and send or defer a response. */
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
    default:
        resp.status = -1;
        snprintf(resp.message, CONTROL_MESSAGE_LEN, "Unknown command");
        break;
    }

    if (should_respond) {
        write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }
    /* For a successful CMD_RUN, client_fd is kept open inside the
     * container record; the response is sent by reap_children(). */
}

/* ── Supervisor main ────────────────────────────────────────────────── */

/* Run the supervisor event loop and perform orderly runtime shutdown. */
static int run_supervisor(const char *rootfs)
{
    (void)rootfs;
    struct sigaction sa_chld, sa_shutdown;

    memset(&global_ctx, 0, sizeof(global_ctx));
    pthread_mutex_init(&global_ctx.metadata_lock, NULL);
    mkdir(LOG_DIR, 0755);

    if (bounded_buffer_init(&global_ctx.log_buffer) != 0) {
        fprintf(stderr, "Failed to initialize bounded buffer\n");
        return 1;
    }

    global_ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (global_ctx.monitor_fd < 0) {
        fprintf(stderr, "Warning: Could not open /dev/container_monitor\n");
    }

    /* Create the self-pipe used by signal handlers to wake the main loop */
    if (pipe(g_signal_pipe) != 0) {
        perror("pipe (signal)");
        return 1;
    }
    fcntl(g_signal_pipe[0], F_SETFL, O_NONBLOCK);
    fcntl(g_signal_pipe[1], F_SETFL, O_NONBLOCK);

    /* Install signal handlers — all write to the self-pipe so that
     * reaping, ioctl, and lock operations happen in the main loop. */
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = unified_signal_handler;
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_shutdown, 0, sizeof(sa_shutdown));
    sa_shutdown.sa_handler = unified_signal_handler;
    sa_shutdown.sa_flags = SA_RESTART;
    sigaction(SIGINT, &sa_shutdown, NULL);
    sigaction(SIGTERM, &sa_shutdown, NULL);

    if (pthread_create(&global_ctx.logger_thread, NULL,
                      logging_thread, &global_ctx) != 0) {
        fprintf(stderr, "Failed to create logger thread\n");
        return 1;
    }

    global_ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (global_ctx.server_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
    unlink(CONTROL_PATH);

    if (bind(global_ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(global_ctx.server_fd, 5) < 0) {
        perror("listen");
        return 1;
    }

    printf("Supervisor started. Listening on %s\n", CONTROL_PATH);
    printf("Ready to accept container requests.\n");

    /* ── Event loop ────────────────────────────── */

    struct pollfd fds[2];
    fds[0].fd = global_ctx.server_fd;
    fds[0].events = POLLIN;
    fds[1].fd = g_signal_pipe[0];
    fds[1].events = POLLIN;

    while (!global_ctx.should_stop) {
        int ret = poll(fds, 2, 1000); /* 1s timeout for escalation checks */

        if (ret < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }

        /* Process pending signals from the self-pipe */
        if (fds[1].revents & POLLIN) {
            drain_signal_pipe(&global_ctx);
        }

        /* Escalate SIGTERM → SIGKILL for containers past grace period */
        check_stop_escalation(&global_ctx);

        /* Accept new CLI connections */
        if (!global_ctx.should_stop && (fds[0].revents & POLLIN)) {
            handle_client_connection(&global_ctx);
        }
    }

    /* ── Orderly shutdown ──────────────────────── */

    printf("\nShutting down supervisor...\n");

    /* Send SIGTERM to every running container that hasn't been stopped */
    pthread_mutex_lock(&global_ctx.metadata_lock);
    container_record_t *curr = global_ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING && !curr->stop_requested) {
            curr->stop_requested = 1;
            kill(curr->host_pid, SIGTERM);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&global_ctx.metadata_lock);

    /* Wait for containers to exit gracefully */
    for (int i = 0; i < STOP_GRACE_SECONDS; i++) {
        sleep(1);
        reap_children(&global_ctx);

        int all_done = 1;
        pthread_mutex_lock(&global_ctx.metadata_lock);
        curr = global_ctx.containers;
        while (curr) {
            if (curr->state == CONTAINER_RUNNING) {
                all_done = 0;
                break;
            }
            curr = curr->next;
        }
        pthread_mutex_unlock(&global_ctx.metadata_lock);
        if (all_done)
            break;
    }

    /* Force-kill any remaining containers */
    pthread_mutex_lock(&global_ctx.metadata_lock);
    curr = global_ctx.containers;
    while (curr) {
        if (curr->state == CONTAINER_RUNNING) {
            kill(curr->host_pid, SIGKILL);
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&global_ctx.metadata_lock);

    usleep(500000); /* 500 ms for SIGKILL to take effect */
    reap_children(&global_ctx);

    /* Notify any CMD_RUN clients still waiting */
    pthread_mutex_lock(&global_ctx.metadata_lock);
    curr = global_ctx.containers;
    while (curr) {
        if (curr->run_client_fd >= 0) {
            control_response_t shutdown_resp;
            memset(&shutdown_resp, 0, sizeof(shutdown_resp));
            shutdown_resp.status = -1;
            snprintf(shutdown_resp.message, CONTROL_MESSAGE_LEN,
                    "Supervisor shutting down");
            write(curr->run_client_fd, &shutdown_resp, sizeof(shutdown_resp));
            close(curr->run_client_fd);
            curr->run_client_fd = -1;
        }
        curr = curr->next;
    }
    pthread_mutex_unlock(&global_ctx.metadata_lock);

    /* Drain and shut down the logging pipeline */
    bounded_buffer_begin_shutdown(&global_ctx.log_buffer);
    pthread_join(global_ctx.logger_thread, NULL);
    bounded_buffer_destroy(&global_ctx.log_buffer);

    /* Clean up file descriptors and socket */
    if (global_ctx.monitor_fd >= 0)
        close(global_ctx.monitor_fd);
    close(global_ctx.server_fd);
    unlink(CONTROL_PATH);
    if (g_signal_pipe[0] >= 0)
        close(g_signal_pipe[0]);
    if (g_signal_pipe[1] >= 0)
        close(g_signal_pipe[1]);

    /* Free all container records */
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

/* Send a control request to the supervisor and return the response code. */
static int send_control_request(const control_request_t *req)
{
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect: Is supervisor running?");
        close(sock_fd);
        return 1;
    }
    if (write(sock_fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(sock_fd);
        return 1;
    }
    control_response_t resp;
    {
        size_t total = 0;
        char *buf = (char *)&resp;
        while (total < sizeof(resp)) {
            ssize_t n = read(sock_fd, buf + total, sizeof(resp) - total);
            if (n > 0) {
                total += (size_t)n;
                continue;
            }
            if (n < 0 && errno == EINTR) {
                /* For CMD_RUN, forward SIGINT/SIGTERM as a stop request */
                if (req->kind == CMD_RUN &&
                    run_signal_pending && run_signal_container_id[0] != '\0') {
                    control_request_t stop_req;

                    memset(&stop_req, 0, sizeof(stop_req));
                    stop_req.kind = CMD_STOP;
                    strncpy(stop_req.container_id,
                            run_signal_container_id,
                            sizeof(stop_req.container_id) - 1);
                    run_signal_pending = 0;
                    send_control_request(&stop_req);
                }
                continue;
            }
            if (n == 0) {
                fprintf(stderr, "Supervisor closed connection\n");
            } else {
                perror("read");
            }
            close(sock_fd);
            return 1;
        }
    }

    close(sock_fd);
    if (resp.message[0] != '\0') {
        printf("%s\n", resp.message);
    }

    return resp.status;
}

/* Mark that the run client received a termination-related signal. */
static void run_client_signal_handler(int sig)
{
    (void)sig;
    run_signal_pending = 1;
}

/* Parse and dispatch the START subcommand request. */
static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

/* Parse and dispatch the RUN subcommand with signal forwarding support. */
static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    struct sigaction sa;
    struct sigaction old_int;
    struct sigaction old_term;
    int rc;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    run_signal_pending = 0;
    memset(run_signal_container_id, 0, sizeof(run_signal_container_id));
    strncpy(run_signal_container_id, req.container_id, sizeof(run_signal_container_id) - 1);

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = run_client_signal_handler;
    sigemptyset(&sa.sa_mask);
    /* No SA_RESTART: we want read() to return EINTR so we can forward stop */

    if (sigaction(SIGINT, &sa, &old_int) != 0) {
        perror("sigaction(SIGINT)");
        return 1;
    }

    if (sigaction(SIGTERM, &sa, &old_term) != 0) {
        perror("sigaction(SIGTERM)");
        sigaction(SIGINT, &old_int, NULL);
        return 1;
    }

    rc = send_control_request(&req);

    sigaction(SIGINT, &old_int, NULL);
    sigaction(SIGTERM, &old_term, NULL);
    run_signal_pending = 0;
    run_signal_container_id[0] = '\0';

    return rc;
}

/* Dispatch the PS subcommand to list tracked containers. */
static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    return send_control_request(&req);
}

/* Parse and dispatch the LOGS subcommand for a container. */
static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

/* Parse and dispatch the STOP subcommand for a container. */
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

/* Route CLI invocations to supervisor mode or client subcommands. */
int main(int argc, char *argv[])
{
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

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
