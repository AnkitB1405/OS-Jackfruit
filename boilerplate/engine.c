/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * This implementation is adapted from the course boilerplate and a
 * reference student repository, then tightened to fit the assignment
 * contract more closely.
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
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
#define WAIT_PENDING_STATUS 1000

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP,
    CMD_WAIT
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_HARD_LIMIT_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    pthread_t producer_thread;
    int producer_thread_started;
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
    int pipe_write_fd;
} child_config_t;

typedef struct {
    int pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_args_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static volatile sig_atomic_t g_sigchld_flag = 0;
static volatile sig_atomic_t g_sigterm_flag = 0;
static volatile sig_atomic_t g_run_client_stop_requested = 0;

static void copy_string(char *dst, size_t dst_size, const char *src)
{
    if (dst_size == 0)
        return;

    if (!src)
        src = "";

    snprintf(dst, dst_size, "%s", src);
}

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
    case CONTAINER_HARD_LIMIT_KILLED:
        return "hard_limit_killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

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

    buffer->items[buffer->tail] = *item;
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

    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return 0;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 1;
}

static container_record_t *find_container_locked(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *record = ctx->containers;

    while (record) {
        if (strcmp(record->id, id) == 0)
            return record;
        record = record->next;
    }

    return NULL;
}

static int normalize_rootfs_path(const char *input, char *output, size_t output_size)
{
    struct stat st;

    (void)output_size;

    if (!realpath(input, output))
        return -1;

    if (stat(output, &st) != 0 || !S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        return -1;
    }

    return 0;
}

static int rootfs_in_use_locked(supervisor_ctx_t *ctx,
                                const char *rootfs,
                                const char *skip_id)
{
    container_record_t *record = ctx->containers;

    while (record) {
        if ((!skip_id || strcmp(record->id, skip_id) != 0) &&
            (record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING) &&
            strcmp(record->rootfs, rootfs) == 0)
            return 1;
        record = record->next;
    }

    return 0;
}

static int container_return_status(const container_record_t *record)
{
    if (record->state == CONTAINER_EXITED)
        return record->exit_code;

    if (record->exit_signal > 0)
        return 128 + record->exit_signal;

    return 0;
}

static void update_container_exit_record(container_record_t *record, int status)
{
    if (WIFEXITED(status)) {
        record->exit_code = WEXITSTATUS(status);
        record->exit_signal = 0;
        record->state = record->stop_requested ? CONTAINER_STOPPED : CONTAINER_EXITED;
        return;
    }

    if (WIFSIGNALED(status)) {
        record->exit_code = 0;
        record->exit_signal = WTERMSIG(status);
        if (record->stop_requested)
            record->state = CONTAINER_STOPPED;
        else if (record->exit_signal == SIGKILL)
            record->state = CONTAINER_HARD_LIMIT_KILLED;
        else
            record->state = CONTAINER_KILLED;
    }
}

static void join_producer_if_needed(container_record_t *record)
{
    pthread_t thread;

    if (!record->producer_thread_started)
        return;

    thread = record->producer_thread;
    record->producer_thread_started = 0;
    pthread_join(thread, NULL);
}

static int write_full(int fd, const void *buf, size_t count)
{
    const char *ptr = buf;
    size_t written = 0;

    while (written < count) {
        ssize_t rc = write(fd, ptr + written, count - written);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        written += (size_t)rc;
    }

    return 0;
}

static int read_full(int fd, void *buf, size_t count)
{
    char *ptr = buf;
    size_t read_bytes = 0;

    while (read_bytes < count) {
        ssize_t rc = read(fd, ptr + read_bytes, count - read_bytes);
        if (rc == 0)
            return -1;
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        read_bytes += (size_t)rc;
    }

    return 0;
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item)) {
        char log_path[PATH_MAX] = {0};
        int fd;

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *record = find_container_locked(ctx, item.container_id);
        if (record)
            copy_string(log_path, sizeof(log_path), record->log_path);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0')
            continue;

        fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0)
            continue;

        (void)write_full(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static void *producer_thread(void *arg)
{
    producer_args_t *producer = arg;
    log_item_t item;

    memset(&item, 0, sizeof(item));
    copy_string(item.container_id, sizeof(item.container_id), producer->container_id);

    while (1) {
        ssize_t bytes_read = read(producer->pipe_fd, item.data, LOG_CHUNK_SIZE);
        if (bytes_read <= 0)
            break;

        item.length = (size_t)bytes_read;
        if (bounded_buffer_push(producer->buffer, &item) != 0)
            break;
    }

    close(producer->pipe_fd);
    free(producer);
    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *config = arg;
    char *argv[] = {"/bin/sh", "-c", config->command, NULL};

    if (dup2(config->pipe_write_fd, STDOUT_FILENO) < 0)
        return 1;
    if (dup2(config->pipe_write_fd, STDERR_FILENO) < 0)
        return 1;
    close(config->pipe_write_fd);

    if (sethostname(config->id, strlen(config->id)) != 0)
        perror("sethostname");
    (void)mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL);

    if (chroot(config->rootfs) != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    (void)mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0 && errno != EBUSY) {
        perror("mount /proc");
        return 1;
    }

    if (config->nice_value != 0) {
        errno = 0;
        if (nice(config->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    execv("/bin/sh", argv);
    perror("execv");
    return 1;
}

static int register_with_monitor(int monitor_fd,
                                 const char *container_id,
                                 pid_t host_pid,
                                 unsigned long soft_limit_bytes,
                                 unsigned long hard_limit_bytes)
{
    struct monitor_request request;

    memset(&request, 0, sizeof(request));
    request.pid = host_pid;
    request.soft_limit_bytes = soft_limit_bytes;
    request.hard_limit_bytes = hard_limit_bytes;
    copy_string(request.container_id, sizeof(request.container_id), container_id);

    return ioctl(monitor_fd, MONITOR_REGISTER, &request);
}

static int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request request;

    memset(&request, 0, sizeof(request));
    request.pid = host_pid;
    copy_string(request.container_id, sizeof(request.container_id), container_id);

    return ioctl(monitor_fd, MONITOR_UNREGISTER, &request);
}

static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                            const control_request_t *request)
{
    char normalized_rootfs[PATH_MAX];
    int pipe_fds[2];
    char *stack = NULL;
    char *stack_top;
    child_config_t *config = NULL;
    container_record_t *record = NULL;
    producer_args_t *producer = NULL;
    int log_fd;
    pid_t pid;
    int flags;

    if (normalize_rootfs_path(request->rootfs, normalized_rootfs, sizeof(normalized_rootfs)) != 0)
        return NULL;

    if (pipe(pipe_fds) != 0)
        return NULL;

    stack = malloc(STACK_SIZE);
    if (!stack)
        goto fail;
    stack_top = stack + STACK_SIZE;

    config = calloc(1, sizeof(*config));
    if (!config)
        goto fail;

    copy_string(config->id, sizeof(config->id), request->container_id);
    copy_string(config->rootfs, sizeof(config->rootfs), normalized_rootfs);
    copy_string(config->command, sizeof(config->command), request->command);
    config->nice_value = request->nice_value;
    config->pipe_write_fd = pipe_fds[1];

    (void)mkdir(LOG_DIR, 0755);

    record = calloc(1, sizeof(*record));
    if (!record)
        goto fail;

    copy_string(record->id, sizeof(record->id), request->container_id);
    copy_string(record->rootfs, sizeof(record->rootfs), normalized_rootfs);
    record->started_at = time(NULL);
    record->state = CONTAINER_STARTING;
    record->soft_limit_bytes = request->soft_limit_bytes;
    record->hard_limit_bytes = request->hard_limit_bytes;
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, request->container_id);

    log_fd = open(record->log_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (log_fd >= 0)
        close(log_fd);

    flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid = clone(child_fn, stack_top, flags, config);
    if (pid < 0)
        goto fail;

    close(pipe_fds[1]);
    pipe_fds[1] = -1;

    record->host_pid = pid;
    record->state = CONTAINER_RUNNING;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0 &&
        register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) != 0) {
        fprintf(stderr,
                "[supervisor] warning: failed to register %s with monitor: %s\n",
                record->id,
                strerror(errno));
    }

    producer = calloc(1, sizeof(*producer));
    if (!producer) {
        close(pipe_fds[0]);
        pipe_fds[0] = -1;
        return record;
    }

    producer->pipe_fd = pipe_fds[0];
    producer->buffer = &ctx->log_buffer;
    copy_string(producer->container_id, sizeof(producer->container_id), record->id);

    if (pthread_create(&record->producer_thread, NULL, producer_thread, producer) == 0) {
        record->producer_thread_started = 1;
        pipe_fds[0] = -1;
    } else {
        fprintf(stderr, "[supervisor] warning: failed to start producer thread for %s\n", record->id);
        close(pipe_fds[0]);
        free(producer);
    }

    free(config);
    free(stack);
    return record;

fail:
    if (pipe_fds[0] >= 0)
        close(pipe_fds[0]);
    if (pipe_fds[1] >= 0)
        close(pipe_fds[1]);
    free(record);
    free(config);
    free(stack);
    return NULL;
}

static void handle_child_exit(supervisor_ctx_t *ctx, pid_t pid, int status)
{
    container_record_t *record = NULL;
    char container_id[CONTAINER_ID_LEN] = {0};
    container_state_t state = CONTAINER_EXITED;
    int return_status = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (record = ctx->containers; record; record = record->next) {
        if (record->host_pid == pid) {
            update_container_exit_record(record, status);
            copy_string(container_id, sizeof(container_id), record->id);
            state = record->state;
            return_status = container_return_status(record);
            break;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!record)
        return;

    join_producer_if_needed(record);

    if (ctx->monitor_fd >= 0)
        (void)unregister_from_monitor(ctx->monitor_fd, container_id, pid);

    fprintf(stderr,
            "[supervisor] container %s exited state=%s return=%d\n",
            container_id,
            state_to_string(state),
            return_status);
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0)
        handle_child_exit(ctx, pid, status);
}

static void supervisor_signal_handler(int sig)
{
    if (sig == SIGCHLD)
        g_sigchld_flag = 1;
    else
        g_sigterm_flag = 1;
}

static void run_client_signal_handler(int sig)
{
    (void)sig;
    g_run_client_stop_requested = 1;
}

static int send_response(int client_fd, int status, const char *message)
{
    control_response_t response;

    memset(&response, 0, sizeof(response));
    response.status = status;
    copy_string(response.message, sizeof(response.message), message);
    return write_full(client_fd, &response, sizeof(response));
}

static void format_ps_response(supervisor_ctx_t *ctx, char *buffer, size_t buffer_size)
{
    container_record_t *record;
    size_t offset = 0;

    offset += (size_t)snprintf(buffer + offset,
                               buffer_size - offset,
                               "%-12s %-8s %-18s %-8s %-8s %-8s %-s\n",
                               "ID",
                               "PID",
                               "STATE",
                               "EXIT",
                               "SIG",
                               "SOFT",
                               "HARD");

    pthread_mutex_lock(&ctx->metadata_lock);
    record = ctx->containers;
    while (record && offset < buffer_size) {
        offset += (size_t)snprintf(buffer + offset,
                                   buffer_size - offset,
                                   "%-12s %-8d %-18s %-8d %-8d %-8lu %-lu\n",
                                   record->id,
                                   record->host_pid,
                                   state_to_string(record->state),
                                   record->exit_code,
                                   record->exit_signal,
                                   record->soft_limit_bytes >> 20,
                                   record->hard_limit_bytes >> 20);
        record = record->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void handle_request(supervisor_ctx_t *ctx, int client_fd, const control_request_t *request)
{
    char normalized_rootfs[PATH_MAX];
    char response[CONTROL_MESSAGE_LEN];
    container_record_t *record;

    memset(response, 0, sizeof(response));

    switch (request->kind) {
    case CMD_START:
    case CMD_RUN:
        if (normalize_rootfs_path(request->rootfs, normalized_rootfs, sizeof(normalized_rootfs)) != 0) {
            snprintf(response,
                     sizeof(response),
                     "ERROR: invalid rootfs path: %s",
                     strerror(errno));
            (void)send_response(client_fd, -1, response);
            return;
        }

        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_locked(ctx, request->container_id);
        if (record &&
            (record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(response,
                     sizeof(response),
                     "ERROR: container '%s' already running",
                     request->container_id);
            (void)send_response(client_fd, -1, response);
            return;
        }

        if (rootfs_in_use_locked(ctx, normalized_rootfs, request->container_id)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(response,
                     sizeof(response),
                     "ERROR: that rootfs is already in use by another live container");
            (void)send_response(client_fd, -1, response);
            return;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        record = launch_container(ctx, request);
        if (!record) {
            snprintf(response,
                     sizeof(response),
                     "ERROR: failed to launch container '%s': %s",
                     request->container_id,
                     strerror(errno));
            (void)send_response(client_fd, -1, response);
            return;
        }

        snprintf(response,
                 sizeof(response),
                 "OK: started container '%s' pid=%d",
                 record->id,
                 record->host_pid);
        (void)send_response(client_fd, 0, response);
        return;

    case CMD_WAIT:
        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_locked(ctx, request->container_id);
        if (!record) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(response,
                     sizeof(response),
                     "ERROR: container '%s' not found",
                     request->container_id);
            (void)send_response(client_fd, -1, response);
            return;
        }

        if (record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(response,
                     sizeof(response),
                     "WAIT: container '%s' still running",
                     request->container_id);
            (void)send_response(client_fd, WAIT_PENDING_STATUS, response);
            return;
        }

        snprintf(response,
                 sizeof(response),
                 "DONE: container '%s' state=%s exit_status=%d",
                 record->id,
                 state_to_string(record->state),
                 container_return_status(record));
        {
            int status = container_return_status(record);
            pthread_mutex_unlock(&ctx->metadata_lock);
            (void)send_response(client_fd, status, response);
        }
        return;

    case CMD_PS:
        format_ps_response(ctx, response, sizeof(response));
        (void)send_response(client_fd, 0, response);
        return;

    case CMD_LOGS:
        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_locked(ctx, request->container_id);
        if (record) {
            copy_string(response, sizeof(response), record->log_path);
            pthread_mutex_unlock(&ctx->metadata_lock);
        } else {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(response, sizeof(response), "%s/%s.log", LOG_DIR, request->container_id);
        }

        if (access(response, R_OK) != 0) {
            snprintf(response,
                     sizeof(response),
                     "ERROR: no log file found for '%s'",
                     request->container_id);
            (void)send_response(client_fd, -1, response);
            return;
        }

        {
            char message[CONTROL_MESSAGE_LEN];
            snprintf(message, sizeof(message), "LOGPATH:%s", response);
            (void)send_response(client_fd, 0, message);
        }
        return;

    case CMD_STOP:
        pthread_mutex_lock(&ctx->metadata_lock);
        record = find_container_locked(ctx, request->container_id);
        if (!record ||
            (record->state != CONTAINER_STARTING && record->state != CONTAINER_RUNNING)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(response,
                     sizeof(response),
                     "ERROR: container '%s' is not running",
                     request->container_id);
            (void)send_response(client_fd, -1, response);
            return;
        }
        record->stop_requested = 1;
        pid_t pid = record->host_pid;
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (kill(pid, SIGTERM) != 0 && errno != ESRCH) {
            snprintf(response,
                     sizeof(response),
                     "ERROR: failed to signal container '%s': %s",
                     request->container_id,
                     strerror(errno));
            (void)send_response(client_fd, -1, response);
            return;
        }

        snprintf(response,
                 sizeof(response),
                 "OK: sent SIGTERM to container '%s' pid=%d",
                 request->container_id,
                 pid);
        (void)send_response(client_fd, 0, response);
        return;

    default:
        (void)send_response(client_fd, -1, "ERROR: unknown command");
        return;
    }
}

static void install_supervisor_signal_handlers(void)
{
    struct sigaction action;

    memset(&action, 0, sizeof(action));
    action.sa_handler = supervisor_signal_handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_RESTART;

    (void)sigaction(SIGCHLD, &action, NULL);
    (void)sigaction(SIGINT, &action, NULL);
    (void)sigaction(SIGTERM, &action, NULL);
}

static int run_supervisor(const char *base_rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr,
                "[supervisor] warning: could not open /dev/container_monitor: %s\n",
                strerror(errno));
    }

    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto fail;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        goto fail;
    }

    if (listen(ctx.server_fd, 16) != 0) {
        perror("listen");
        goto fail;
    }

    {
        int socket_flags = fcntl(ctx.server_fd, F_GETFL, 0);
        if (socket_flags >= 0)
            (void)fcntl(ctx.server_fd, F_SETFL, socket_flags | O_NONBLOCK);
    }

    install_supervisor_signal_handlers();

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        goto fail;
    }

    fprintf(stderr,
            "[supervisor] started base-rootfs=%s control=%s\n",
            base_rootfs,
            CONTROL_PATH);

    while (!g_sigterm_flag) {
        control_request_t request;
        int client_fd;

        if (g_sigchld_flag) {
            g_sigchld_flag = 0;
            reap_children(&ctx);
        }

        client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(10000);
                continue;
            }
            if (errno == EINTR)
                continue;
            perror("accept");
            break;
        }

        if (read_full(client_fd, &request, sizeof(request)) == 0)
            handle_request(&ctx, client_fd, &request);

        close(client_fd);
    }

    fprintf(stderr, "[supervisor] shutting down...\n");

    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *record = ctx.containers;
    while (record) {
        if (record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING) {
            record->stop_requested = 1;
            (void)kill(record->host_pid, SIGTERM);
        }
        record = record->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    while (1) {
        int status;
        pid_t pid = waitpid(-1, &status, 0);
        if (pid < 0) {
            if (errno == EINTR)
                continue;
            break;
        }
        handle_child_exit(&ctx, pid, status);
    }

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    pthread_mutex_lock(&ctx.metadata_lock);
    record = ctx.containers;
    while (record) {
        container_record_t *next = record->next;
        join_producer_if_needed(record);
        free(record);
        record = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] clean exit.\n");
    return 0;

fail:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);
    return 1;
}

static int send_single_request(const control_request_t *request, control_response_t *response)
{
    int fd;
    struct sockaddr_un addr;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    copy_string(addr.sun_path, sizeof(addr.sun_path), CONTROL_PATH);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        fprintf(stderr,
                "Cannot connect to supervisor at %s: %s\nIs the supervisor running?\n",
                CONTROL_PATH,
                strerror(errno));
        close(fd);
        return 1;
    }

    if (write_full(fd, request, sizeof(*request)) != 0 ||
        read_full(fd, response, sizeof(*response)) != 0) {
        fprintf(stderr, "Control channel I/O failed.\n");
        close(fd);
        return 1;
    }

    close(fd);
    return 0;
}

static int render_log_response(const control_response_t *response)
{
    const char *path = response->message + 8;
    FILE *file = fopen(path, "r");
    char line[512];

    if (!file) {
        fprintf(stderr, "Cannot open log file: %s\n", path);
        return 1;
    }

    while (fgets(line, sizeof(line), file))
        fputs(line, stdout);

    fclose(file);
    return 0;
}

static int send_control_request(const control_request_t *request)
{
    control_response_t response;

    memset(&response, 0, sizeof(response));

    if (request->kind != CMD_RUN) {
        if (send_single_request(request, &response) != 0)
            return 1;

        if (strncmp(response.message, "LOGPATH:", 8) == 0)
            return render_log_response(&response);

        printf("%s\n", response.message);
        return (response.status < 0) ? 1 : 0;
    }

    if (send_single_request(request, &response) != 0)
        return 1;

    printf("%s\n", response.message);
    if (response.status < 0)
        return 1;

    {
        struct sigaction action;
        struct sigaction old_int;
        struct sigaction old_term;
        control_request_t wait_request;
        int forwarded_stop = 0;

        memset(&action, 0, sizeof(action));
        action.sa_handler = run_client_signal_handler;
        sigemptyset(&action.sa_mask);
        (void)sigaction(SIGINT, &action, &old_int);
        (void)sigaction(SIGTERM, &action, &old_term);

        memset(&wait_request, 0, sizeof(wait_request));
        wait_request.kind = CMD_WAIT;
        copy_string(wait_request.container_id,
                    sizeof(wait_request.container_id),
                    request->container_id);

        while (1) {
            if (g_run_client_stop_requested && !forwarded_stop) {
                control_request_t stop_request;
                control_response_t stop_response;

                memset(&stop_request, 0, sizeof(stop_request));
                memset(&stop_response, 0, sizeof(stop_response));
                stop_request.kind = CMD_STOP;
                copy_string(stop_request.container_id,
                            sizeof(stop_request.container_id),
                            request->container_id);
                if (send_single_request(&stop_request, &stop_response) == 0)
                    fprintf(stderr, "%s\n", stop_response.message);
                forwarded_stop = 1;
            }

            if (send_single_request(&wait_request, &response) != 0) {
                (void)sigaction(SIGINT, &old_int, NULL);
                (void)sigaction(SIGTERM, &old_term, NULL);
                return 1;
            }

            if (response.status == WAIT_PENDING_STATUS) {
                usleep(250000);
                continue;
            }

            printf("%s\n", response.message);
            (void)sigaction(SIGINT, &old_int, NULL);
            (void)sigaction(SIGTERM, &old_term, NULL);
            g_run_client_stop_requested = 0;
            return (response.status < 0) ? 1 : response.status;
        }
    }
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t request;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&request, 0, sizeof(request));
    request.kind = CMD_START;
    copy_string(request.container_id, sizeof(request.container_id), argv[2]);
    copy_string(request.rootfs, sizeof(request.rootfs), argv[3]);
    copy_string(request.command, sizeof(request.command), argv[4]);
    request.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    request.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&request, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&request);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t request;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&request, 0, sizeof(request));
    request.kind = CMD_RUN;
    copy_string(request.container_id, sizeof(request.container_id), argv[2]);
    copy_string(request.rootfs, sizeof(request.rootfs), argv[3]);
    copy_string(request.command, sizeof(request.command), argv[4]);
    request.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    request.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&request, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&request);
}

static int cmd_ps(void)
{
    control_request_t request;

    memset(&request, 0, sizeof(request));
    request.kind = CMD_PS;
    return send_control_request(&request);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t request;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&request, 0, sizeof(request));
    request.kind = CMD_LOGS;
    copy_string(request.container_id, sizeof(request.container_id), argv[2]);
    return send_control_request(&request);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t request;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&request, 0, sizeof(request));
    request.kind = CMD_STOP;
    copy_string(request.container_id, sizeof(request.container_id), argv[2]);
    return send_control_request(&request);
}

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
