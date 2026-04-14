/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 * Complete implementation for OS-Jackfruit project
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

#define STACK_SIZE           (1024 * 1024)
#define CONTAINER_ID_LEN     32
#define CONTROL_PATH         "/tmp/mini_runtime.sock"
#define LOG_DIR              "logs"
#define CONTROL_MESSAGE_LEN  512
#define CHILD_COMMAND_LEN    256
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  16
#define DEFAULT_SOFT_LIMIT   (40UL << 20)
#define DEFAULT_HARD_LIMIT   (64UL << 20)
#define MAX_PS_RESPONSE      8192

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
    int stop_requested;          /* set before SIGTERM so we classify correctly */
    char log_path[PATH_MAX];
    int pipe_read_fd;            /* supervisor side of container stdout/stderr pipe */
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
    int run_mode;   /* 1 = run (block), 0 = start */
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
    int pipe_write_fd;  /* child writes stdout/stderr here */
} child_config_t;

typedef struct {
    /* producer args */
    int pipe_fd;
    char container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_args_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* global supervisor context pointer for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

/* ------------------------------------------------------------------ */
/*  Utility                                                            */
/* ------------------------------------------------------------------ */

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

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
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
                                int argc, char *argv[], int start_index)
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

/* ------------------------------------------------------------------ */
/*  Bounded buffer                                                     */
/* ------------------------------------------------------------------ */

static int bounded_buffer_init(bounded_buffer_t *buf)
{
    int rc;
    memset(buf, 0, sizeof(*buf));
    rc = pthread_mutex_init(&buf->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buf->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buf->mutex); return rc; }
    rc = pthread_cond_init(&buf->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buf->not_empty);
        pthread_mutex_destroy(&buf->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buf)
{
    pthread_cond_destroy(&buf->not_full);
    pthread_cond_destroy(&buf->not_empty);
    pthread_mutex_destroy(&buf->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buf)
{
    pthread_mutex_lock(&buf->mutex);
    buf->shutting_down = 1;
    pthread_cond_broadcast(&buf->not_empty);
    pthread_cond_broadcast(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
}

/* Returns 0 on success, -1 if shut down */
int bounded_buffer_push(bounded_buffer_t *buf, const log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == LOG_BUFFER_CAPACITY && !buf->shutting_down)
        pthread_cond_wait(&buf->not_full, &buf->mutex);

    if (buf->shutting_down) {
        pthread_mutex_unlock(&buf->mutex);
        return -1;
    }

    buf->items[buf->tail] = *item;
    buf->tail = (buf->tail + 1) % LOG_BUFFER_CAPACITY;
    buf->count++;

    pthread_cond_signal(&buf->not_empty);
    pthread_mutex_unlock(&buf->mutex);
    return 0;
}

/* Returns 1 item available, 0 if shut down and empty */
int bounded_buffer_pop(bounded_buffer_t *buf, log_item_t *item)
{
    pthread_mutex_lock(&buf->mutex);
    while (buf->count == 0 && !buf->shutting_down)
        pthread_cond_wait(&buf->not_empty, &buf->mutex);

    if (buf->count == 0) {
        pthread_mutex_unlock(&buf->mutex);
        return 0;
    }

    *item = buf->items[buf->head];
    buf->head = (buf->head + 1) % LOG_BUFFER_CAPACITY;
    buf->count--;

    pthread_cond_signal(&buf->not_full);
    pthread_mutex_unlock(&buf->mutex);
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Logging consumer thread                                            */
/* ------------------------------------------------------------------ */

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        int got = bounded_buffer_pop(&ctx->log_buffer, &item);
        if (!got)
            break;  /* shutdown + empty */

        /* find the log file path for this container */
        char log_path[PATH_MAX] = {0};
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (strcmp(rec->id, item.container_id) == 0) {
                strncpy(log_path, rec->log_path, sizeof(log_path) - 1);
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') continue;

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) continue;
        write(fd, item.data, item.length);
        close(fd);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Producer thread: reads from container pipe, pushes to buffer      */
/* ------------------------------------------------------------------ */

static void *producer_thread(void *arg)
{
    producer_args_t *pa = (producer_args_t *)arg;
    log_item_t item;

    while (1) {
        ssize_t n = read(pa->pipe_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0) break;

        item.data[n] = '\0';
        item.length = (size_t)n;
        strncpy(item.container_id, pa->container_id, CONTAINER_ID_LEN - 1);
        bounded_buffer_push(pa->buffer, &item);
    }

    close(pa->pipe_fd);
    free(pa);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Container child entry point (runs after clone())                  */
/* ------------------------------------------------------------------ */

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* redirect stdout and stderr to the pipe */
    dup2(cfg->pipe_write_fd, STDOUT_FILENO);
    dup2(cfg->pipe_write_fd, STDERR_FILENO);
    close(cfg->pipe_write_fd);

    /* set hostname to container ID */
    sethostname(cfg->id, strlen(cfg->id));

    /* chroot into container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir");
        return 1;
    }

    /* mount /proc so ps etc. work inside */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        /* non-fatal: some rootfs already have it */
    }

    /* apply nice value */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* exec the requested command */
    char *argv[] = { "/bin/sh", "-c", cfg->command, NULL };
    execv("/bin/sh", argv);

    perror("execv");
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Monitor ioctl helpers                                              */
/* ------------------------------------------------------------------ */

int register_with_monitor(int monitor_fd,
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

int unregister_from_monitor(int monitor_fd,
                            const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Container launch (called inside supervisor when CMD_START/RUN)    */
/* ------------------------------------------------------------------ */

static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                            const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return NULL;
    }

    /* allocate child stack */
    char *stack = malloc(STACK_SIZE);
    if (!stack) { close(pipefd[0]); close(pipefd[1]); return NULL; }
    char *stack_top = stack + STACK_SIZE;

    /* build child config */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) { free(stack); close(pipefd[0]); close(pipefd[1]); return NULL; }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(cfg->command, req->command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value    = req->nice_value;
    cfg->pipe_write_fd = pipefd[1];

    /* ensure log dir exists */
    mkdir(LOG_DIR, 0755);

    /* build record */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    if (!rec) { free(cfg); free(stack); close(pipefd[0]); close(pipefd[1]); return NULL; }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_STARTING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->pipe_read_fd     = pipefd[0];
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    /* clone into new namespaces */
    int flags = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;
    pid_t pid = clone(child_fn, stack_top, flags, cfg);
    if (pid < 0) {
        perror("clone");
        free(rec); free(cfg); free(stack);
        close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }
    close(pipefd[1]);  /* parent doesn't write */
    free(stack);
    free(cfg);

    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;

    /* register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, rec->id, pid,
                              rec->soft_limit_bytes, rec->hard_limit_bytes);

    /* start a producer thread for this container's pipe */
    producer_args_t *pa = malloc(sizeof(producer_args_t));
    if (pa) {
        pa->pipe_fd = pipefd[0];
        pa->buffer  = &ctx->log_buffer;
        strncpy(pa->container_id, rec->id, CONTAINER_ID_LEN - 1);
        pthread_t pt;
        pthread_create(&pt, NULL, producer_thread, pa);
        pthread_detach(pt);
    }

    /* insert at head of list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next      = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    fprintf(stderr, "[supervisor] started container %s pid=%d\n", rec->id, pid);
    return rec;
}

/* ------------------------------------------------------------------ */
/*  SIGCHLD / reaping                                                  */
/* ------------------------------------------------------------------ */

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->state     = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else
                        rec->state = CONTAINER_KILLED;  /* hard-limit or external kill */
                }
                fprintf(stderr, "[supervisor] container %s exited state=%s\n",
                        rec->id, state_to_string(rec->state));
                /* unregister from monitor */
                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, rec->id, pid);
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static volatile int g_sigchld_flag = 0;
static volatile int g_sigterm_flag = 0;

static void sigchld_handler(int sig)
{
    (void)sig;
    g_sigchld_flag = 1;
}

static void sigterm_handler(int sig)
{
    (void)sig;
    g_sigterm_flag = 1;
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle one control request                             */
/* ------------------------------------------------------------------ */

static void handle_request(supervisor_ctx_t *ctx,
                            int client_fd,
                            const control_request_t *req)
{
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    switch (req->kind) {

    case CMD_START:
    case CMD_RUN: {
        /* check no duplicate id */
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *existing = ctx->containers;
        while (existing) {
            if (strcmp(existing->id, req->container_id) == 0 &&
                (existing->state == CONTAINER_STARTING ||
                 existing->state == CONTAINER_RUNNING)) {
                break;
            }
            existing = existing->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (existing) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: container '%s' already running", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }

        container_record_t *rec = launch_container(ctx, req);
        if (!rec) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: failed to launch container '%s'", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }

        resp.status = 0;
        if (req->run_mode) {
            /* for 'run': block until container exits, then report */
            snprintf(resp.message, sizeof(resp.message),
                     "OK: running container '%s' pid=%d (blocking)",
                     rec->id, rec->host_pid);
            write(client_fd, &resp, sizeof(resp));

            /* wait for it to finish */
            int wstatus;
            waitpid(rec->host_pid, &wstatus, 0);

            pthread_mutex_lock(&ctx->metadata_lock);
            if (WIFEXITED(wstatus)) {
                rec->exit_code = WEXITSTATUS(wstatus);
                rec->state     = CONTAINER_EXITED;
            } else if (WIFSIGNALED(wstatus)) {
                rec->exit_signal = WTERMSIG(wstatus);
                rec->state = rec->stop_requested ? CONTAINER_STOPPED : CONTAINER_KILLED;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, rec->id, rec->host_pid);

            snprintf(resp.message, sizeof(resp.message),
                     "DONE: container '%s' state=%s exit_code=%d",
                     rec->id, state_to_string(rec->state), rec->exit_code);
            write(client_fd, &resp, sizeof(resp));
        } else {
            snprintf(resp.message, sizeof(resp.message),
                     "OK: started container '%s' pid=%d", rec->id, rec->host_pid);
            write(client_fd, &resp, sizeof(resp));
        }
        break;
    }

    case CMD_PS: {
        char buf[MAX_PS_RESPONSE];
        int off = 0;
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-10s %-8s %-12s %-12s\n",
                        "ID", "PID", "STATE", "EXIT",
                        "SOFT_MiB", "HARD_MiB");
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%-16s %-8s %-10s %-8s %-12s %-12s\n",
                        "----------------","--------","----------",
                        "--------","------------","------------");

        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        while (r && off < (int)sizeof(buf) - 128) {
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8d %-10s %-8d %-12lu %-12lu\n",
                            r->id, r->host_pid,
                            state_to_string(r->state),
                            r->exit_code,
                            r->soft_limit_bytes >> 20,
                            r->hard_limit_bytes >> 20);
            r = r->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        write(client_fd, &resp, sizeof(resp));
        break;
    }

    case CMD_LOGS: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        char log_path[PATH_MAX] = {0};
        while (r) {
            if (strcmp(r->id, req->container_id) == 0) {
                strncpy(log_path, r->log_path, PATH_MAX - 1);
                break;
            }
            r = r->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (log_path[0] == '\0') {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: no container '%s' found", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }

        /* send log file path so client can print it */
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message), "LOGPATH:%s", log_path);
        write(client_fd, &resp, sizeof(resp));
        break;
    }

    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *r = ctx->containers;
        while (r) {
            if (strcmp(r->id, req->container_id) == 0) break;
            r = r->next;
        }
        if (!r || r->state != CONTAINER_RUNNING) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: container '%s' not running", req->container_id);
            write(client_fd, &resp, sizeof(resp));
            return;
        }
        r->stop_requested = 1;
        pid_t pid = r->host_pid;
        pthread_mutex_unlock(&ctx->metadata_lock);

        kill(pid, SIGTERM);
        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "OK: sent SIGTERM to container '%s' pid=%d",
                 req->container_id, pid);
        write(client_fd, &resp, sizeof(resp));
        break;
    }

    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "ERROR: unknown command");
        write(client_fd, &resp, sizeof(resp));
        break;
    }
}

/* ------------------------------------------------------------------ */
/*  Supervisor main loop                                               */
/* ------------------------------------------------------------------ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init");
                   pthread_mutex_destroy(&ctx.metadata_lock); return 1; }

    /* 1) open /dev/container_monitor (optional — only if module loaded) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] WARNING: cannot open /dev/container_monitor: %s\n",
                strerror(errno));

    /* 2) create UNIX-domain control socket */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(ctx.server_fd); return 1;
    }
    if (listen(ctx.server_fd, 10) < 0) {
        perror("listen"); close(ctx.server_fd); return 1;
    }

    /* 3) signal handlers */
    signal(SIGCHLD, sigchld_handler);
    signal(SIGINT,  sigterm_handler);
    signal(SIGTERM, sigterm_handler);

    /* 4) spawn logger consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create logger"); return 1; }

    fprintf(stderr, "[supervisor] started, rootfs=%s, control=%s\n",
            rootfs, CONTROL_PATH);

    /* make server_fd non-blocking so we can check signals in the loop */
    int flags2 = fcntl(ctx.server_fd, F_GETFL, 0);
    fcntl(ctx.server_fd, F_SETFL, flags2 | O_NONBLOCK);

    /* 5) event loop */
    while (!g_sigterm_flag) {
        if (g_sigchld_flag) {
            g_sigchld_flag = 0;
            reap_children(&ctx);
        }

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(10000);
                continue;
            }
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        control_request_t req;
        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n == (ssize_t)sizeof(req))
            handle_request(&ctx, client_fd, &req);

        close(client_fd);
    }

    /* orderly shutdown */
    fprintf(stderr, "[supervisor] shutting down...\n");

    /* stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *r = ctx.containers;
    while (r) {
        if (r->state == CONTAINER_RUNNING) {
            r->stop_requested = 1;
            kill(r->host_pid, SIGTERM);
        }
        r = r->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* wait for children */
    int ws;
    while (waitpid(-1, &ws, 0) > 0) {}

    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* free container list */
    pthread_mutex_lock(&ctx.metadata_lock);
    r = ctx.containers;
    while (r) {
        container_record_t *next = r->next;
        free(r);
        r = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] clean exit.\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Client-side: connect and send request                              */
/* ------------------------------------------------------------------ */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n"
                "Is the supervisor running?\n", CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    write(fd, req, sizeof(*req));

    /* read response(s) */
    control_response_t resp;
    while (read(fd, &resp, sizeof(resp)) == (ssize_t)sizeof(resp)) {
        if (strncmp(resp.message, "LOGPATH:", 8) == 0) {
            /* logs command: read and print the log file */
            const char *path = resp.message + 8;
            FILE *f = fopen(path, "r");
            if (!f) {
                fprintf(stderr, "Cannot open log file: %s\n", path);
            } else {
                char line[512];
                while (fgets(line, sizeof(line), f))
                    fputs(line, stdout);
                fclose(f);
            }
        } else {
            printf("%s\n", resp.message);
        }

        if (strncmp(resp.message, "OK: started", 11) == 0 ||
            strncmp(resp.message, "OK: sent",    8) == 0  ||
            strncmp(resp.message, "ERROR",        5) == 0)
            break;
    }

    close(fd);
    return (resp.status == 0) ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/*  CLI sub-commands                                                   */
/* ------------------------------------------------------------------ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [opts]\n",
                argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    req.run_mode = 0;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [opts]\n",
                argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    req.run_mode = 1;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    strncpy(req.rootfs,       argv[3], PATH_MAX - 1);
    strncpy(req.command,      argv[4], CHILD_COMMAND_LEN - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s logs <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) { fprintf(stderr, "Usage: %s stop <id>\n", argv[0]); return 1; }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], CONTAINER_ID_LEN - 1);
    return send_control_request(&req);
}

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
