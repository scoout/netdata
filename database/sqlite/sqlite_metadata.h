// SPDX-License-Identifier: GPL-3.0-or-later

#ifndef NETDATA_SQLITE_METADATA_H
#define NETDATA_SQLITE_METADATA_H

#include "sqlite3.h"
#include "sqlite_functions.h"

#define INC(x)   __atomic_fetch_add(&x, 1, __ATOMIC_SEQ_CST)
#define DEC(x)   __atomic_fetch_add(&x, -1, __ATOMIC_SEQ_CST)

extern sqlite3 *db_meta;

#define METADATA_DATABASE_CMD_Q_MAX_SIZE (100000)
#define METADATA_MAINTENANCE_FIRST_CHECK (60)
#define METADATA_MAINTENANCE_RETRY (60)
#define METADATA_MAINTENANCE_INTERVAL (3600)
#define MAX_METADATA_CLEANUP (500)
#define METADATA_MAX_BATCH_SIZE (1024)

struct metadata_completion {
    uv_mutex_t mutex;
    uv_cond_t cond;
    volatile unsigned completed;
};

static inline void init_metadata_completion(struct metadata_completion *p)
{
    p->completed = 0;
    fatal_assert(0 == uv_cond_init(&p->cond));
    fatal_assert(0 == uv_mutex_init(&p->mutex));
}

static inline void destroy_metadata_completion(struct metadata_completion *p)
{
    uv_cond_destroy(&p->cond);
    uv_mutex_destroy(&p->mutex);
}

static inline void wait_for_metadata_completion(struct metadata_completion *p)
{
    uv_mutex_lock(&p->mutex);
    while (0 == p->completed) {
        uv_cond_wait(&p->cond, &p->mutex);
    }
    fatal_assert(1 == p->completed);
    uv_mutex_unlock(&p->mutex);
}

static inline void metadata_complete(struct metadata_completion *p)
{
    uv_mutex_lock(&p->mutex);
    p->completed = 1;
    uv_mutex_unlock(&p->mutex);
    uv_cond_broadcast(&p->cond);
}

//extern uv_mutex_t metadata_async_lock;

enum metadata_database_opcode {
    METADATA_DATABASE_NOOP = 0,
    METADATA_DATABASE_TIMER,
    METADATA_ADD_CHART,
    METADATA_ADD_CHART_LABEL,
    METADATA_ADD_DIMENSION,
    METADATA_DEL_DIMENSION,
    METADATA_ADD_DIMENSION_OPTION,
    METADATA_ADD_HOST_SYSTEM_INFO,
    METADATA_ADD_HOST_INFO,
    METADATA_STORE_CLAIM_ID,
    METADATA_STORE_HOST_LABELS,
    METADATA_SKIP_TRANSACTION,
    METADATA_MAINTENANCE,
    METADATA_SYNC_SHUTDOWN,
    // leave this last
    // we need it to check for worker utilization
    METADATA_MAX_ENUMERATIONS_DEFINED
};

#define MAX_PARAM_LIST  (4)

#define DEF METADATA_MAX_BATCH_SIZE
struct metadata_database_cmd {
    enum metadata_database_opcode opcode;
    const void *param[MAX_PARAM_LIST];
    struct metadata_completion *completion;
};

struct metadata_database_cmdqueue {
    unsigned head, tail;
    struct metadata_database_cmd cmd_array[METADATA_DATABASE_CMD_Q_MAX_SIZE];
};

struct metadata_database_worker_config {
    uv_thread_t thread;
    time_t startup_time;           // When the sync thread started
    unsigned max_batch;
    unsigned max_commands_in_queue;
    volatile unsigned queue_size;
    int is_shutting_down;
    uv_loop_t *loop;
    uv_async_t async;
    time_t check_metadata_after;
    int metadata_cleanup_running;
    uint64_t row_id;
    /* FIFO command queue */
    uv_mutex_t cmd_mutex;
    uv_cond_t cmd_cond;
    struct metadata_database_cmdqueue cmd_queue;
};

void metadata_sync_init(struct metadata_database_worker_config *metasync_worker);
//int metadata_database_enq_cmd_noblock(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd);
//void metadata_database_enq_cmd(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd);
void queue_dimension_update_metadata(RRDDIM *rd);
void queue_chart_update_metadata(RRDSET *st);
void queue_dimension_update_flags(RRDDIM *rd);
void queue_host_update_system_info(const char *machine_guid);
void queue_host_update_info(const char *machine_guid);
void queue_delete_dimension_uuid(uuid_t *uuid);
void queue_store_claim_id(uuid_t *host_uuid, uuid_t *claim_uuid);
void queue_store_host_labels(const char *machine_guid);
void metadata_sync_exit(void);
#endif //NETDATA_SQLITE_METADATA_H
