// SPDX-License-Identifier: GPL-3.0-or-later

#include "sqlite_functions.h"
#include "sqlite_metadata.h"

//const char *metadata_sync_config[] = {
//    "CREATE TABLE IF NOT EXISTS dimension_delete (dimension_id blob, dimension_name text, chart_type_id text, "
//    "dim_id blob, chart_id blob, host_id blob, date_created);",
//
//    "CREATE INDEX IF NOT EXISTS ind_h1 ON dimension_delete (host_id);",
//
//    "CREATE TRIGGER IF NOT EXISTS tr_dim_del AFTER DELETE ON dimension BEGIN INSERT INTO dimension_delete "
//    "(dimension_id, dimension_name, chart_type_id, dim_id, chart_id, host_id, date_created)"
//    " select old.id, old.name, c.type||\".\"||c.id, old.dim_id, old.chart_id, c.host_id, strftime('%s') FROM"
//    " chart c WHERE c.chart_id = old.chart_id; END;",
//
//    "DELETE FROM dimension_delete WHERE host_id NOT IN"
//    " (SELECT host_id FROM host) OR strftime('%s') - date_created > 604800;",
//
//    NULL,
//};


// Metadata store functions
/*
 * Store a chart in the database
 */

int sql_store_chart(
    uuid_t *chart_uuid, uuid_t *host_uuid, const char *type, const char *id, const char *name, const char *family,
    const char *context, const char *title, const char *units, const char *plugin, const char *module, long priority,
    int update_every, int chart_type, int memory_mode, long history_entries)
{
    static __thread sqlite3_stmt *res = NULL;
    int rc, param = 0;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE)
            return 0;
        error_report("Database has not been initialized");
        return 1;
    }

    if (unlikely(!res)) {
        rc = prepare_statement(db_meta, SQL_STORE_CHART, &res);
        if (unlikely(rc != SQLITE_OK)) {
            error_report("Failed to prepare statement to store chart, rc = %d", rc);
            return 1;
        }
    }

    param++;
    rc = sqlite3_bind_blob(res, 1, chart_uuid, sizeof(*chart_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_blob(res, 2, host_uuid, sizeof(*host_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 3, type, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 4, id, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    if (name && *name)
        rc = sqlite3_bind_text(res, 5, name, -1, SQLITE_STATIC);
    else
        rc = sqlite3_bind_null(res, 5);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 6, family, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 7, context, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 8, title, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 9, units, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 10, plugin, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_text(res, 11, module, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_int(res, 12, priority);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_int(res, 13, update_every);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_int(res, 14, chart_type);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_int(res, 15, memory_mode);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    param++;
    rc = sqlite3_bind_int(res, 16, history_entries);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = execute_insert(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to store chart, rc = %d", rc);

    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement in chart store function, rc = %d", rc);

    return 0;

bind_fail:
    error_report("Failed to bind parameter %d to store chart, rc = %d", param, rc);
    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement in chart store function, rc = %d", rc);
    return 1;
}

/*
 * Store a dimension
 */
static int sql_store_dimension(
    uuid_t *dim_uuid, uuid_t *chart_uuid, const char *id, const char *name, collected_number multiplier,
    collected_number divisor, int algorithm)
{
    static __thread sqlite3_stmt *res = NULL;
    int rc;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE)
            return 0;
        error_report("Database has not been initialized");
        return 1;
    }

    if (unlikely(!res)) {
        rc = prepare_statement(db_meta, SQL_STORE_DIMENSION, &res);
        if (unlikely(rc != SQLITE_OK)) {
            error_report("Failed to prepare statement to store dimension, rc = %d", rc);
            return 1;
        }
    }

    rc = sqlite3_bind_blob(res, 1, dim_uuid, sizeof(*dim_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_blob(res, 2, chart_uuid, sizeof(*chart_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_text(res, 3, id, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_text(res, 4, name, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 5, multiplier);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 6, divisor);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 7, algorithm);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = execute_insert(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to store dimension, rc = %d", rc);

    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement in store dimension, rc = %d", rc);
    return 0;

bind_fail:
    error_report("Failed to bind parameter to store dimension, rc = %d", rc);
    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement in store dimension, rc = %d", rc);
    return 1;
}


int update_chart_metadata(uuid_t *chart_uuid, RRDSET *st, const char *id, const char *name)
{
    int rc;

    if (unlikely(!db_meta) && default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE)
        return 0;

    rc = sql_store_chart(
        chart_uuid, &st->rrdhost->host_uuid, rrdset_type(st), id, name,
        rrdset_family(st), rrdset_context(st), rrdset_title(st), rrdset_units(st),
        rrdset_plugin_name(st), rrdset_module_name(st),
        st->priority, st->update_every, st->chart_type,
        st->rrd_memory_mode, st->entries);

    return rc;
}




uv_mutex_t metadata_async_lock;

void metadata_database_init_cmd_queue(struct metadata_database_worker_config *wc)
{
    wc->cmd_queue.head = wc->cmd_queue.tail = 0;
    wc->queue_size = 0;
    fatal_assert(0 == uv_cond_init(&wc->cmd_cond));
    fatal_assert(0 == uv_mutex_init(&wc->cmd_mutex));
}

//int metadata_database_enq_cmd_noblock(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd)
//{
//    unsigned queue_size;
//
//    /* wait for free space in queue */
//    uv_mutex_lock(&wc->cmd_mutex);
//    if ((queue_size = wc->queue_size) == METADATA_DATABASE_CMD_Q_MAX_SIZE || wc->is_shutting_down) {
//        uv_mutex_unlock(&wc->cmd_mutex);
//        return 1;
//    }
//
//    fatal_assert(queue_size < METADATA_DATABASE_CMD_Q_MAX_SIZE);
//    /* enqueue command */
//    wc->cmd_queue.cmd_array[wc->cmd_queue.tail] = *cmd;
//    wc->cmd_queue.tail = wc->cmd_queue.tail != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
//                             wc->cmd_queue.tail + 1 : 0;
//    wc->queue_size = queue_size + 1;
//    uv_mutex_unlock(&wc->cmd_mutex);
//    return 0;
//}

void metadata_database_enq_cmd(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd)
{
    unsigned queue_size;

    /* wait for free space in queue */
    uv_mutex_lock(&wc->cmd_mutex);
    if (wc->is_shutting_down) {
        uv_mutex_unlock(&wc->cmd_mutex);
        return;
    }

    while ((queue_size = wc->queue_size) == METADATA_DATABASE_CMD_Q_MAX_SIZE) {
        uv_cond_wait(&wc->cmd_cond, &wc->cmd_mutex);
    }
    fatal_assert(queue_size < METADATA_DATABASE_CMD_Q_MAX_SIZE);
    /* enqueue command */
    wc->cmd_queue.cmd_array[wc->cmd_queue.tail] = *cmd;
    wc->cmd_queue.tail = wc->cmd_queue.tail != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
                             wc->cmd_queue.tail + 1 : 0;
    wc->queue_size = queue_size + 1;
    if (wc->queue_size > wc->max_commands_in_queue)
        wc->max_commands_in_queue = wc->queue_size;
    uv_mutex_unlock(&wc->cmd_mutex);

    /* wake up event loop */
    (void) uv_async_send(&wc->async);
}

struct metadata_database_cmd metadata_database_deq_cmd(struct metadata_database_worker_config* wc)
{
    struct metadata_database_cmd ret;
    unsigned queue_size;

    uv_mutex_lock(&wc->cmd_mutex);
    queue_size = wc->queue_size;
    if (queue_size == 0 || wc->is_shutting_down) {
        memset(&ret, 0, sizeof(ret));
        ret.opcode = METADATA_DATABASE_NOOP;
        ret.completion = NULL;
        if (wc->is_shutting_down)
            uv_cond_signal(&wc->cmd_cond);
    } else {
        /* dequeue command */
        ret = wc->cmd_queue.cmd_array[wc->cmd_queue.head];
        if (queue_size == 1) {
            wc->cmd_queue.head = wc->cmd_queue.tail = 0;
        } else {
            wc->cmd_queue.head = wc->cmd_queue.head != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
                                     wc->cmd_queue.head + 1 : 0;
        }
        wc->queue_size = queue_size - 1;
        /* wake up producers */
        uv_cond_signal(&wc->cmd_cond);
    }
    uv_mutex_unlock(&wc->cmd_mutex);

    return ret;
}

static void async_cb(uv_async_t *handle)
{
    uv_stop(handle->loop);
    uv_update_time(handle->loop);
}

#define TIMER_INITIAL_PERIOD_MS (1000)
#define TIMER_REPEAT_PERIOD_MS (1000)

static void timer_cb(uv_timer_t* handle)
{
    uv_stop(handle->loop);
    uv_update_time(handle->loop);

   struct metadata_database_worker_config *wc = handle->data;
//    wc->max_batch += 32;
//    struct metadata_database_cmd cmd;
//    memset(&cmd, 0, sizeof(cmd));
//    cmd.opcode = METADATA_DATABASE_TIMER;
//    metadata_database_enq_cmd_noblock(wc, &cmd);
    wc->wakeup_now = 1;
}

#define MAX_CMD_BATCH_SIZE (32)

static int store_labels_callback(const char *name, const char *value, RRDLABEL_SRC ls, void *data) {
    sql_store_chart_label((uuid_t *)data, (int)ls, (char *) name, (char *) value);
    return 1;
}

void metadata_database_worker(void *arg)
{
    worker_register("METASYNC");
    worker_register_job_name(METADATA_DATABASE_NOOP,        "noop");
    worker_register_job_name(METADATA_DATABASE_TIMER,       "timer");
    worker_register_job_name(METADATA_ADD_CHART,            "add chart");
    worker_register_job_name(METADATA_ADD_CHART_LABEL,      "add chart label");
    worker_register_job_name(METADATA_ADD_DIMENSION,        "add dimension");
    worker_register_job_name(METADATA_DEL_DIMENSION,        "delete dimension");
    worker_register_job_name(METADATA_ADD_DIMENSION_OPTION, "dimension option");

    struct metadata_database_worker_config *wc = arg;
    uv_loop_t *loop;
    int ret;
    enum metadata_database_opcode opcode;
    uv_timer_t timer_req;
//    uv_timer_t timer_req1;
    struct metadata_database_cmd cmd;
    unsigned cmd_batch_size;

    uv_thread_set_name_np(wc->thread, "METASYNC");
    loop = wc->loop = mallocz(sizeof(uv_loop_t));
    ret = uv_loop_init(loop);
    if (ret) {
        error("uv_loop_init(): %s", uv_strerror(ret));
        goto error_after_loop_init;
    }
    loop->data = wc;

    ret = uv_async_init(wc->loop, &wc->async, async_cb);
    if (ret) {
        error("uv_async_init(): %s", uv_strerror(ret));
        goto error_after_async_init;
    }
    wc->async.data = wc;

    ret = uv_timer_init(loop, &timer_req);
    if (ret) {
        error("uv_timer_init(): %s", uv_strerror(ret));
        goto error_after_timer_init;
    }
    timer_req.data = wc;
    fatal_assert(0 == uv_timer_start(&timer_req, timer_cb, TIMER_INITIAL_PERIOD_MS, TIMER_REPEAT_PERIOD_MS));

//    ret = uv_timer_init(loop, &timer_req1);
//    if (ret) {
//        error("uv_timer_init(): %s", uv_strerror(ret));
//        goto error_after_timer_init;
//    }
//    timer_req1.data = wc;
//    fatal_assert(0 == uv_timer_start(&timer_req1, timer_cb1, 120000, 11000));

    info("Starting metadata sync thread -- scratch area %d entries, %lu bytes", METADATA_DATABASE_CMD_Q_MAX_SIZE, sizeof(*wc));

    memset(&cmd, 0, sizeof(cmd));
    wc->startup_time = now_realtime_sec();
    wc->max_batch = 128;

    unsigned int max_commands_in_queue = 0;
    while (likely(!netdata_exit)) {
        RRDDIM *rd;
        RRDSET *st;
        uuid_t  *uuid;
        int rc;
        DICTIONARY_ITEM *dict_item;

        worker_is_idle();
        uv_run(loop, UV_RUN_DEFAULT);

        /* wait for commands */
        cmd_batch_size = 0;
        do {
            if (unlikely(cmd_batch_size >= wc->max_batch))
                break;
            cmd = metadata_database_deq_cmd(wc);

            if (netdata_exit)
                break;

            opcode = cmd.opcode;
            ++cmd_batch_size;

            if (wc->max_commands_in_queue > max_commands_in_queue) {
                max_commands_in_queue = wc->max_commands_in_queue;
                info("Maximum commands in metadata queue = %u", max_commands_in_queue);
            }

            if (!wc->wakeup_now) {
                worker_is_idle();
                usleep(50 * USEC_PER_MS);
            }

            if (likely(opcode != METADATA_DATABASE_NOOP)) {
                worker_is_busy(opcode);
            }

            switch (opcode) {
                case METADATA_DATABASE_NOOP:
                    /* the command queue was empty, do nothing */
                    break;
                case METADATA_DATABASE_TIMER:
                    /* the command queue was empty, do nothing */
                    //info("Metadata timer tick!");
                    break;
                case METADATA_ADD_CHART:
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    st = (RRDSET *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing CHART %s", string2str(st->id));
                    update_chart_metadata(st->chart_uuid, st, string2str(st->id), string2str(st->name));
                    dictionary_acquired_item_release(st->rrdhost->rrdset_root_index, dict_item);
                    break;
                case METADATA_ADD_CHART_LABEL:
                      rrdlabels_walkthrough_read(st->state->chart_labels, store_labels_callback, st->chart_uuid);
                    break;
                case METADATA_ADD_DIMENSION:
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    rd = (RRDDIM *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing DIM %s", string2str(rd->id));
                    rc = sql_store_dimension(&rd->metric_uuid, rd->rrdset->chart_uuid, string2str(rd->id),
                                             string2str(rd->name), rd->multiplier, rd->divisor, rd->algorithm);
                    if (unlikely(rc))
                        error_report("Failed to store dimension %s", string2str(rd->id));
                    dictionary_acquired_item_release(rd->rrdset->rrddim_root_index, dict_item);
                    break;
                case METADATA_DEL_DIMENSION:
                    uuid = (uuid_t *) cmd.param[0];
                    delete_dimension_uuid(uuid);
                    freez(uuid);
                    break;
                case METADATA_ADD_DIMENSION_OPTION:
                    rd = (RRDDIM *) cmd.param[0];
                    if (likely(!cmd.param[1]))
                        (void)sql_set_dimension_option(&rd->metric_uuid, NULL);
                    else
                        (void)sql_set_dimension_option(&rd->metric_uuid, (char *) cmd.param[1]);
                    freez(cmd.param[1]);
                    break;
                default:
                    break;
            }
            if (cmd.completion)
                metadata_complete(cmd.completion);
        } while (opcode != METADATA_DATABASE_NOOP);
        //db_execute("COMMIT TRANSACTION;");
    }

    if (!uv_timer_stop(&timer_req))
        uv_close((uv_handle_t *)&timer_req, NULL);

//    if (!uv_timer_stop(&timer_req1))
//        uv_close((uv_handle_t *)&timer_req1, NULL);

    /*
     * uv_async_send after uv_close does not seem to crash in linux at the moment,
     * it is however undocumented behaviour we need to be aware if this becomes
     * an issue in the future.
     */
    uv_close((uv_handle_t *)&wc->async, NULL);
    uv_run(loop, UV_RUN_DEFAULT);

    info("Shutting down metadata event loop. Maximum commands in queue %u", max_commands_in_queue);
    /* TODO: don't let the API block by waiting to enqueue commands */
    uv_cond_destroy(&wc->cmd_cond);
    /*  uv_mutex_destroy(&wc->cmd_mutex); */
    //fatal_assert(0 == uv_loop_close(loop));
    int rc;

    do {
        rc = uv_loop_close(loop);
    } while (rc != UV_EBUSY);

    freez(loop);
    worker_unregister();
    return;

error_after_timer_init:
    uv_close((uv_handle_t *)&wc->async, NULL);
error_after_async_init:
    fatal_assert(0 == uv_loop_close(loop));
error_after_loop_init:
    freez(loop);
    worker_unregister();
}

// -------------------------------------------------------------

void metadata_sync_init(struct metadata_database_worker_config *wc)
{

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE) {
            return;
        }
        error_report("Database has not been initialized");
        return;
    }

    fatal_assert(0 == uv_mutex_init(&metadata_async_lock));

    memset(wc, 0, sizeof(*wc));
    metadata_database_init_cmd_queue(wc);
    fatal_assert(0 == uv_thread_create(&(wc->thread), metadata_database_worker, wc));
    info("SQLite metadata sync initialization completed");
    return;
}


// Helpers
static inline void _queue_metadata_cmd(enum metadata_database_opcode opcode, void *param0)
{
    struct metadata_database_cmd cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = opcode;
    cmd.param[0] = param0;
    metadata_database_enq_cmd(&metasync_worker, &cmd);
}

// Public
void queue_chart_update_metadata(RRDSET *st)
{
    DICTIONARY_ITEM *acquired_st = dictionary_get_and_acquire_item(st->rrdhost->rrdset_root_index, string2str(st->id));
    _queue_metadata_cmd(METADATA_ADD_CHART, acquired_st);
}

void queue_dimension_update_metadata(RRDDIM *rd)
{
    DICTIONARY_ITEM *acquired_rd = dictionary_get_and_acquire_item(rd->rrdset->rrddim_root_index, string2str(rd->id));
    _queue_metadata_cmd(METADATA_ADD_DIMENSION, acquired_rd);
}
