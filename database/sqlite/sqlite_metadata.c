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

extern DICTIONARY *rrdhost_root_index;

// Metadata functions

static int sql_store_label(sqlite3_stmt *res, uuid_t *uuid, int source_type, const char *label, const char *value)
{
    int rc;

    rc = sqlite3_bind_blob(res, 1, uuid, sizeof(*uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind UUID parameter to store label information");
        goto skip_store;
    }

    rc = sqlite3_bind_int(res, 2, source_type);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind type parameter to store label information");
        goto skip_store;
    }

    rc = sqlite3_bind_text(res, 3, label, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind label parameter to store label information");
        goto skip_store;
    }

    rc = sqlite3_bind_text(res, 4, value, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind value parameter to store label information");
        goto skip_store;
    }

    rc = execute_insert(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to store label entry, rc = %d", rc);

skip_store:
    if (unlikely(sqlite3_reset(res) != SQLITE_OK))
        error_report("Failed to reset the prepared statement when storing label information");

    return rc != SQLITE_DONE;
}

#define SQL_INS_HOST_LABEL "INSERT OR REPLACE INTO host_label " \
    "(host_id, source_type, label_key, label_value, date_created) " \
    "values (@chart, @source, @label, @value, unixepoch());"

static void sql_store_host_label(uuid_t *host_uuid, int source_type, const char *label, const char *value)
{
    static __thread sqlite3_stmt *res = NULL;
    int rc;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode == RRD_MEMORY_MODE_DBENGINE)
            error_report("Database has not been initialized");
        return;
    }

    if (unlikely(!res)) {
        rc = prepare_statement(db_meta, SQL_INS_HOST_LABEL, &res);
        if (unlikely(rc != SQLITE_OK)) {
            error_report("Failed to prepare statement store chart labels");
            return;
        }
    }

    (void) sql_store_label(res, host_uuid, source_type, label, value);
}


static int save_host_label_callback(const char *name, const char *value, RRDLABEL_SRC label_source, void *data)
{
    RRDHOST *host = (RRDHOST *)data;
    sql_store_host_label(&host->host_uuid, (int)label_source & ~(RRDLABEL_FLAG_INTERNAL), name, value);
    return 0;
}

#define SQL_STORE_CLAIM_ID  "insert into node_instance " \
    "(host_id, claim_id, date_created) values (@host_id, @claim_id, unixepoch()) " \
    "on conflict(host_id) do update set claim_id = excluded.claim_id;"

static void store_claim_id(uuid_t *host_id, uuid_t *claim_id)
{
    sqlite3_stmt *res = NULL;
    int rc;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode == RRD_MEMORY_MODE_DBENGINE)
            error_report("Database has not been initialized");
        return;
    }

    rc = sqlite3_prepare_v2(db_meta, SQL_STORE_CLAIM_ID, -1, &res, 0);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to prepare statement store chart labels");
        return;
    }

    rc = sqlite3_bind_blob(res, 1, host_id, sizeof(*host_id), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind host_id parameter to store node instance information");
        goto failed;
    }

    if (claim_id)
        rc = sqlite3_bind_blob(res, 2, claim_id, sizeof(*claim_id), SQLITE_STATIC);
    else
        rc = sqlite3_bind_null(res, 2);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind claim_id parameter to store node instance information");
        goto failed;
    }

    rc = execute_insert(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to store node instance information, rc = %d", rc);

failed:
    if (unlikely(sqlite3_finalize(res) != SQLITE_OK))
        error_report("Failed to finalize the prepared statement when storing node instance information");

    return;
}

#define DELETE_DIMENSION_UUID   "delete from dimension where dim_id = @uuid;"
static void delete_dimension_uuid(uuid_t *dimension_uuid)
{
    static __thread sqlite3_stmt *res = NULL;
    int rc;

#ifdef NETDATA_INTERNAL_CHECKS
    char uuid_str[GUID_LEN + 1];
    uuid_unparse_lower(*dimension_uuid, uuid_str);
    debug(D_METADATALOG,"Deleting dimension uuid %s", uuid_str);
#endif

    if (unlikely(!res)) {
        rc = prepare_statement(db_meta, DELETE_DIMENSION_UUID, &res);
        if (rc != SQLITE_OK) {
            error_report("Failed to prepare statement to delete a dimension uuid");
            return;
        }
    }

    rc = sqlite3_bind_blob(res, 1, dimension_uuid,  sizeof(*dimension_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_step_monitored(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to delete dimension uuid, rc = %d", rc);

bind_fail:
    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement when deleting dimension UUID, rc = %d", rc);
    return;
}

//
// Store host and host system info information in the database
#define SQL_STORE_HOST_INFO "INSERT OR REPLACE INTO host " \
        "(host_id, hostname, registry_hostname, update_every, os, timezone," \
        "tags, hops, memory_mode, abbrev_timezone, utc_offset, program_name, program_version," \
        "entries, health_enabled) " \
        "values (@host_id, @hostname, @registry_hostname, @update_every, @os, @timezone, @tags, @hops, @memory_mode, " \
        "@abbrev_timezone, @utc_offset, @program_name, @program_version, " \
        "@entries, @health_enabled);"

static int sql_store_host_info(RRDHOST *host)
{
    static __thread sqlite3_stmt *res = NULL;
    int rc;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE)
            return 0;
        error_report("Database has not been initialized");
        return 1;
    }

    if (unlikely((!res))) {
        rc = prepare_statement(db_meta, SQL_STORE_HOST_INFO, &res);
        if (unlikely(rc != SQLITE_OK)) {
            error_report("Failed to prepare statement to store host, rc = %d", rc);
            return 1;
        }
    }

    rc = sqlite3_bind_blob(res, 1, &host->host_uuid, sizeof(host->host_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 2, rrdhost_hostname(host), 0);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 3, rrdhost_registry_hostname(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 4, host->rrd_update_every);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 5, rrdhost_os(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 6, rrdhost_timezone(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 7, rrdhost_tags(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 8, host->system_info ? host->system_info->hops : 0);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 9, host->rrd_memory_mode);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 10, rrdhost_abbrev_timezone(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 11, host->utc_offset);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 12, rrdhost_program_name(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = bind_text_null(res, 13, rrdhost_program_version(host), 1);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int64(res, 14, host->rrd_history_entries);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = sqlite3_bind_int(res, 15, host->health_enabled);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    int store_rc = sqlite3_step_monitored(res);
    if (unlikely(store_rc != SQLITE_DONE))
        error_report("Failed to store host %s, rc = %d", rrdhost_hostname(host), rc);

    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement to store host %s, rc = %d", rrdhost_hostname(host), rc);

    return !(store_rc == SQLITE_DONE);
bind_fail:
    error_report("Failed to bind parameter to store host %s, rc = %d", rrdhost_hostname(host), rc);
    rc = sqlite3_reset(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to reset statement to store host %s, rc = %d", rrdhost_hostname(host), rc);
    return 1;
}

#define SQL_INS_HOST_SYSTEM_INFO "INSERT OR REPLACE INTO host_info " \
    "(host_id, system_key, system_value, date_created) " \
    "VALUES (@host, @key, @value, unixepoch());"

static void sql_store_host_system_info_key_value(uuid_t *host_id, const char *name, const char *value)
{
    sqlite3_stmt *res = NULL;
    int rc;

    rc = sqlite3_prepare_v2(db_meta, SQL_INS_HOST_SYSTEM_INFO, -1, &res, 0);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to prepare statement to store system info");
        return;
    }

    rc = sqlite3_bind_blob(res, 1, host_id, sizeof(*host_id), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind host parameter to store system information");
        goto skip_store;
    }

    rc = sqlite3_bind_text(res, 2, name, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind label parameter to store name information");
        goto skip_store;
    }

    rc = sqlite3_bind_text(res, 3, value, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind value parameter to store value information");
        goto skip_store;
    }

    rc = execute_insert(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to store host system info, rc = %d", rc);

skip_store:
    if (unlikely(sqlite3_finalize(res) != SQLITE_OK))
        error_report("Failed to finalize the prepared statement when storing  host system information");

    return;
}


static void sql_store_host_system_info(uuid_t *host_id, const struct rrdhost_system_info *system_info)
{
    if (unlikely(!system_info))
        return;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode == RRD_MEMORY_MODE_DBENGINE)
            error_report("Database has not been initialized");
        return;
    }

    if (system_info->container_os_name)
        sql_store_host_system_info_key_value(host_id, "NETDATA_CONTAINER_OS_NAME", system_info->container_os_name);

    if (system_info->container_os_id)
        sql_store_host_system_info_key_value(host_id, "NETDATA_CONTAINER_OS_ID", system_info->container_os_id);

    if (system_info->container_os_id_like)
        sql_store_host_system_info_key_value(host_id, "NETDATA_CONTAINER_OS_ID_LIKE", system_info->container_os_id_like);

    if (system_info->container_os_version)
        sql_store_host_system_info_key_value(host_id, "NETDATA_CONTAINER_OS_VERSION", system_info->container_os_version);

    if (system_info->container_os_version_id)
        sql_store_host_system_info_key_value(host_id, "NETDATA_CONTAINER_OS_VERSION_ID", system_info->container_os_version_id);

    if (system_info->host_os_detection)
        sql_store_host_system_info_key_value(host_id, "NETDATA_CONTAINER_OS_DETECTION", system_info->host_os_detection);

    if (system_info->host_os_name)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_OS_NAME", system_info->host_os_name);

    if (system_info->host_os_id)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_OS_ID", system_info->host_os_id);

    if (system_info->host_os_id_like)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_OS_ID_LIKE", system_info->host_os_id_like);

    if (system_info->host_os_version)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_OS_VERSION", system_info->host_os_version);

    if (system_info->host_os_version_id)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_OS_VERSION_ID", system_info->host_os_version_id);

    if (system_info->host_os_detection)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_OS_DETECTION", system_info->host_os_detection);

    if (system_info->kernel_name)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_KERNEL_NAME", system_info->kernel_name);

    if (system_info->host_cores)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_CPU_LOGICAL_CPU_COUNT", system_info->host_cores);

    if (system_info->host_cpu_freq)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_CPU_FREQ", system_info->host_cpu_freq);

    if (system_info->host_ram_total)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_TOTAL_RAM", system_info->host_ram_total);

    if (system_info->host_disk_space)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_TOTAL_DISK_SIZE", system_info->host_disk_space);

    if (system_info->kernel_version)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_KERNEL_VERSION", system_info->kernel_version);

    if (system_info->architecture)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_ARCHITECTURE", system_info->architecture);

    if (system_info->virtualization)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_VIRTUALIZATION", system_info->virtualization);

    if (system_info->virt_detection)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_VIRT_DETECTION", system_info->virt_detection);

    if (system_info->container)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_CONTAINER", system_info->container);

    if (system_info->container_detection)
        sql_store_host_system_info_key_value(host_id, "NETDATA_SYSTEM_CONTAINER_DETECTION", system_info->container_detection);

    if (system_info->is_k8s_node)
        sql_store_host_system_info_key_value(host_id, "NETDATA_HOST_IS_K8S_NODE", system_info->is_k8s_node);

    return;
}


/*
 * Store set option for a dimension
 */
static int sql_set_dimension_option(uuid_t *dim_uuid, char *option)
{
    sqlite3_stmt *res = NULL;
    int rc;

    if (unlikely(!db_meta)) {
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE)
            return 0;
        error_report("Database has not been initialized");
        return 1;
    }

    rc = sqlite3_prepare_v2(db_meta, "UPDATE dimension SET options = @options WHERE dim_id = @dim_id", -1, &res, 0);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to prepare statement to update dimension options");
        return 0;
    };

    rc = sqlite3_bind_blob(res, 2, dim_uuid, sizeof(*dim_uuid), SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    if (!option || !strcmp(option,"unhide"))
        rc = sqlite3_bind_null(res, 1);
    else
        rc = sqlite3_bind_text(res, 1, option, -1, SQLITE_STATIC);
    if (unlikely(rc != SQLITE_OK))
        goto bind_fail;

    rc = execute_insert(res);
    if (unlikely(rc != SQLITE_DONE))
        error_report("Failed to update dimension option, rc = %d", rc);

bind_fail:
    rc = sqlite3_finalize(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to finalize statement in update dimension options, rc = %d", rc);
    return 0;
}

/*
 * Store a chart in the database
 */

static int sql_store_chart(
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


static int update_chart_metadata(uuid_t *chart_uuid, RRDSET *st, const char *id, const char *name)
{
    int rc;

    if (unlikely(!db_meta) && default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE)
        return 0;

    rc = sql_store_chart(
        chart_uuid, &st->rrdhost->host_uuid, rrdset_type_name(st->chart_type), id, name,
        rrdset_family(st), rrdset_context(st), rrdset_title(st), rrdset_units(st),
        rrdset_plugin_name(st), rrdset_module_name(st),
        st->priority, st->update_every, st->chart_type,
        st->rrd_memory_mode, st->entries);

    return rc;
}

#define SQL_DELETE_HOST_LABELS  "DELETE FROM host_label WHERE host_id = @uuid;"
static void sql_store_host_labels(RRDHOST *host)
{
    int rc = exec_statement_with_uuid(SQL_DELETE_HOST_LABELS, &host->host_uuid);
    if (rc != SQLITE_OK)
        error_report("Failed to remove old host labels for host %s", rrdhost_hostname(host));

    rrdlabels_walkthrough_read(host->rrdlabels, save_host_label_callback, host);
}


static bool dimension_can_be_deleted(uuid_t *dim_uuid)
{

#ifdef ENABLE_DBENGINE
    bool no_retention = true;
    for (int tier = 0; tier < storage_tiers; tier++) {
        if (!multidb_ctx[tier])
            continue;
        time_t first_time_t = 0, last_time_t = 0;
        if (rrdeng_metric_retention_by_uuid((void *) multidb_ctx[tier], dim_uuid, &first_time_t, &last_time_t) == 0) {
            if (first_time_t > 0) {
                no_retention = false;
                break;
            }
        }
    }
    return no_retention;
#else
    return false;
#endif
}

#define SELECT_DIMENSION_LIST "SELECT dim_id, rowid FROM dimension WHERE rowid > @row_id;"

static void check_dimension_metadata(struct metadata_database_worker_config *wc)
{
    int rc;

    sqlite3_stmt *res = NULL;

    rc = sqlite3_prepare_v2(db_meta, SELECT_DIMENSION_LIST, -1, &res, 0);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to prepare statement to fetch host dimensions");
        return;
    }

    rc = sqlite3_bind_int64(res, 1, wc->row_id);
    if (unlikely(rc != SQLITE_OK)) {
        error_report("Failed to bind host parameter to fetch host dimensions");
        goto skip_run;
    }

    uint32_t total_checked = 0;
    uint32_t total_deleted= 0;
    uint64_t last_row_id = wc->row_id;

    info("METADATA: Checking dimensions starting after row %lu", wc->row_id);

    while (sqlite3_step_monitored(res) == SQLITE_ROW && total_deleted < MAX_METADATA_CLEANUP) {
        if (unlikely(netdata_exit))
            break;
        total_checked++;

        last_row_id = sqlite3_column_int64(res, 1);
        rc = dimension_can_be_deleted((uuid_t *)sqlite3_column_blob(res, 0));
        if (rc == true) {
            //delete_dimension_uuid((uuid_t *)sqlite3_column_blob(res, 0));
            total_deleted++;
        }
    }
    wc->row_id = last_row_id;
    total_checked++;
    int64_t now = now_realtime_sec();
    if (total_deleted > 0) {
        wc->check_metadata_after = now + METADATA_MAINTENANCE_RETRY;
    } else
        wc->row_id = 0;
    info("METADATA: Checked %u, deleted %u -- will resume after row %lu in %lu seconds", total_checked, total_deleted, wc->row_id,
         wc->check_metadata_after - now);

skip_run:
    rc = sqlite3_finalize(res);
    if (unlikely(rc != SQLITE_OK))
        error_report("Failed to finalize the prepared statement when reading host dimensions");
    return;
}


//
// EVENT LOOP STARTS HERE
//
static uv_mutex_t metadata_async_lock;

static void metadata_database_init_cmd_queue(struct metadata_database_worker_config *wc)
{
    wc->cmd_queue.head = wc->cmd_queue.tail = 0;
    wc->queue_size = 0;
    fatal_assert(0 == uv_cond_init(&wc->cmd_cond));
    fatal_assert(0 == uv_mutex_init(&wc->cmd_mutex));
}

int metadata_database_enq_cmd_noblock(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd)
{
    unsigned queue_size;

    /* wait for free space in queue */
    uv_mutex_lock(&wc->cmd_mutex);
    if ((queue_size = wc->queue_size) == METADATA_DATABASE_CMD_Q_MAX_SIZE || wc->is_shutting_down) {
        uv_mutex_unlock(&wc->cmd_mutex);
        return 1;
    }

    fatal_assert(queue_size < METADATA_DATABASE_CMD_Q_MAX_SIZE);
    /* enqueue command */
    wc->cmd_queue.cmd_array[wc->cmd_queue.tail] = *cmd;
    wc->cmd_queue.tail = wc->cmd_queue.tail != METADATA_DATABASE_CMD_Q_MAX_SIZE - 1 ?
                             wc->cmd_queue.tail + 1 : 0;
    wc->queue_size = queue_size + 1;
    uv_mutex_unlock(&wc->cmd_mutex);
    return 0;
}

static void metadata_database_enq_cmd(struct metadata_database_worker_config *wc, struct metadata_database_cmd *cmd)
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

static struct metadata_database_cmd metadata_database_deq_cmd(struct metadata_database_worker_config* wc)
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


static int metadata_cleanup_running = 0;

static void stop_metadata_maintenance_run()
{
    uv_mutex_lock(&metadata_async_lock);
    metadata_cleanup_running = 0;
    uv_mutex_unlock(&metadata_async_lock);
}

static int metadata_maintenance_run()
{
    int rc = 0;
    uv_mutex_lock(&metadata_async_lock);
    if (unlikely(metadata_cleanup_running))
        rc = 1;
    else
        metadata_cleanup_running = 1;
    uv_mutex_unlock(&metadata_async_lock);
    return rc;
}

static void after_metadata_cleanup(uv_work_t *req, int status)
{
    struct metadata_database_worker_config *wc = req->data;
    (void)status;
    stop_metadata_maintenance_run();
    wc->metadata_cleanup_running = 0;
    //    struct aclk_database_cmd cmd;
    //    memset(&cmd, 0, sizeof(cmd));
    //    cmd.opcode = ACLK_DATABASE_DIM_DELETION;
    //    if (aclk_database_enq_cmd_noblock(wc, &cmd))
    //        info("Failed to queue a dimension deletion message");
    //
    //    cmd.opcode = ACLK_DATABASE_NODE_INFO;
    //    if (aclk_database_enq_cmd_noblock(wc, &cmd))
    //        info("Failed to queue a node update info message");
}

static void start_metadata_cleanup(uv_work_t *req)
{
    struct metadata_database_worker_config *wc = req->data;

    if (unlikely(wc->is_shutting_down))
        return;

    check_dimension_metadata(wc);
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
   struct metadata_database_cmd cmd;
   memset(&cmd, 0, sizeof(cmd));

   time_t now = now_realtime_sec();

   if (wc->check_metadata_after && wc->check_metadata_after < now) {
       cmd.opcode = METADATA_MAINTENANCE;
       if (!metadata_database_enq_cmd_noblock(wc, &cmd))
           wc->check_metadata_after = now + METADATA_MAINTENANCE_INTERVAL;
   }

//    wc->max_batch += 32;
//    struct metadata_database_cmd cmd;
//    memset(&cmd, 0, sizeof(cmd));
//    cmd.opcode = METADATA_DATABASE_TIMER;
//    metadata_database_enq_cmd_noblock(wc, &cmd);
    wc->wakeup_now = 1;
}

#define MAX_CMD_BATCH_SIZE (32)

//static int store_labels_callback(const char *name, const char *value, RRDLABEL_SRC ls, void *data) {
//    sql_store_chart_label((uuid_t *)data, (int)ls, (char *) name, (char *) value);
//    return 1;
//}

static void metadata_database_worker(void *arg)
{
    worker_register("METASYNC");
    worker_register_job_name(METADATA_DATABASE_NOOP,        "noop");
    worker_register_job_name(METADATA_DATABASE_TIMER,       "timer");
    worker_register_job_name(METADATA_ADD_CHART,            "add chart");
    worker_register_job_name(METADATA_ADD_CHART_LABEL,      "add chart label");
    worker_register_job_name(METADATA_ADD_DIMENSION,        "add dimension");
    worker_register_job_name(METADATA_DEL_DIMENSION,        "delete dimension");
    worker_register_job_name(METADATA_ADD_DIMENSION_OPTION, "dimension option");
    worker_register_job_name(METADATA_ADD_HOST_SYSTEM_INFO, "host system info");
    worker_register_job_name(METADATA_ADD_HOST_INFO,        "host info");
    worker_register_job_name(METADATA_STORE_CLAIM_ID,        "add claim id");


    struct metadata_database_worker_config *wc = arg;
    uv_loop_t *loop;
    int ret;
    enum metadata_database_opcode opcode;
    uv_timer_t timer_req;
//    uv_timer_t timer_req1;
    struct metadata_database_cmd cmd;
    unsigned cmd_batch_size;
    uv_work_t metadata_work;

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

    info("Starting metadata sync thread -- scratch area %d entries, %lu bytes", METADATA_DATABASE_CMD_Q_MAX_SIZE, sizeof(*wc));

    memset(&cmd, 0, sizeof(cmd));
    wc->startup_time = now_realtime_sec();
    wc->max_batch = 128;
    wc->metadata_cleanup_running = 0;
    wc->check_metadata_after = wc->startup_time + METADATA_MAINTENANCE_FIRST_CHECK;

    unsigned int max_commands_in_queue = 0;
    while (likely(!netdata_exit)) {
        RRDDIM *rd;
        RRDSET *st;
        RRDHOST *host;
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
                    update_chart_metadata(&st->chart_uuid, st, string2str(st->id), string2str(st->name));
                    dictionary_acquired_item_release(st->rrdhost->rrdset_root_index, dict_item);
                    break;
                case METADATA_ADD_CHART_LABEL:
//                      rrdlabels_walkthrough_read(st->state->chart_labels, store_labels_callback, st->chart_uuid);
                    break;
                case METADATA_ADD_DIMENSION:
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    rd = (RRDDIM *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing DIM %s", string2str(rd->id));
                    rc = sql_store_dimension(&rd->metric_uuid, &rd->rrdset->chart_uuid, string2str(rd->id),
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
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    rd = (RRDDIM *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing DIM %s", string2str(rd->id));

                    rc = sql_set_dimension_option(&rd->metric_uuid, rrddim_flag_check(rd, RRDDIM_FLAG_META_HIDDEN) ? "hidden" : NULL);
                    if (unlikely(rc))
                        error_report("Failed to store dimension option for %s", string2str(rd->id));

                    dictionary_acquired_item_release(rd->rrdset->rrddim_root_index, dict_item);
                    break;
                case METADATA_ADD_HOST_SYSTEM_INFO:
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    host = (RRDHOST *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing HOST SYSTEM INFO %s", string2str(host->hostname));
                    sql_store_host_system_info(&host->host_uuid, host->system_info);
                    dictionary_acquired_item_release(rrdhost_root_index, dict_item);
                    break;
                case METADATA_ADD_HOST_INFO:
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    host = (RRDHOST *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing HOST SYSTEM INFO %s", string2str(host->hostname));
                    rc = sql_store_host_info(host);
                    if (unlikely(rc))
                        error_report("Failed to store host info in the database for %s", string2str(host->hostname));
                    dictionary_acquired_item_release(rrdhost_root_index, dict_item);
                    break;
                case METADATA_STORE_CLAIM_ID:
                    info("METADATA: Storing CLAIM ID FOR HOST");
                    store_claim_id((uuid_t *) cmd.param[0], (uuid_t *) cmd.param[1]);
                    freez((void *) cmd.param[0]);
                    freez((void *) cmd.param[1]);
                    break;
                case METADATA_STORE_HOST_LABELS:
                    dict_item = (DICTIONARY_ITEM * ) cmd.param[0];
                    host = (RRDHOST *) dictionary_acquired_item_value(dict_item);
                    info("METADATA: Storing HOST LABELS %s", string2str(host->hostname));
                    sql_store_host_labels(host);
                    if (unlikely(rc))
                        error_report("Failed to store host labels in the database for %s", string2str(host->hostname));
                    dictionary_acquired_item_release(rrdhost_root_index, dict_item);
                    break;
                case METADATA_MAINTENANCE:
                    if (unlikely(wc->metadata_cleanup_running))
                        break;

                    if (unlikely(metadata_maintenance_run())) {
                        wc->check_metadata_after = now_realtime_sec() + METADATA_MAINTENANCE_RETRY;
                        break;
                    }

                    metadata_work.data = wc;
                    wc->metadata_cleanup_running = 1;
                    if (unlikely(
                            uv_queue_work(loop, &metadata_work, start_metadata_cleanup, after_metadata_cleanup))) {
                        wc->metadata_cleanup_running = 0;
                        stop_metadata_maintenance_run();
                    }
                    break;
                default:
                    break;
            }
            if (cmd.completion)
                metadata_complete(cmd.completion);
        } while (opcode != METADATA_DATABASE_NOOP);
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
// Init function called on agent startup
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


//  Helpers
static inline void _queue_metadata_cmd(enum metadata_database_opcode opcode, const void *param0, const void *param1)
{
    struct metadata_database_cmd cmd;
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = opcode;
    cmd.param[0] = param0;
    cmd.param[1] = param1;
    metadata_database_enq_cmd(&metasync_worker, &cmd);
}

// Public

void queue_chart_update_metadata(RRDSET *st)
{
    const DICTIONARY_ITEM *acquired_st = dictionary_get_and_acquire_item(st->rrdhost->rrdset_root_index, string2str(st->id));
    _queue_metadata_cmd(METADATA_ADD_CHART, acquired_st, NULL);
}

void queue_dimension_update_metadata(RRDDIM *rd)
{
    const DICTIONARY_ITEM *acquired_rd = dictionary_get_and_acquire_item(rd->rrdset->rrddim_root_index, string2str(rd->id));
    _queue_metadata_cmd(METADATA_ADD_DIMENSION, acquired_rd, NULL);
}

void queue_dimension_update_flags(RRDDIM *rd)
{
    const DICTIONARY_ITEM *acquired_rd = dictionary_get_and_acquire_item(rd->rrdset->rrddim_root_index, string2str(rd->id));
    _queue_metadata_cmd(METADATA_ADD_DIMENSION_OPTION, acquired_rd, NULL);
}

void queue_host_update_system_info(const char *machine_guid)
{
    const DICTIONARY_ITEM *acquired_host = dictionary_get_and_acquire_item(rrdhost_root_index, machine_guid);
    _queue_metadata_cmd(METADATA_ADD_HOST_SYSTEM_INFO, acquired_host, NULL);
}

void queue_host_update_info(const char *machine_guid)
{
    const DICTIONARY_ITEM *acquired_host = dictionary_get_and_acquire_item(rrdhost_root_index, machine_guid);
    _queue_metadata_cmd(METADATA_ADD_HOST_INFO, acquired_host, NULL);
}

void queue_delete_dimension_uuid(uuid_t *uuid)
{
    _queue_metadata_cmd(METADATA_DEL_DIMENSION, uuid, NULL);
}

void queue_store_claim_id(uuid_t *host_uuid, uuid_t *claim_uuid)
{
    if (unlikely(!host_uuid))
        return;

    uuid_t *local_host_uuid = mallocz(sizeof(*host_uuid));
    uuid_t *local_claim_uuid = NULL;

    uuid_copy(*local_host_uuid, *host_uuid);
    if (likely(claim_uuid)) {
        local_claim_uuid = mallocz(sizeof(*claim_uuid));
        uuid_copy(*local_claim_uuid, *claim_uuid);
    }
    _queue_metadata_cmd(METADATA_STORE_CLAIM_ID, local_host_uuid, local_claim_uuid);
}

void queue_store_host_labels(const char *machine_guid)
{
    const DICTIONARY_ITEM *acquired_host = dictionary_get_and_acquire_item(rrdhost_root_index, machine_guid);
    _queue_metadata_cmd(METADATA_STORE_HOST_LABELS, acquired_host, NULL);
}