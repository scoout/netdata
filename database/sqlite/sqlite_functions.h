// SPDX-License-Identifier: GPL-3.0-or-later

#ifndef NETDATA_SQLITE_FUNCTIONS_H
#define NETDATA_SQLITE_FUNCTIONS_H

#include "daemon/common.h"
#include "sqlite3.h"

// return a node list
struct node_instance_list {
    uuid_t  node_id;
    uuid_t  host_id;
    char *hostname;
    int live;
    int queryable;
    int hops;
};

typedef enum db_check_action_type {
    DB_CHECK_NONE  = 0x0000,
    DB_CHECK_INTEGRITY  = 0x0001,
    DB_CHECK_FIX_DB = 0x0002,
    DB_CHECK_RECLAIM_SPACE = 0x0004,
    DB_CHECK_CONT = 0x00008
} db_check_action_type_t;

#define SQL_MAX_RETRY (100)
#define SQLITE_INSERT_DELAY (50)        // Insert delay in case of lock

#define SQL_STORE_HOST "insert or replace into host (host_id,hostname,registry_hostname,update_every,os,timezone,tags, hops) " \
        "values (?1,?2,?3,?4,?5,?6,?7,?8);"

#define SQL_STORE_CHART "insert or replace into chart (chart_id, host_id, type, id, " \
    "name, family, context, title, unit, plugin, module, priority, update_every , chart_type , memory_mode , " \
    "history_entries) values (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16);"

#define SQL_FIND_CHART_UUID                                                                                            \
    "select chart_id from chart where host_id = @host and type=@type and id=@id and (name is null or name=@name) and chart_id is not null;"

#define SQL_STORE_DIMENSION                                                                                           \
    "INSERT OR REPLACE into dimension (dim_id, chart_id, id, name, multiplier, divisor , algorithm) values (?0001,?0002,?0003,?0004,?0005,?0006,?0007);"

#define SQL_FIND_DIMENSION_UUID \
    "select dim_id from dimension where chart_id=@chart and id=@id and name=@name and length(dim_id)=16;"

#define CHECK_SQLITE_CONNECTION(db_meta)                                                                               \
    if (unlikely(!db_meta)) {                                                                                          \
        if (default_rrd_memory_mode != RRD_MEMORY_MODE_DBENGINE) {                                                     \
            return 1;                                                                                                  \
        }                                                                                                              \
        error_report("Database has not been initialized");                                                             \
        return 1;                                                                                                      \
    }

extern SQLITE_API int sqlite3_step_monitored(sqlite3_stmt *stmt);
extern SQLITE_API int sqlite3_exec_monitored(
    sqlite3 *db,                               /* An open database */
    const char *sql,                           /* SQL to be evaluated */
    int (*callback)(void*,int,char**,char**),  /* Callback function */
    void *data,                                /* 1st argument to callback */
    char **errmsg                              /* Error msg written here */
    );

// Initialization and shutdown
int init_database_batch(sqlite3 *database, int rebuild, int init_type, const char *batch[]);
extern int sql_init_database(db_check_action_type_t rebuild, int memory);
extern void sql_close_database(void);

// Helpers
extern int bind_text_null(sqlite3_stmt *res, int position, const char *text, bool can_be_null);
extern int prepare_statement(sqlite3 *database, char *query, sqlite3_stmt **statement);
extern int execute_insert(sqlite3_stmt *res);
extern void db_execute(const char *cmd);
extern int file_is_migrated(char *path);
extern void add_migrated_file(char *path, uint64_t file_size);
extern void db_unlock(void);
extern void db_lock(void);
void migrate_localhost(uuid_t *host_uuid);
int exec_statement_with_uuid(const char *sql, uuid_t *uuid);

// Look up functions
extern int find_dimension_uuid(RRDSET *st, RRDDIM *rd, uuid_t *store_uuid);
extern int find_chart_uuid(RRDHOST *host, const char *type, const char *id, const char *name, uuid_t *store_uuid);
extern int find_uuid_type(uuid_t *uuid);
extern int get_node_id(uuid_t *host_id, uuid_t *node_id);
extern int get_host_id(uuid_t *node_id, uuid_t *host_id);
extern struct node_instance_list *get_node_list(void);
extern void sql_load_node_id(RRDHOST *host);
char *get_hostname_by_node_id(char *node_id);

// Help build archived hosts in memory when agent starts
extern void sql_build_host_system_info(uuid_t *host_id, struct rrdhost_system_info *system_info);
DICTIONARY *sql_load_host_labels(uuid_t *host_id);

// For queries: To be removed when context queries are implemented
extern RRDHOST *sql_create_host_by_uuid(char *guid);
extern void sql_rrdset2json(RRDHOST *host, BUFFER *wb);
extern void sql_build_context_param_list(ONEWAYALLOC  *owa, struct context_param **param_list, RRDHOST *host, char *context, char *chart);
void free_temporary_host(RRDHOST *host);

// TODO: move to metadata
extern int update_node_id(uuid_t *host_id, uuid_t *node_id);
//void sql_store_host_labels(RRDHOST *host);

extern void invalidate_node_instances(uuid_t *host_id, uuid_t *claim_id);

// Provide statistics
extern int sql_metadata_cache_stats(int op);

#endif //NETDATA_SQLITE_FUNCTIONS_H
