// SPDX-License-Identifier: GPL-3.0-or-later

#include "functions_evloop.h"

#define MAX_FUNCTION_PARAMETERS 1024

struct functions_evloop_worker_job {
    bool used;
    bool running;
    bool cancelled;
    usec_t stop_monotonic_ut;
    char *cmd;
    const char *transaction;
    time_t timeout;
    functions_evloop_worker_execute_t cb;
};

struct rrd_functions_expectation {
    const char *function;
    size_t function_length;
    functions_evloop_worker_execute_t cb;
    time_t default_timeout;
    struct rrd_functions_expectation *prev, *next;
};

struct functions_evloop_globals {
    const char *tag;

    DICTIONARY *worker_queue;
    pthread_mutex_t worker_mutex;
    pthread_cond_t worker_cond_var;
    size_t workers;

    netdata_mutex_t *stdout_mutex;
    bool *plugin_should_exit;

    netdata_thread_t reader_thread;
    netdata_thread_t *worker_threads;

    struct rrd_functions_expectation *expectations;
};

static void *rrd_functions_worker_globals_worker_main(void *arg) {
    struct functions_evloop_globals *wg = arg;

    bool last_acquired = true;
    while (true) {
        pthread_mutex_lock(&wg->worker_mutex);

        if(dictionary_entries(wg->worker_queue) == 0 || !last_acquired)
            pthread_cond_wait(&wg->worker_cond_var, &wg->worker_mutex);

        const DICTIONARY_ITEM *acquired = NULL;
        struct functions_evloop_worker_job *j;
        dfe_start_write(wg->worker_queue, j) {
            if(j->running || j->cancelled)
                continue;

            acquired = dictionary_acquired_item_dup(wg->worker_queue, j_dfe.item);
            j->running = true;
            break;
        }
        dfe_done(j);

        pthread_mutex_unlock(&wg->worker_mutex);

        if(acquired) {
            ND_LOG_STACK lgs[] = {
                    ND_LOG_FIELD_TXT(NDF_REQUEST, j->cmd),
                    ND_LOG_FIELD_END(),
            };
            ND_LOG_STACK_PUSH(lgs);

            last_acquired = true;
            j = dictionary_acquired_item_value(acquired);
            j->cb(j->transaction, j->cmd, &j->stop_monotonic_ut, &j->cancelled);
            dictionary_del(wg->worker_queue, j->transaction);
            dictionary_acquired_item_release(wg->worker_queue, acquired);
            dictionary_garbage_collect(wg->worker_queue);
        }
        else
            last_acquired = false;
    }
    return NULL;
}

static void *rrd_functions_worker_globals_reader_main(void *arg) {
    struct functions_evloop_globals *wg = arg;

    char buffer[PLUGINSD_LINE_MAX + 1];

    char *s = NULL;
    while(!(*wg->plugin_should_exit) && (s = fgets(buffer, PLUGINSD_LINE_MAX, stdin))) {

        char *words[MAX_FUNCTION_PARAMETERS] = { NULL };
        size_t num_words = quoted_strings_splitter_pluginsd(buffer, words, MAX_FUNCTION_PARAMETERS);

        const char *keyword = get_word(words, num_words, 0);

        if(keyword && strcmp(keyword, PLUGINSD_KEYWORD_FUNCTION) == 0) {
            char *transaction = get_word(words, num_words, 1);
            char *timeout_s = get_word(words, num_words, 2);
            char *function = get_word(words, num_words, 3);

            if(!transaction || !*transaction || !timeout_s || !*timeout_s || !function || !*function) {
                nd_log(NDLS_COLLECTORS, NDLP_ERR, "Received incomplete %s (transaction = '%s', timeout = '%s', function = '%s'). Ignoring it.",
                                  keyword,
                                  transaction?transaction:"(unset)",
                                  timeout_s?timeout_s:"(unset)",
                                  function?function:"(unset)");
            }
            else {
                int timeout = str2i(timeout_s);

                const char *msg = "No function with this name found";
                bool found = false;
                struct rrd_functions_expectation *we;
                for(we = wg->expectations; we ;we = we->next) {
                    if(strncmp(function, we->function, we->function_length) == 0) {
                        if(timeout <= 0)
                            timeout = (int)we->default_timeout;

                        struct functions_evloop_worker_job t = {
                                .cmd = strdupz(function),
                                .transaction = strdupz(transaction),
                                .running = false,
                                .cancelled = false,
                                .timeout = timeout,
                                .stop_monotonic_ut = now_monotonic_usec() + (timeout * USEC_PER_SEC),
                                .used = false,
                                .cb = we->cb,
                        };
                        struct functions_evloop_worker_job *j = dictionary_set(wg->worker_queue, transaction, &t, sizeof(t));
                        if(j->used) {
                            nd_log(NDLS_COLLECTORS, NDLP_WARNING, "Received duplicate function transaction '%s'. Ignoring it.", transaction);
                            freez((void *)t.cmd);
                            freez((void *)t.transaction);
                            msg = "Duplicate function transaction. Ignoring it.";
                        }
                        else {
                            found = true;
                            j->used = true;
                            pthread_cond_signal(&wg->worker_cond_var);
                        }
                    }
                }

                if(!found) {
                    netdata_mutex_lock(wg->stdout_mutex);
                    pluginsd_function_json_error_to_stdout(transaction, HTTP_RESP_NOT_FOUND, msg);
                    netdata_mutex_unlock(wg->stdout_mutex);
                }
            }
        }
        else if(keyword && strcmp(keyword, PLUGINSD_KEYWORD_FUNCTION_CANCEL) == 0) {
            char *transaction = get_word(words, num_words, 1);
            const DICTIONARY_ITEM *acquired = dictionary_get_and_acquire_item(wg->worker_queue, transaction);
            if(acquired) {
                struct functions_evloop_worker_job *j = dictionary_acquired_item_value(acquired);
                __atomic_store_n(&j->cancelled, true, __ATOMIC_RELAXED);
                dictionary_acquired_item_release(wg->worker_queue, acquired);
                dictionary_del(wg->worker_queue, transaction);
                dictionary_garbage_collect(wg->worker_queue);
            }
            else
                nd_log(NDLS_COLLECTORS, NDLP_NOTICE, "Received CANCEL for transaction '%s', but it not available here", transaction);
        }
        else if(keyword && strcmp(keyword, PLUGINSD_KEYWORD_FUNCTION_PROGRESS) == 0) {
            char *transaction = get_word(words, num_words, 1);
            const DICTIONARY_ITEM *acquired = dictionary_get_and_acquire_item(wg->worker_queue, transaction);
            if(acquired) {
                struct functions_evloop_worker_job *j = dictionary_acquired_item_value(acquired);

                functions_stop_monotonic_update_on_progress(&j->stop_monotonic_ut);

                dictionary_acquired_item_release(wg->worker_queue, acquired);
            }
            else
                nd_log(NDLS_COLLECTORS, NDLP_NOTICE, "Received PROGRESS for transaction '%s', but it not available here", transaction);
        }
        else
            nd_log(NDLS_COLLECTORS, NDLP_NOTICE, "Received unknown command: %s", keyword?keyword:"(unset)");
    }

    if(!s || feof(stdin) || ferror(stdin)) {
        *wg->plugin_should_exit = true;
        nd_log(NDLS_COLLECTORS, NDLP_ERR, "Received error on stdin.");
    }

    exit(1);
}

void worker_queue_delete_cb(const DICTIONARY_ITEM *item __maybe_unused, void *value, void *data __maybe_unused) {
    struct functions_evloop_worker_job *j = value;
    freez((void *)j->cmd);
    freez((void *)j->transaction);
}

struct functions_evloop_globals *functions_evloop_init(size_t worker_threads, const char *tag, netdata_mutex_t *stdout_mutex, bool *plugin_should_exit) {
    struct functions_evloop_globals *wg = callocz(1, sizeof(struct functions_evloop_globals));

    wg->worker_queue = dictionary_create(DICT_OPTION_DONT_OVERWRITE_VALUE);
    dictionary_register_delete_callback(wg->worker_queue, worker_queue_delete_cb, NULL);

    pthread_mutex_init(&wg->worker_mutex, NULL);
    pthread_cond_init(&wg->worker_cond_var, NULL);

    wg->plugin_should_exit = plugin_should_exit;
    wg->stdout_mutex = stdout_mutex;
    wg->workers = worker_threads;
    wg->worker_threads = callocz(wg->workers, sizeof(netdata_thread_t ));
    wg->tag = tag;

    char tag_buffer[NETDATA_THREAD_TAG_MAX + 1];
    snprintfz(tag_buffer, NETDATA_THREAD_TAG_MAX, "%s_READER", wg->tag);
    netdata_thread_create(&wg->reader_thread, tag_buffer, NETDATA_THREAD_OPTION_DONT_LOG,
                          rrd_functions_worker_globals_reader_main, wg);

    for(size_t i = 0; i < wg->workers ; i++) {
        snprintfz(tag_buffer, NETDATA_THREAD_TAG_MAX, "%s_WORK[%zu]", wg->tag, i+1);
        netdata_thread_create(&wg->worker_threads[i], tag_buffer, NETDATA_THREAD_OPTION_DONT_LOG,
                              rrd_functions_worker_globals_worker_main, wg);
    }

    return wg;
}

void functions_evloop_add_function(struct functions_evloop_globals *wg, const char *function, functions_evloop_worker_execute_t cb, time_t default_timeout) {
    struct rrd_functions_expectation *we = callocz(1, sizeof(*we));
    we->function = function;
    we->function_length = strlen(we->function);
    we->cb = cb;
    we->default_timeout = default_timeout;
    DOUBLE_LINKED_LIST_APPEND_ITEM_UNSAFE(wg->expectations, we, prev, next);
}

void functions_evloop_cancel_threads(struct functions_evloop_globals *wg){
    for(size_t i = 0; i < wg->workers ; i++)
        netdata_thread_cancel(wg->worker_threads[i]);

    netdata_thread_cancel(wg->reader_thread);
}
