/*
 * DB usage options.
 * Location and name of DB, type of DataSotre primary key type,
 */
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>

#include "mdhim_constants.h"
#include "mdhim_options.h"
#include "mdhim_options_private.h"
#include "mdhim_config.hpp"

#define MANIFEST_FILE_NAME "/mdhim_manifest_"

static bool valid_opts(mdhim_options_t* opts) {
    return (opts && opts->p && opts->p->transport && opts->p->db);
}

int mdhim_options_init(mdhim_options_t* opts) {
    if (!opts) {
        return MDHIM_ERROR;
    }

    // Initialize opts->p
    opts->p = new mdhim_options_private_t();
    opts->p->transport = new mdhim_transport_options();
    opts->p->db = new mdhim_db_options();

    // // Set default options
    // mdhim_options_set_transport(opts, MDHIM_TRANSPORT_NONE, nullptr);

    // mdhim_options_set_db_path(opts, "./");
    // mdhim_options_set_db_name(opts, "mdhimTstDB-");
    // mdhim_options_set_manifest_path(opts, "./");
    // mdhim_options_set_db_type(opts, LEVELDB);
    // mdhim_options_set_key_type(opts, MDHIM_INT_KEY);
    // mdhim_options_set_create_new_db(opts, 1);
    // mdhim_options_set_value_append(opts, MDHIM_DB_OVERWRITE);
    // mdhim_options_set_login_c(opts, "localhost", "test", "pass", "localhost", "test", "pass");

    // mdhim_options_set_debug_level(opts, 1);
    // mdhim_options_set_server_factor(opts, 1);
    // mdhim_options_set_max_recs_per_slice(opts, 100000);
    // mdhim_options_set_db_paths(opts, nullptr, 0);
    // mdhim_options_set_num_worker_threads(opts, 1);

    // Hugh's settings for his test configuration
    //mdhim_options_set_db_path(opts, "/tmp/mdhim/");
    //mdhim_options_set_db_name(opts, "mdhimDb");
    //mdhim_options_set_db_type(opts, LEVELDB);
    //mdhim_options_set_server_factor(opts, 1);
    //mdhim_options_set_max_recs_per_slice(opts, 1000);
    //mdhim_options_set_key_type(opts, MDHIM_BYTE_KEY);
    //mdhim_options_set_debug_level(opts, MLOG_CRIT);
    //mdhim_options_set_num_worker_threads(opts, 30);
    return MDHIM_SUCCESS;
}

int mdhim_options_set_transport(mdhim_options_t *opts, const int type, void *data) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    if (opts->p->transport->data) {
        switch (opts->p->transport->type) {
            case MDHIM_TRANSPORT_MPI:
                delete static_cast<MPIOptions_t *>(opts->p->transport->data);
                break;
            case MDHIM_TRANSPORT_THALLIUM:
                delete static_cast<std::string *>(opts->p->transport->data);
                break;
            default:
                break;
        }
    }

    opts->p->transport->type = type;
    opts->p->transport->data = data;

    return MDHIM_SUCCESS;
}

int check_path_length(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    int path_len;
    int ret = 0;

    path_len = strlen(path) + 1;
    if (((!opts->p->db->name && path_len < PATH_MAX) ||
         ((path_len + strlen(opts->p->db->name)) < PATH_MAX)) &&
        (path_len + strlen(MANIFEST_FILE_NAME)) < PATH_MAX) {
        ret = 1;
    } else {
        printf("Path: %s exceeds: %d bytes, so it won't be used\n", path, PATH_MAX);
    }

    return ret;
}

int mdhim_options_set_manifest_path(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    if (opts->p->db->manifest_path) {
        delete [] opts->p->db->manifest_path;
        opts->p->db->manifest_path = nullptr;
    }

    const int path_len = strlen(path) + strlen(MANIFEST_FILE_NAME) + 1;
    char *manifest_path = new char[path_len]();
    sprintf(manifest_path, "%s%s", path, MANIFEST_FILE_NAME);
    opts->p->db->manifest_path = manifest_path;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_host(mdhim_options_t* opts, const char* db_hl) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->db_host = db_hl;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_login(mdhim_options_t* opts, const char *db_ln) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->db_user = db_ln;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_password(mdhim_options_t* opts, const char *db_pw) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->db_upswd = db_pw;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_dbs_host(mdhim_options_t* opts, const char* dbs_hl) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->dbs_host = dbs_hl;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_dbs_login(mdhim_options_t* opts, const char *dbs_ln) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->dbs_user = dbs_ln;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_dbs_password(mdhim_options_t* opts, const char *dbs_pw) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->dbs_upswd = dbs_pw;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_login_c(mdhim_options_t* opts, const char* db_hl, const char *db_ln, const char *db_pw, const char *dbs_hl, const char *dbs_ln, const char *dbs_pw) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    mdhim_options_set_db_host(opts, db_hl);
    mdhim_options_set_db_login(opts, db_ln);
    mdhim_options_set_db_password(opts, db_pw);
    mdhim_options_set_db_host(opts, dbs_hl);
    mdhim_options_set_db_login(opts, dbs_ln);
    mdhim_options_set_db_password(opts, dbs_pw);

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_path(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    if (!path) {
        return MDHIM_ERROR;
    }

    if (check_path_length(opts, path)) {
        opts->p->db->path = path;
        mdhim_options_set_manifest_path(opts, path);
    }

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_paths(struct mdhim_options* opts, char **paths, int num_paths) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    int ret;
    int verified_paths = -1;

    if (num_paths <= 0) {
        return MDHIM_SUCCESS;
    }

    opts->p->db->paths = new char *[num_paths]();
    for (int i = 0; i < num_paths; i++) {
        if (!paths[i]) {
            continue;
        }

        if (!check_path_length(opts, paths[i])) {
            continue;
        }

        if (!i) {
            mdhim_options_set_manifest_path(opts, paths[i]);
        }

        verified_paths++;
        opts->p->db->paths[verified_paths] = new char[strlen(paths[i]) + 1]();
        sprintf(opts->p->db->paths[verified_paths], "%s", paths[i]);
    }

    opts->p->db->num_paths = ++verified_paths;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_name(mdhim_options_t* opts, const char *name) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->name = name;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_type(mdhim_options_t* opts, int type) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->type = type;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_key_type(mdhim_options_t* opts, int key_type) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->key_type = key_type;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_create_new_db(mdhim_options_t* opts, int create_new) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->create_new = create_new;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_debug_level(mdhim_options_t* opts, int dbug) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->debug_level = dbug;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_value_append(mdhim_options_t* opts, int append) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->value_append = append;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_server_factor(mdhim_options_t* opts, int server_factor) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->rserver_factor = server_factor;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_max_recs_per_slice(mdhim_options_t* opts, uint64_t max_recs_per_slice) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->max_recs_per_slice = max_recs_per_slice;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_num_worker_threads(mdhim_options_t* opts, int num_wthreads) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    if (num_wthreads > 0) {
        opts->p->db->num_wthreads = num_wthreads;
    }

    return MDHIM_SUCCESS;
}

int mdhim_options_destroy(mdhim_options_t *opts) {
    if (!opts) {
        return MDHIM_ERROR;
    }

    if (opts->p) {
        if (opts->p->db) {
            for (int i = 0; i < opts->p->db->num_paths; i++) {
                delete [] opts->p->db->paths[i];
            }
            delete [] opts->p->db->paths;

            delete [] opts->p->db->manifest_path;
            delete opts->p->db;
        }

        if (opts->p->transport) {
            mdhim_options_set_transport(opts, MDHIM_TRANSPORT_NONE, nullptr);
            delete opts->p->transport;
        }

        delete opts->p;
        opts->p = nullptr;
    }

    return MDHIM_SUCCESS;
}
