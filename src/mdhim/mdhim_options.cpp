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

/**
 * valid_opts
 * This is a common check that most functions call.
 * opts->p->transport is not checked because it is
 * not set here.
 *
 * @param opts the options struct to check
 * @return whether or not the options struct can be filled
 */
static bool valid_opts(mdhim_options_t* opts) {
    return (opts && opts->p && opts->p->db);
}

/**
 * update_c_str
 * Utility function for deleting and creating
 * a new string at an address. *dst is expected
 * to be either null or a valid address, which
 * will be deleted and overwritten.
 *
 * @param src the source string
 * @param dst the address of the destination string
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
static int update_c_str(const char *src, char **dst) {
    if (!src || !dst) {
        return MDHIM_ERROR;
    }

    delete [] *dst;

    const size_t len = strlen(src);
    if (!(*dst = new char[len + 1]())) {
        return MDHIM_ERROR;
    }

    memcpy((void *)*dst, (const void *)src, len);

    return MDHIM_SUCCESS;
}

/**
 * mdhim_options_init
 * Initializes the private pointer of an mdhim_option_t.
 * The values inside opts->p are initialized to 0 only, not allocated
 * The transport and database pointers should be set
 * by their respective functions.
 *
 * @param opts         the options struct to initialize
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int mdhim_options_init(mdhim_options_t* opts) {
    if (!opts) {
        return MDHIM_ERROR;
    }

    if (!(opts->p = new mdhim_options_private_t())) {
        return MDHIM_ERROR;
    }

    opts->p->transport = nullptr;
    opts->p->db = nullptr;
    return MDHIM_SUCCESS;
}

/**
 * mdhim_options_init_db
 * Initializes the database portion of an mdhim_options_t
 * variable. Additionally fills in the struct, if requested.
 *
 * @param opts         the options struct to fill
 * @param set_defaults whether or not to fill the options struct with default values
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int mdhim_options_init_db(mdhim_options_t* opts, int set_defaults) {
    if (!opts || !opts->p) {
        return MDHIM_ERROR;
    }

    delete opts->p->db;
    if (!(opts->p->db = new mdhim_db_options())) {
        return MDHIM_ERROR;
    }

    // Set default options
    if (set_defaults) {
        mdhim_options_set_db_path(opts, "./");
        mdhim_options_set_db_name(opts, "mdhimTstDB-");
        mdhim_options_set_manifest_path(opts, "./");
        mdhim_options_set_db_type(opts, LEVELDB);
        mdhim_options_set_key_type(opts, MDHIM_INT_KEY);
        mdhim_options_set_create_new_db(opts, 1);
        mdhim_options_set_value_append(opts, MDHIM_DB_OVERWRITE);
        mdhim_options_set_login_c(opts,
                                  "localhost", "test", "pass",
                                  "localhost", "test", "pass");

        mdhim_options_set_debug_level(opts, 1);
        mdhim_options_set_server_factor(opts, 1);
        mdhim_options_set_max_recs_per_slice(opts, 100000);
        mdhim_options_set_db_paths(opts, nullptr, 0);
        mdhim_options_set_num_worker_threads(opts, 1);

        // Hugh's settings for his test configuration
        //mdhim_options_set_db_path(opts, "/tmp/mdhim/");
        //mdhim_options_set_db_name(opts, "mdhimDb");
        //mdhim_options_set_db_type(opts, LEVELDB);
        //mdhim_options_set_server_factor(opts, 1);
        //mdhim_options_set_max_recs_per_slice(opts, 1000);
        //mdhim_options_set_key_type(opts, MDHIM_BYTE_KEY);
        //mdhim_options_set_debug_level(opts, MLOG_CRIT);
        //mdhim_options_set_num_worker_threads(opts, 30);
    }

    return MDHIM_SUCCESS;
}

int mdhim_options_set_manifest_path(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    const int path_len = strlen(path) + strlen(MANIFEST_FILE_NAME) + 1;
    char *manifest_path = new char[path_len]();
    if (!manifest_path) {
        return MDHIM_ERROR;
    }

    sprintf(manifest_path, "%s%s", path, MANIFEST_FILE_NAME);

    int ret = update_c_str(manifest_path, &opts->p->db->manifest_path);
    delete [] manifest_path;
    return ret;
}

int mdhim_options_set_db_host(mdhim_options_t* opts, const char* db_hl) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(db_hl, &opts->p->db->db_host);
}

int mdhim_options_set_db_login(mdhim_options_t* opts, const char *db_ln) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(db_ln, &opts->p->db->db_user);
}

int mdhim_options_set_db_password(mdhim_options_t* opts, const char *db_pw) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(db_pw, &opts->p->db->db_upswd);
}

int mdhim_options_set_dbs_host(mdhim_options_t* opts, const char* dbs_hl) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(dbs_hl, &opts->p->db->dbs_host);
}

int mdhim_options_set_dbs_login(mdhim_options_t* opts, const char *dbs_ln) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(dbs_ln, &opts->p->db->dbs_user);
}

int mdhim_options_set_dbs_password(mdhim_options_t* opts, const char *dbs_pw) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(dbs_pw, &opts->p->db->dbs_upswd);
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

static int check_path_length(mdhim_options_t* opts, const char *path) {
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

int mdhim_options_set_db_path(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    if (!path) {
        return MDHIM_ERROR;
    }

    if (check_path_length(opts, path)) {
        update_c_str(path, &opts->p->db->path);
        mdhim_options_set_manifest_path(opts, path);
    }

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_paths(struct mdhim_options* opts, char **paths, const int num_paths) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

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
        update_c_str(paths[i], &opts->p->db->paths[verified_paths]);
    }

    opts->p->db->num_paths = ++verified_paths;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_db_name(mdhim_options_t* opts, const char *name) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    return update_c_str(name, &opts->p->db->name);
}

int mdhim_options_set_db_type(mdhim_options_t* opts, const int type) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->type = type;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_key_type(mdhim_options_t* opts, const int key_type) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->key_type = key_type;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_create_new_db(mdhim_options_t* opts, const int create_new) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->create_new = create_new;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_debug_level(mdhim_options_t* opts, const int dbug) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->debug_level = dbug;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_value_append(mdhim_options_t* opts, const int append) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->value_append = append;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_server_factor(mdhim_options_t* opts, const int server_factor) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->rserver_factor = server_factor;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_max_recs_per_slice(mdhim_options_t* opts, const uint64_t max_recs_per_slice) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    opts->p->db->max_recs_per_slice = max_recs_per_slice;

    return MDHIM_SUCCESS;
}

int mdhim_options_set_num_worker_threads(mdhim_options_t* opts, const int num_wthreads) {
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
            delete [] opts->p->db->path;

            for (int i = 0; i < opts->p->db->num_paths; i++) {
                delete [] opts->p->db->paths[i];
            }
            delete [] opts->p->db->paths;

            delete [] opts->p->db->manifest_path;
            delete [] opts->p->db->name;

            delete [] opts->p->db->db_host;
            delete [] opts->p->db->dbs_host;
            delete [] opts->p->db->db_user;
            delete [] opts->p->db->db_upswd;
            delete [] opts->p->db->dbs_user;
            delete [] opts->p->db->dbs_upswd;
        }

        delete opts->p->db;
        delete opts->p->transport;
        delete opts->p;
        opts->p = nullptr;
    }

    return MDHIM_SUCCESS;
}
