/*
 * DB usage options.
 * Location and name of DB, type of DataSotre primary key type,
 */
#include <cassert>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "mdhim_constants.h"
#include "mdhim_options.h"
#include "mdhim_options_private.h"
#include "MemoryManagers.hpp"

// Default path to a local path and name, levelDB=2, int_key_type=1, yes_create_new=1
// and debug=1 (mlog_CRIT)

#define MANIFEST_FILE_NAME "/mdhim_manifest_"

static bool valid_opts(mdhim_options_t* opts) {
    return (opts && opts->p);
}

int mdhim_options_init(mdhim_options_t* opts) {
    if (!opts) {
        return MDHIM_ERROR;
    }

    opts->p = new mdhim_options_private_t();
    // Set default options
    mdhim_options_set_comm(opts, MPI_COMM_WORLD);
    mdhim_options_set_transporttype(opts, MDHIM_TRANSPORT_MPI);

    mdhim_options_set_db_path(opts, "./");
    mdhim_options_set_db_name(opts, "mdhimTstDB-");
    mdhim_options_set_manifest_path(opts, "./");
    mdhim_options_set_db_type(opts, LEVELDB);
    mdhim_options_set_key_type(opts, MDHIM_INT_KEY);
    mdhim_options_set_create_new_db(opts, 1);
    mdhim_options_set_value_append(opts, MDHIM_DB_OVERWRITE);
    mdhim_options_set_login_c(opts, "localhost", "test", "pass", "localhost", "test", "pass");

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
    return MDHIM_SUCCESS;
}

void mdhim_options_set_comm(mdhim_options_t* opts, const MPI_Comm comm) {
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->comm = comm;
}

void mdhim_options_set_transporttype(mdhim_options_t *opts, const int transporttype) {
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->transporttype = transporttype;
}

int check_path_length(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    int path_len;
    int ret = 0;

    path_len = strlen(path) + 1;
    if (((!opts->p->db_name && path_len < PATH_MAX) ||
         ((path_len + strlen(opts->p->db_name)) < PATH_MAX)) &&
        (path_len + strlen(MANIFEST_FILE_NAME)) < PATH_MAX) {
        ret = 1;
    } else {
        printf("Path: %s exceeds: %d bytes, so it won't be used\n", path, PATH_MAX);
    }

    return ret;
}

void mdhim_options_set_manifest_path(mdhim_options_t* opts, const char *path) {
    if (!valid_opts(opts)) {
        return;
    }

    if (opts->p->manifest_path) {
        delete [] opts->p->manifest_path;
        opts->p->manifest_path = nullptr;
    }

    const int path_len = strlen(path) + strlen(MANIFEST_FILE_NAME) + 1;
    char *manifest_path = new char[path_len]();
    sprintf(manifest_path, "%s%s", path, MANIFEST_FILE_NAME);
    opts->p->manifest_path = manifest_path;
}

void mdhim_options_set_login_c(mdhim_options_t* opts, const char* db_hl, const char *db_ln, const char *db_pw, const char *dbs_hl, const char *dbs_ln, const char *dbs_pw) {
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->db_host = db_hl;
    opts->p->db_user = db_ln;
    opts->p->db_upswd = db_pw;
    opts->p->dbs_host = dbs_hl;
    opts->p->dbs_user = dbs_ln;
    opts->p->dbs_upswd = dbs_pw;

}

void mdhim_options_set_db_path(mdhim_options_t* opts, const char *path)
{
    if (!valid_opts(opts)) {
        return;
    }

    int ret;

    if (!path) {
        return;
    }

    ret = check_path_length(opts, path);
    if (ret) {
        opts->p->db_path = path;
        mdhim_options_set_manifest_path(opts, path);
    }
}

void mdhim_options_set_db_paths(struct mdhim_options* opts, char **paths, int num_paths)
{
    if (!valid_opts(opts)) {
        return;
    }

    int i = 0;
    int ret;
    int verified_paths = -1;

    if (num_paths <= 0) {
        return;
    }

    opts->p->db_paths = new char *[num_paths]();
    for (i = 0; i < num_paths; i++) {
        if (!paths[i]) {
            continue;
        }

        ret = check_path_length(opts, paths[i]);
        if (!ret) {
            continue;
        }
        if (!i) {
            mdhim_options_set_manifest_path(opts, paths[i]);
        }

        verified_paths++;
        opts->p->db_paths[verified_paths] = new char[strlen(paths[i]) + 1]();
        sprintf(opts->p->db_paths[verified_paths], "%s", paths[i]);
    }

    opts->p->num_paths = ++verified_paths;
}

void mdhim_options_set_db_name(mdhim_options_t* opts, const char *name)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->db_name = name;
}

void mdhim_options_set_db_type(mdhim_options_t* opts, int type)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->db_type = type;
}

void mdhim_options_set_key_type(mdhim_options_t* opts, int key_type)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->db_key_type = key_type;
}

void mdhim_options_set_create_new_db(mdhim_options_t* opts, int create_new)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->db_create_new = create_new;
}

void mdhim_options_set_debug_level(mdhim_options_t* opts, int dbug)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->debug_level = dbug;
}

void mdhim_options_set_value_append(mdhim_options_t* opts, int append)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->db_value_append = append;
}

void mdhim_options_set_server_factor(mdhim_options_t* opts, int server_factor)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->rserver_factor = server_factor;
}

void mdhim_options_set_max_recs_per_slice(mdhim_options_t* opts, uint64_t max_recs_per_slice)
{
    if (!valid_opts(opts)) {
        return;
    }

    opts->p->max_recs_per_slice = max_recs_per_slice;
}

void mdhim_options_set_num_worker_threads(mdhim_options_t* opts, int num_wthreads)
{
    if (!valid_opts(opts)) {
        return;
    }

    if (num_wthreads > 0) {
        opts->p->num_wthreads = num_wthreads;
    }
}

int mdhim_options_destroy(mdhim_options_t *opts) {
    if (!valid_opts(opts)) {
        return MDHIM_ERROR;
    }

    for (int i = 0; i < opts->p->num_paths; i++) {
        delete [] opts->p->db_paths[i];
    }
    delete [] opts->p->db_paths;

    delete [] opts->p->manifest_path;
    delete opts->p;
    opts->p = nullptr;
    return MDHIM_SUCCESS;
}
