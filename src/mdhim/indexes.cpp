#include <cctype>
#include <cinttypes>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "mdhim_private.h"
#include "mdhim_options_private.h"
#include "indexes.h"
#include "partitioner.h"

/**
 * im_range_server
 * checks if I'm a range server
 *
 * @param md  Pointer to the main MDHIM structure
 * @return 0 if false, 1 if true
 */
int im_range_server(index_t *index) {
    return (index->myinfo.rangesrv_num > 0);
}

/**
 * get_manifest_name
 * returns a pointer to the manifest name
 *
 * @param md       Pointer to the main MDHIM structure
 * @param index    the index to write
 */
char *get_manifest_name(mdhim_t *md, index_t *index) {
    char *path = new char[PATH_MAX]();
    if (path) {
        sprintf(path, "%s%d_%d_%d", md->p->db_opts->manifest_path, index->type, index->id, md->rank);
    }
    return path;
}

/**
 * write_manifest
 * Writes out the manifest file
 *
 * @param md       Pointer to the main MDHIM structure
 */
static void write_manifest(mdhim_t *md, index_t *index) {
    //Range server with range server number 1, for the primary index, is in charge of the manifest
    if (index->type != LOCAL_INDEX &&
        (index->myinfo.rangesrv_num != 1)) {
        return;
    }

    const char *manifest_name = get_manifest_name(md, index);
    if (!manifest_name) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error getting manifest file name",
             md->rank);
        return;
    }

    std::ofstream manifest(manifest_name);
    delete [] manifest_name;
    if (!manifest) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error opening manifest file",
             md->rank);
        return;
    }

    if (!(manifest
          << index->key_type << std::endl
          << index->db_type << std::endl
          << get_num_databases(md->size, index->range_server_factor, index->dbs_per_server) << std::endl
          << index->slice_size << std::endl
          << md->p->db_opts->histogram.min << std::endl
          << md->p->db_opts->histogram.step_size << std::endl
          << md->p->db_opts->histogram.count << std::endl)) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error writing manifest file",
             md->rank);
    }
}

/**
 * read_manifest
 * Reads in and validates the manifest file
 *
 * @param md       Pointer to the main MDHIM structure
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int read_manifest(mdhim_t *md, index_t *index) {
    const char *manifest_name = get_manifest_name(md, index);
    if (!manifest_name) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Error getting manifest file name",
             md->rank);
        return MDHIM_ERROR;
    }

    std::ifstream manifest(manifest_name);
    delete [] manifest_name;
    if (!manifest) {
        mlog(MDHIM_SERVER_DBG, "Rank %d - Error opening manifest file",
             md->rank);
        return MDHIM_SUCCESS;
    }

    int key_type, db_type, databases;
    uint64_t slice_size;
    long double hist_min, hist_step;
    std::size_t hist_count;
    if (!(manifest >> key_type >> db_type >> databases >> slice_size
                   >> hist_min >> hist_step >> hist_count)) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Couldn't read manifest file",
             md->rank);
        return MDHIM_ERROR;
    }

    int ret = MDHIM_SUCCESS;
    mlog(MDHIM_SERVER_DBG, "Rank %d - Manifest contents - \nkey_type: %d, "
         "db_type: %d, slice_size: %" PRIu64 ", databases: %d",
         md->rank, key_type, db_type,
         slice_size, databases);

    //Check that the manifest and the current config match
    if (key_type != index->key_type) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - The key type in the manifest file"
             " doesn't match the current key type",
             md->rank);
        ret = MDHIM_ERROR;
    }

    if (db_type != index->db_type) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - The database type in the manifest file"
             " doesn't match the current database type",
             md->rank);
        ret = MDHIM_ERROR;
    }

    if (slice_size != index->slice_size) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - The slice size in the manifest file (%" PRIu64 ")"
             " doesn't match the current slice size (%" PRIu64 ")",
             md->rank, slice_size, index->slice_size);
        ret = MDHIM_ERROR;
    }

    // the current number of databases should be the same as the previous size
    const int curr_dbs = get_num_databases(md->size, index->range_server_factor, index->dbs_per_server);
    if (databases != curr_dbs) {
        mlog(MDHIM_SERVER_INFO, "Rank %d - The number of databases in this MDHIM instance (%d)"
             " doesn't match the number used previously (%d)",
             md->rank, curr_dbs, databases);
        ret = MDHIM_ERROR;
    }

    if (std::fabs(hist_min - md->p->db_opts->histogram.min) > 1e-6) {
        mlog(MDHIM_SERVER_INFO, "Rank %d - The histogram minimum value in this MDHIM instance (%Le)"
             " doesn't match the number used previously (%Le)",
             md->rank, hist_min, md->p->db_opts->histogram.min);
        ret = MDHIM_ERROR;
    }

    if (std::fabs(hist_step - md->p->db_opts->histogram.step_size) > 1e-6) {
        mlog(MDHIM_SERVER_INFO, "Rank %d - The histogram step size in this MDHIM instance (%Le)"
             " doesn't match the number used previously (%Le)",
             md->rank, hist_step, md->p->db_opts->histogram.step_size);
        ret = MDHIM_ERROR;
    }

    if (hist_count != md->p->db_opts->histogram.count) {
        mlog(MDHIM_SERVER_INFO, "Rank %d - The histogram bucket count in this MDHIM instance (%zu)"
             " doesn't match the number used previously (%zu)",
             md->rank, hist_count, md->p->db_opts->histogram.count);
        ret = MDHIM_ERROR;
    }
    return ret;
}

/**
 * update_stat
 * Adds or updates the given stat to the hash table
 *
 * @param md       pointer to the main MDHIM structure
 * @param rs_idx   the range server index
 * @param key      pointer to the key we are examining
 * @param key_len  the key's length
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int update_stat(mdhim_t *md, index_t *index, const int rs_idx, void *key, std::size_t key_len) {
    //Acquire the lock to update the stats
    while (pthread_rwlock_wrlock(&index->mdhim_stores[rs_idx]->mdhim_store_stats_lock) == EBUSY) {
        usleep(10);
    }

    const int float_type = is_float_key(index->key_type);
    void *val1, *val2;
    if (float_type == 1) {
        val1 = (void *) malloc(sizeof(long double));
        val2 = (void *) malloc(sizeof(long double));
    } else {
        val1 = (void *) malloc(sizeof(uint64_t));
        val2 = (void *) malloc(sizeof(uint64_t));
    }

    if (index->key_type == MDHIM_STRING_KEY) {
        *(long double *)val1 = get_str_num(key, key_len);
        *(long double *)val2 = *(long double *)val1;
    } else if (index->key_type == MDHIM_FLOAT_KEY) {
        *(long double *)val1 = *(float *) key;
        *(long double *)val2 = *(float *) key;
    } else if (index->key_type == MDHIM_DOUBLE_KEY) {
        *(long double *)val1 = *(double *) key;
        *(long double *)val2 = *(double *) key;
    } else if (index->key_type == MDHIM_INT_KEY) {
        *(uint64_t *)val1 = *(uint32_t *) key;
        *(uint64_t *)val2 = *(uint32_t *) key;
    } else if (index->key_type == MDHIM_LONG_INT_KEY) {
        *(uint64_t *)val1 = *(uint64_t *) key;
        *(uint64_t *)val2 = *(uint64_t *) key;
    } else if ((index->key_type == MDHIM_BYTE_KEY)     ||
               (index->key_type == MDHIM_LEX_BYTE_KEY)) {
        *(long double *)val1 = get_byte_num(key, key_len);
        *(long double *)val2 = *(long double *)val1;
    }

    const uint64_t slice_num = get_slice_num(index->key_type, index->slice_size, key, key_len);

    mdhim_stat_t *os = nullptr;;
    HASH_FIND_INT(index->mdhim_stores[rs_idx]->mdhim_store_stats, &slice_num, os);

    mdhim_stat_t *stat = (mdhim_stat_t *)malloc(sizeof(mdhim_stat_t));
    stat->min = val1;
    stat->max = val2;
    stat->num = 1;
    stat->key = slice_num;
    stat->dirty = 1;
    if (os) {
        if (float_type) {
            if (*(long double *)os->min > *(long double *)val1) {
                free(os->min);
                stat->min = val1;
            } else {
                free(val1);
                stat->min = os->min;
            }

            if (*(long double *)os->max < *(long double *)val2) {
                free(os->max);
                stat->max = val2;
            } else {
                free(val2);
                stat->max = os->max;
            }
        }
        else {
            if (*(uint64_t *)os->min > *(uint64_t *)val1) {
                free(os->min);
                stat->min = val1;
            } else {
                free(val1);
                stat->min = os->min;
            }

            if (*(uint64_t *)os->max < *(uint64_t *)val2) {
                free(os->max);
                stat->max = val2;
            } else {
                free(val2);
                stat->max = os->max;
            }
        }

        stat->num = os->num + 1;
        //Replace the existing stat
        HASH_REPLACE_INT(index->mdhim_stores[rs_idx]->mdhim_store_stats, key, stat, os);
        // do not free os->min and os->max here
        free(os);
    }
    else {
        HASH_ADD_INT(index->mdhim_stores[rs_idx]->mdhim_store_stats, key, stat);
    }

    //Release the stats lock
    pthread_rwlock_unlock(&index->mdhim_stores[rs_idx]->mdhim_store_stats_lock);
    return MDHIM_SUCCESS;
}

/**
 * load_stat
 * Loads the statistics from the database
 *
 * @param md  Pointer to the main MDHIM structure
 * @param rs_idx   the range server index
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int load_stat(mdhim_t *md, index_t *index, const int rs_idx) {
    void **val;
    std::size_t *val_len, *key_len;
    uint64_t **slice;
    uint64_t *old_slice;
    mdhim_stat_t *stat;
    int float_type = 0;
    void *min, *max;
    int done = 0;

    float_type = is_float_key(index->key_type);
    slice = (uint64_t**)malloc(sizeof(uint64_t *));
    *slice = nullptr;
    key_len = (std::size_t*)malloc(sizeof(std::size_t));
    *key_len = sizeof(**slice);
    // BWS This line looks like it was bugged, fixing it without a test
    val = (void**)malloc(sizeof(mdhim_db_stat_t*));
    val_len = (std::size_t*)malloc(sizeof(std::size_t));
    old_slice = nullptr;
    index->mdhim_stores[rs_idx]->mdhim_store_stats = nullptr;

    while (!done) {
        //Check the db for the key/value
        *val = nullptr;
        *val_len = 0;
        index->mdhim_stores[rs_idx]->get_next(index->mdhim_stores[rs_idx]->db_stats,
                                             (void **) slice, key_len, (void **) val,
                                             val_len);

        //Add the stat to the hash table - the value is 0 if the key was not in the db
        if (!*val || !*val_len) {
            done = 1;
            free(*val);
            continue;
        }

        if (old_slice) {
            free(old_slice);
            old_slice = nullptr;
        }

        mlog(MDHIM_SERVER_INFO, "Rank %d - Loaded stat for slice: %" PRIu64 " with "
             "imin: %lu and imax: %lu, dmin: %Lf, dmax: %Lf, and num: %lu",
             md->rank, **slice, (*(mdhim_db_stat_t **)val)->imin,
             (*(mdhim_db_stat_t **)val)->imax, (*(mdhim_db_stat_t **)val)->dmin,
             (*(mdhim_db_stat_t **)val)->dmax, (*(mdhim_db_stat_t **)val)->num);

        stat = (mdhim_stat*)malloc(sizeof(mdhim_stat_t));
        if (float_type) {
            min = (void *) malloc(sizeof(long double));
            max = (void *) malloc(sizeof(long double));
            *(long double *)min = (*(mdhim_db_stat_t **)val)->dmin;
            *(long double *)max = (*(mdhim_db_stat_t **)val)->dmax;
        } else {
            min = (void *) malloc(sizeof(uint64_t));
            max = (void *) malloc(sizeof(uint64_t));
            *(uint64_t *)min = (*(mdhim_db_stat_t **)val)->imin;
            *(uint64_t *)max = (*(mdhim_db_stat_t **)val)->imax;
        }

        stat->min = min;
        stat->max = max;
        stat->num = (*(mdhim_db_stat_t **)val)->num;
        stat->key = **slice;
        stat->dirty = 0;
        old_slice = *slice;
        HASH_ADD_INT(index->mdhim_stores[rs_idx]->mdhim_store_stats, key, stat);
        free(*val);
    }

    if (old_slice) {
        free(old_slice);
    }

    free(val);
    free(val_len);
    free(key_len);
    free(*slice);
    free(slice);
    return MDHIM_SUCCESS;
}

/**
 * write_stat
 * Writes the statistics stored in a hash table to the database
 * This is done on mdhim_close
 *
 * @param md  Pointer to the main MDHIM structure
 * @param rs_idx   the range server index
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int write_stat(mdhim_t *md, index_t *bi, const int rs_idx) {
    mdhim_stat_t *stat, *tmp;
    mdhim_db_stat_t *dbstat;
    int float_type = is_float_key(bi->key_type);

    //Iterate through the stat hash entries
    HASH_ITER(hh, bi->mdhim_stores[rs_idx]->mdhim_store_stats, stat, tmp) {
        if (!stat || !stat->dirty) {
            continue;
        }

        dbstat = (mdhim_db_stat_t*)calloc(1, sizeof(mdhim_db_stat_t));
        if (float_type) {
            dbstat->dmax = *(long double *)stat->max;
            dbstat->dmin = *(long double *)stat->min;
            dbstat->imax = 0;
            dbstat->imin = 0;
        } else {
            dbstat->imax = *(uint64_t *)stat->max;
            dbstat->imin = *(uint64_t *)stat->min;
            dbstat->dmax = 0;
            dbstat->dmin = 0;
        }

        dbstat->slice = stat->key;
        dbstat->num = stat->num;
        //Write the key to the database
        bi->mdhim_stores[rs_idx]->put(bi->mdhim_stores[rs_idx]->db_stats,
                                     &dbstat->slice, sizeof(int), dbstat,
                                     sizeof(mdhim_db_stat_t));
        //Delete and free hash entry
        free(dbstat);
    }

    return MDHIM_SUCCESS;
}

/**
 * open_db_store
 * Opens a single databases for the given index
 *
 * @param md     Pointer to the main MDHIM structure
 * @param index  Pointer to the index
 * @param path   The path of the database file
 * @param rs_idx The staring database id
 * @param offset The offset from the starting database id
 * @param ret    MDHIM_SUCCESS or MDHIM_ERROR
 */
static void open_db_store(mdhim_t *md, index_t *index, const char *path, const int rs_index, const int offset, int &ret) {
    mdhim_store_t **mdhim_store = &index->mdhim_stores[offset];

    //Database filenames are dependent on the selected path, name, range server factor, rank, and the number of databses per range server
    char filename[PATH_MAX] = {0};
    sprintf(filename, "%s/%s-%d", path, md->p->db_opts->name, rs_index + offset);

    //Initialize data store
    if (!(*mdhim_store = mdhim_db_init(index->db_type))) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while initializing data store with file: %s",
             md->rank,
             filename);
        ret = MDHIM_ERROR;
        return;
    }

    //Open the main database and the stats database
    int flags = MDHIM_CREATE;
    if ((*mdhim_store)->open(&(*mdhim_store)->db_handle,
                             &(*mdhim_store)->db_stats,
                             filename, flags, index->key_type,
                             md->p->db_opts) != MDHIM_SUCCESS){
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while opening database %s",
             md->rank, filename);
        ret = MDHIM_ERROR;
        return;
    }

    //Load the stats from the database
    if (load_stat(md, index, offset) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while loading stats",
             md->rank);
        ret = MDHIM_ERROR;
        return;
    }

    mlog(MDHIM_SERVER_INFO, "MDHIM Rank %d - "
         "Opened database file %s",
         md->rank, filename);

    ret = MDHIM_SUCCESS;
    return;
}

/**
 * open_db_stores
 * Opens the databases for the given index
 *
 * @param md     Pointer to the main MDHIM structure
 * @param index Pointer to the index
 * @param rs_idx   the range server index
 * @return the initialized data store or nullptr on error
 */
static int open_db_stores(mdhim_t *md, index_t *index) {
    if (!md || !md->p || !md->p->db_opts || !index) {
        return MDHIM_ERROR;
    }

    //Only open database files if this rank has a range server
    if (is_range_server(md, md->rank, index)) {
        if (!(index->mdhim_stores = new mdhim_store_t *[index->dbs_per_server]())) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
                 "Error allocating space for database records",
                 md->rank);
            return MDHIM_ERROR;
        }

        //Select a path
        char *path = md->p->db_opts->path;
        if (md->p->db_opts->paths) {
            int path_num = index->myinfo.rangesrv_num/((double) index->num_rangesrvs/(double) md->p->db_opts->num_paths);
            path_num = path_num >= md->p->db_opts->num_paths ? md->p->db_opts->num_paths - 1 : path_num;
            if (path_num < 0) {
                path = md->p->db_opts->path;
            } else {
                path = md->p->db_opts->paths[path_num];
            }
        }

        const int rs_index = index->dbs_per_server * md->rank / index->range_server_factor;

        // open the files in parallel
        std::vector<std::thread> threads(index->dbs_per_server);
        std::vector<int> retvals(index->dbs_per_server);
        for(int offset = 0; offset < index->dbs_per_server; offset++) {
            threads[offset] = std::thread(open_db_store, md, index, path, rs_index, offset, std::ref(retvals[offset]));
        }

        for(std::thread &t : threads) {
            t.join();
        }

        for(int ret : retvals) {
            if (ret != MDHIM_SUCCESS) {
                return MDHIM_ERROR;
            }
        }
    }

    return MDHIM_SUCCESS;
}

/**
 * create_local_index
 * Creates an index on the primary index that is handled by the same servers as the primary index.
 * This index does not have global ordering.  Ordering is local to the range server only.
 * Retrieving a key from this index will require querying multiple range servers simultaneously.
 *
 * @param  md  main MDHIM struct
 * @return     MDHIM_ERROR on error, otherwise the index identifier
 */
index_t *create_local_index(mdhim_t *md, int db_type, int key_type, const char *index_name) {
    index_t *check = nullptr;
    int32_t rangesrv_num;

    MPI_Barrier(md->comm);

    //Check that the key type makes sense
    if (key_type < MDHIM_INT_KEY || key_type > MDHIM_LEX_BYTE_KEY) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Invalid key type specified");
        return nullptr;
    }

    //Acquire the lock to update indexes
    while (pthread_rwlock_wrlock(&md->p->indexes_lock) == EBUSY) {
        usleep(10);
    }

    //Create a new global_index to hold our index entry
    index_t *li = (index_t*)calloc(1, sizeof(index_t));
    if (!li) {
        goto done;
    }

    //Initialize the new index struct
    li->id = HASH_COUNT(md->p->indexes);
    li->range_server_factor = md->p->primary_index->range_server_factor;
    li->slice_size = MDHIM_MAX_SLICES;
    li->type = LOCAL_INDEX;
    li->key_type = key_type;
    li->db_type = db_type;
    li->myinfo.rangesrv_num = 0;
    li->myinfo.rank = md->rank;
    li->primary_id = md->p->primary_index->id;
    li->stats = nullptr;

    //Figure out how many range servers we could have based on the range server factor
    li->num_rangesrvs = get_num_range_servers(md->size, li->range_server_factor);

    //Get the number of databases per server there are
    li->dbs_per_server = md->p->db_opts->dbs_per_server;

    //Get the range servers for this index
    if (get_rangesrvs(md, li) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Couldn't get the range server list",
             md->rank);
    }

    //Add it to the hash table
    HASH_ADD_INT(md->p->indexes, id, li);

    //Test if I'm a range server and get the range server number
    if ((rangesrv_num = is_range_server(md, md->rank, li)) == MDHIM_ERROR) {
        goto done;
    }

    if (rangesrv_num > 0) {
        //Populate my range server info for this index
        li->myinfo.rank = md->rank;
        li->myinfo.rangesrv_num = rangesrv_num;
    }

    //If not a range server, our work here is done
    if (!rangesrv_num) {
        goto done;
    }

    //Read in the manifest file if the rangesrv_num is 1 for the primary index
    if (rangesrv_num == 1 &&
        read_manifest(md, li) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error: There was a problem reading or validating the manifest file",
             md->rank);
        MPI_Abort(md->comm, 0);
    }

    // allocate space for the histogram
    li->histogram = new std::size_t[md->p->db_opts->histogram.count + 1]();

    //Open the data store
    if (open_db_stores(md, (index_t *) li) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error opening data store for index: %d",
             md->rank, li->id);
        MPI_Abort(md->comm, 0);
    }

    //Initialize the range server threads if they haven't been already
    if (!md->p->mdhim_rs) {
        range_server_init(md);
    }

done:
    //Release the indexes lock
    if (pthread_rwlock_unlock(&md->p->indexes_lock) != 0) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error unlocking the indexes_lock",
             md->rank);
        return nullptr;
    }

    if (!li) {
        return nullptr;
    }

    // The index name has already been taken
    if(check) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error creating index: Name %s, already exists", md->rank, index_name);
        return nullptr;
    }

    return li;
}

/**
 * create_global_index
 * Collective call that creates a global index.
 * A global index has global ordering.  This means that range servers serve mutually exclusive keys
 * and keys can be retrieved across servers in order.  Retrieving a key will query only one range
 * server.
 *
 * @param md                 main MDHIM struct
 * @param server_factor      used in calculating the number of range servers
 * @param max_recs_per_slice the number of records per slice
 * @return                   MDHIM_ERROR on error, otherwise the index identifier
 */
index_t *create_global_index(mdhim_t *md, int server_factor,
                             uint64_t max_recs_per_slice,
                             int db_type, int key_type, char *index_name) {
    index_t *check = nullptr;
    int32_t rangesrv_num;

    MPI_Barrier(md->comm);

    //Check that the key type makes sense
    if (key_type < MDHIM_INT_KEY || key_type > MDHIM_LEX_BYTE_KEY) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Invalid key type specified");
        return nullptr;
    }

    //Acquire the lock to update indexes
    while (pthread_rwlock_wrlock(&md->p->indexes_lock) == EBUSY) {
        usleep(10);
    }

    //Create a new global_index to hold our index entry
    index_t *gi = (index_t*)calloc(1, sizeof(index_t));
    if (!gi) {
        goto done;
    }

    //Initialize the new index struct
    gi->id = HASH_COUNT(md->p->indexes);
    gi->range_server_factor = server_factor;
    gi->slice_size = max_recs_per_slice;
    gi->type = gi->id > 0 ? SECONDARY_INDEX : PRIMARY_INDEX;
    gi->key_type = key_type;
    gi->db_type = db_type;
    gi->myinfo.rangesrv_num = 0;
    gi->myinfo.rank = md->rank;
    gi->primary_id = gi->type == SECONDARY_INDEX ? md->p->primary_index->id : -1;
    gi->stats = nullptr;

    //Figure out how many range servers we could have based on the range server factor
    gi->num_rangesrvs = get_num_range_servers(md->size, gi->range_server_factor);

    //Get the number of databases per server there are
    gi->dbs_per_server = md->p->db_opts->dbs_per_server;

    //Get the range servers for this index
    if (get_rangesrvs(md, gi) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Couldn't get the range server list",
             md->rank);
    }

    //Add it to the hash table
    HASH_ADD_INT(md->p->indexes, id, gi);

    //Test if I'm a range server and get the range server number
    if ((rangesrv_num = is_range_server(md, md->rank, gi)) == MDHIM_ERROR) {
        goto done;
    }

    if (rangesrv_num > 0) {
        //Populate my range server info for this index
        gi->myinfo.rank = md->rank;
        gi->myinfo.rangesrv_num = rangesrv_num;
    }

    //Initialize the communicator for this index
    if (index_init_comm(md, gi) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error creating the index communicator",
             md->rank);
        goto done;
    }

    //If not a range server, our work here is done
    if (!rangesrv_num) {
        goto done;
    }

    //Read in the manifest file if the rangesrv_num is 1 for the primary index
    if (rangesrv_num == 1 &&
        read_manifest(md, gi) != MDHIM_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error: There was a problem reading or validating the manifest file",
             md->rank);
        MPI_Abort(md->comm, 0);
    }

    // allocate space for the histogram
    gi->histogram = new std::size_t[md->p->db_opts->histogram.count + 1]();

    //Open the data store
    if (open_db_stores(md, (index_t *) gi) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error opening data store for index: %d",
             md->rank, gi->id);
    }

    //Initialize the range server threads if they haven't been already
    if (!md->p->mdhim_rs) {
        range_server_init(md);
    }

done:
    //Release the indexes lock
    if (pthread_rwlock_unlock(&md->p->indexes_lock) != 0) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error unlocking the indexes_lock",
             md->rank);
        return nullptr;
    }

    if (!gi) {
        return nullptr;
    }

    // The index name has already been taken
    if(check) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error creating index: Name %s, already exists", md->rank, index_name);
        return nullptr;
    }

    return gi;
}

/**
 * get_rangesrvs
 * Creates a rangesrv_info hash table
 *
 * @param md      in   main MDHIM struct
 * @return a list of range servers
 */
int get_rangesrvs(mdhim_t *md, index_t *index) {
    //Iterate through the ranks to determine which ones are range servers
    for (int i = 0; i < md->size; i++) {
        const int32_t rangesrv_num = is_range_server(md, i, index);

        //Test if the rank is range server for this index
        if (rangesrv_num == MDHIM_ERROR) {
            continue;
        }

        if (!rangesrv_num) {
            continue;
        }

        //Set the master range server to be the server with the largest rank
        if (i > index->rangesrv_master) {
            index->rangesrv_master = i;
        }

        for(int j = 0; j < index->dbs_per_server; j++) {
            //All three have the same contents
            //They have to be different because using the same pointer will confuse uthash
            rangesrv_info_t rs_entry;
            rs_entry.database = i * index->dbs_per_server + j;
            rs_entry.rank = i;
            rs_entry.rangesrv_num = rangesrv_num;

            rangesrv_info_t *rs_entry_database = new rangesrv_info_t(rs_entry);
            rangesrv_info_t *rs_entry_num = new rangesrv_info_t(rs_entry);
            rangesrv_info_t *rs_entry_rank = new rangesrv_info_t(rs_entry);

            //Add it to the hash tables
            HASH_ADD_INT(index->rangesrvs_by_database, database, rs_entry_database);
            HASH_ADD_INT(index->rangesrvs_by_num, rangesrv_num, rs_entry_num);
            HASH_ADD_INT(index->rangesrvs_by_rank, rank, rs_entry_rank);
         }
    }

    return MDHIM_SUCCESS;
}

/**
 * is_range_server
 * Tests to see if the given rank is a range server for one or more indexes
 *
 * @param md      main MDHIM struct
 * @param rank    rank to find out if it is a range server
 * @return        MDHIM_ERROR on error, 0 on false, 1 or greater to represent the range server number otherwise
 */
int32_t is_range_server(mdhim_t *md, int rank, index_t *index) {
    //If a local index, check to see if the rank is a range server for the primary index
    if (index->type == LOCAL_INDEX) {
        return is_range_server(md, rank, md->p->primary_index);
    }

    /* Get the range server number, which is just a number from 1 onward
       It represents the ranges the server serves and is calculated with the RANGE_SERVER_FACTOR

       The RANGE_SERVER_FACTOR is a number that is divided by the rank such that if the
       remainder is zero, then the rank is a rank server

       For example, if there were 8 ranks and the RANGE_SERVER_FACTOR is 2,
       then ranks: 2, 4, 6, 8 are range servers

       If the size of communicator is less than the RANGE_SERVER_FACTOR,
       the last rank is the range server
    */

    const int size = md->size - 1;
    int32_t rangesrv_num = 0;
    if (size < index->range_server_factor && rank == size) {
        //The size of the communicator is less than the RANGE_SERVER_FACTOR
        rangesrv_num = 1;
    } else if (size >= index->range_server_factor && rank % index->range_server_factor == 0) {
        //This is a range server, get the range server's number
        rangesrv_num = rank / index->range_server_factor;
        rangesrv_num++;
    }

    if (rangesrv_num > index->num_rangesrvs) {
        rangesrv_num = 0;
    }

    return rangesrv_num;
}

/**
 * range_server_init_comm
 * Initializes the range server communicator that is used for range server to range
 * server collectives
 * The stat flush function will use this communicator
 *
 * @param md  Pointer to the main MDHIM structure
 * @return    MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int index_init_comm(mdhim_t *md, index_t *bi) {
    rangesrv_info_t *rangesrv, *tmp;
    int *ranks = nullptr;
    int size = 0;
    int prev_rank = -1;
    //Populate the ranks array that will be in our new comm
    if (im_range_server(bi) == 1) {
        ranks = new int[bi->num_rangesrvs]();
        //Iterate through the stat hash entries
        HASH_ITER(hh, bi->rangesrvs_by_rank, rangesrv, tmp) {
            if (!rangesrv) {
                continue;
            }

            if (rangesrv->rank != prev_rank) {
                prev_rank = ranks[size++] = rangesrv->rank;
            }
        }
    } else {
        size = md->size;
        ranks = new int[size]();
        for (int i = 0; i < size; i++) {
            HASH_FIND_INT(bi->rangesrvs_by_rank, &i, rangesrv);
            if (rangesrv) {
                continue;
            }

            if (rangesrv->rank != prev_rank) {
                prev_rank = ranks[size++] = rangesrv->rank;
            }
        }
    }

    //Create a new group with the range servers only
    MPI_Group orig;
    if (MPI_Comm_group(md->comm, &orig) != MPI_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while creating a new group in range_server_init_comm",
             md->rank);
        return MDHIM_ERROR;
    }

    MPI_Group new_group;
    if (MPI_Group_incl(orig, size, ranks, &new_group) != MPI_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while creating adding ranks to the new group in range_server_init_comm",
             md->rank);
        MPI_Group_free(&orig);
        return MDHIM_ERROR;
    }

    MPI_Comm new_comm;
    if (MPI_Comm_create(md->comm, new_group, &new_comm) != MPI_SUCCESS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
             "Error while creating the new communicator in range_server_init_comm",
             md->rank);
        MPI_Group_free(&orig);
        MPI_Group_free(&new_group);
        return MDHIM_ERROR;
    }

    if (im_range_server(bi) == 1) {
        bi->rs_comm = new_comm;
    }

    MPI_Group_free(&orig);
    MPI_Group_free(&new_group);
    delete [] ranks;
    return MDHIM_SUCCESS;
}

index_t *get_index(mdhim_t *md, int index_id) {
    index_t *index = nullptr;

    //Acquire the lock to update indexes
    while (pthread_rwlock_wrlock(&md->p->indexes_lock) == EBUSY) {
        usleep(10);
    }

    if (index_id >= 0) {
        HASH_FIND(hh, md->p->indexes, &index_id, sizeof(int), index);
    }

    if (pthread_rwlock_unlock(&md->p->indexes_lock) != 0) {
        mlog(MDHIM_CLIENT_CRIT, "Rank %d - Error unlocking the indexes_lock",
             md->rank);
        return nullptr;
    }

    return index;
}

void indexes_release(mdhim_t *md) {
    index_t *cur_indx, *tmp_indx;

    HASH_ITER(hh, md->p->indexes, cur_indx, tmp_indx) {
        HASH_DELETE(hh, md->p->indexes, cur_indx);

        rangesrv_info_t *cur_rs, *tmp_rs;
        HASH_ITER(hh, cur_indx->rangesrvs_by_database, cur_rs, tmp_rs) {
            HASH_DEL(cur_indx->rangesrvs_by_database, cur_rs);
            delete cur_rs;
        }

        HASH_ITER(hh, cur_indx->rangesrvs_by_num, cur_rs, tmp_rs) {
            HASH_DEL(cur_indx->rangesrvs_by_num, cur_rs);
            delete cur_rs;
        }

        HASH_ITER(hh, cur_indx->rangesrvs_by_rank, cur_rs, tmp_rs) {
            HASH_DEL(cur_indx->rangesrvs_by_rank, cur_rs);
            delete cur_rs;
        }

        //Clean up the storage if I'm a range server for this index
        if (cur_indx->myinfo.rangesrv_num > 0) {
            if (cur_indx->myinfo.rangesrv_num == 1) {
                //Write the manifest
                write_manifest(md, cur_indx);
            }

            //Write the stats to the database
            for(int i = 0; i < cur_indx->dbs_per_server; i++) {
                if (write_stat(md, cur_indx, i) != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - "
                         "Error while loading stats",
                         md->rank);
                }

                // free stats from memory
                mdhim_stat_t *stat, *tmp;
                HASH_ITER(hh, cur_indx->mdhim_stores[i]->mdhim_store_stats, stat, tmp) {
                    if (!stat) {
                        continue;
                    }

                    HASH_DEL(cur_indx->mdhim_stores[i]->mdhim_store_stats, stat);
                    free(stat->max);
                    free(stat->min);
                    free(stat);
                }

                // force the database to flush
                cur_indx->mdhim_stores[i]->commit(cur_indx->mdhim_stores[i]->db_stats);

                //Close the database
                if (cur_indx->mdhim_stores[i]->close(cur_indx->mdhim_stores[i]->db_handle,
                                                     cur_indx->mdhim_stores[i]->db_stats)
                    != MDHIM_SUCCESS) {
                    mlog(MDHIM_SERVER_CRIT, "Rank %d - Error closing database",
                         md->rank);
                }

                pthread_rwlock_destroy(&cur_indx->mdhim_stores[i]->mdhim_store_stats_lock);
                free(cur_indx->mdhim_stores[i]);
            }
            delete [] cur_indx->histogram;
            delete [] cur_indx->mdhim_stores;
            if (cur_indx->type != LOCAL_INDEX) {
                MPI_Comm_free(&cur_indx->rs_comm);
            }
        }

        //Iterate through the stat hash entries to free them
        mdhim_stat_t *stat, *tmp;
        HASH_ITER(hh, cur_indx->stats, stat, tmp) {
            if (!stat) {
                continue;
            }

            HASH_DEL(cur_indx->stats, stat);
            free(stat->max);
            free(stat->min);
            free(stat);
        }

        free(cur_indx);
    }
}

static int pack_stats(index_t *index, int rs_idx, void *buf, int size,
                      int float_type, int stat_size, MPI_Comm comm) {

    mdhim_stat_t *stat, *tmp;
    void *tstat;
    int ret = MPI_SUCCESS;
    int sendidx = 0;

    //Pack the stat data I have by iterating through the stats hash
    HASH_ITER(hh, index->mdhim_stores[rs_idx]->mdhim_store_stats, stat, tmp) {
        //Get the appropriate struct to send
        if (float_type) {
            mdhim_db_fstat_t *fstat = (mdhim_db_fstat_t*)malloc(sizeof(mdhim_db_fstat_t));
            fstat->slice = stat->key;
            fstat->num = stat->num;
            fstat->dmin = *(long double *) stat->min;
            fstat->dmax = *(long double *) stat->max;
            tstat = fstat;
        } else {
            mdhim_db_istat_t *istat = (mdhim_db_istat_t*)malloc(sizeof(mdhim_db_istat_t));
            istat->slice = stat->key;
            istat->num = stat->num;
            istat->imin = *(uint64_t *) stat->min;
            istat->imax = *(uint64_t *) stat->max;
            tstat = istat;
        }

        //Pack the struct
        if ((ret = MPI_Pack(tstat, stat_size, MPI_CHAR, buf, size, &sendidx,
                            comm)) != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Error packing buffer when sending stat info"
                 " to master range server");
            free(buf);
            free(tstat);
            return ret;
        }

        free(tstat);
    }

    return ret;
}

static int get_stat_flush_global(mdhim_t *md, index_t *index) {
    int recvidx = 0;
    char *recvbuf = nullptr;
    int *recvcounts;
    int *displs;
    int recvsize;
    mdhim_stat_t *stat, *tmp;
    void *tstat;
    int num_items = 0;

    //Determine the size of the buffers to send based on the number and type of stats
    const int float_type = is_float_key(index->key_type);
    const int stat_size = (float_type == 1)?sizeof(mdhim_db_fstat_t):sizeof(mdhim_db_istat_t);

    for(int i = 0; i < index->dbs_per_server; i++) {
        if (index->myinfo.rangesrv_num > 0) {
            //Get the number stats in our hash table
            if (index->mdhim_stores[i]->mdhim_store_stats) {
                num_items = HASH_COUNT(index->mdhim_stores[i]->mdhim_store_stats);
            }

            const int sendsize = num_items * stat_size;

            //The master rank is the last rank in range server comm
            const int master = md->size - 1;

            //First we send the number of items that we are going to send
            //Allocate the receive buffer size
            recvsize = index->num_rangesrvs * sizeof(int);
            recvbuf = (char*)malloc(recvsize);
            memset(recvbuf, 0, recvsize);
            MPI_Barrier(index->rs_comm);
            //The master server will receive the number of stats each server has
            if (MPI_Gather(&num_items, 1, MPI_UNSIGNED, recvbuf, 1,
                           MPI_INT, master, index->rs_comm) != MPI_SUCCESS) {
                mlog(MDHIM_SERVER_CRIT, "Rank %d - "
                     "Error while receiving the number of statistics from each range server",
                     md->rank);
                goto error;
            }

            num_items = 0;
            displs = (int*)malloc(sizeof(int) * index->num_rangesrvs);
            recvcounts = (int*)malloc(sizeof(int) * index->num_rangesrvs);
            for (std::size_t i = 0; i < index->num_rangesrvs; i++) {
                displs[i] = num_items * stat_size;
                num_items += ((int *)recvbuf)[i];
                recvcounts[i] = ((int *)recvbuf)[i] * stat_size;
            }

            free(recvbuf);
            recvbuf = nullptr;

            //Allocate send buffer
            char *sendbuf = (char*)malloc(sendsize);

            //Pack the stat data I have by iterating through the stats hash table
            if (pack_stats(index, i, sendbuf, sendsize,
                           float_type, stat_size, index->rs_comm) != MPI_SUCCESS) {
                goto error;
            }

            //Allocate the recv buffer for the master range server
            if (md->rank == index->rangesrv_master) {
                recvsize = num_items * stat_size;
                recvbuf = (char*)malloc(recvsize);
                memset(recvbuf, 0, recvsize);
            } else {
                recvbuf = nullptr;
                recvsize = 0;
            }

            MPI_Barrier(index->rs_comm);
            //The master server will receive the stat info from each rank in the range server comm
            if (MPI_Gatherv(sendbuf, sendsize, MPI_PACKED, recvbuf, recvcounts, displs,
                            MPI_PACKED, master, index->rs_comm) != MPI_SUCCESS) {
                mlog(MDHIM_SERVER_CRIT, "Rank %d - "
                     "Error while receiving range server info",
                     md->rank);
                goto error;
            }

            free(recvcounts);
            free(displs);
            free(sendbuf);
        }

        MPI_Barrier(md->comm);
        //The master range server broadcasts the number of stats it is going to send
        if (MPI_Bcast(&num_items, 1, MPI_UNSIGNED, index->rangesrv_master,
                      md->comm) != MPI_SUCCESS) {
            mlog(MDHIM_CLIENT_CRIT, "Rank %d - "
                 "Error while receiving the number of stats to receive",
                 md->rank);
            goto error;
        }

        MPI_Barrier(md->comm);

        recvsize = num_items * stat_size;
        //Allocate the receive buffer size for clients
        if (md->rank != index->rangesrv_master) {
            recvbuf = (char*)malloc(recvsize);
            memset(recvbuf, 0, recvsize);
        }

        //The master range server broadcasts the receive buffer to the comm
        if (MPI_Bcast(recvbuf, recvsize, MPI_PACKED, index->rangesrv_master,
                      md->comm) != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error while receiving range server info",
                 md->rank);
            goto error;
        }

        //Unpack the receive buffer and populate our index->stats hash table
        recvidx = 0;
        for (int j = 0; j < recvsize; j+=stat_size) {
            tstat = malloc(stat_size);
            memset(tstat, 0, stat_size);
            if (MPI_Unpack(recvbuf, recvsize, &recvidx, tstat, stat_size,
                           MPI_CHAR, md->comm) != MPI_SUCCESS) {
                mlog(MPI_CRIT, "Rank %d - "
                     "Error while unpacking stat data",
                     md->rank);
                free(tstat);
                goto error;
            }

            stat = (mdhim_stat*)malloc(sizeof(mdhim_stat_t));
            stat->dirty = 0;
            if (float_type) {
                stat->min = (void *) malloc(sizeof(long double));
                stat->max = (void *) malloc(sizeof(long double));
                *(long double *)stat->min = ((mdhim_db_fstat_t *)tstat)->dmin;
                *(long double *)stat->max = ((mdhim_db_fstat_t *)tstat)->dmax;
                stat->key = ((mdhim_db_fstat_t *)tstat)->slice;
                stat->num = ((mdhim_db_fstat_t *)tstat)->num;
            } else {
                stat->min = (void *) malloc(sizeof(uint64_t));
                stat->max = (void *) malloc(sizeof(uint64_t));
                *(uint64_t *)stat->min = ((mdhim_db_istat_t *)tstat)->imin;
                *(uint64_t *)stat->max = ((mdhim_db_istat_t *)tstat)->imax;
                stat->key = ((mdhim_db_istat_t *)tstat)->slice;
                stat->num = ((mdhim_db_istat_t *)tstat)->num;
            }

            HASH_FIND_INT(index->stats, &stat->key, tmp);
            if (!tmp) {
                HASH_ADD_INT(index->stats, key, stat);
            } else {
                //Replace the existing stat
                HASH_REPLACE_INT(index->stats, key, stat, tmp);
                free(tmp->min);
                free(tmp->max);
                free(tmp);
            }
            free(tstat);
        }

        free(recvbuf);
    }

    return MDHIM_SUCCESS;

error:
    if (recvbuf) {
        free(recvbuf);
    }

    return MDHIM_ERROR;
}

static int get_stat_flush_local(mdhim_t *md, index_t *index) {
    char *sendbuf;
    int sendsize = 0;
    int recvidx = 0;
    char *recvbuf = nullptr;
    int *num_items_to_recv;
    int *recvcounts;
    int *displs;
    int recvsize;
    int ret = 0;
    mdhim_stat_t *stat, *tmp, *rank_stat;
    void *tstat;
    int num_items = 0;

    //Determine the size of the buffers to send based on the number and type of stats
    const int float_type = is_float_key(index->key_type);
    const int stat_size = (float_type == 1)?sizeof(mdhim_db_fstat_t):sizeof(mdhim_db_istat_t);

    for(int i = 0; i < index->dbs_per_server; i++) {

        if (index->myinfo.rangesrv_num > 0) {
            //Get the number stats in our hash table
            if (index->mdhim_stores[i]->mdhim_store_stats) {
                num_items = HASH_COUNT(index->mdhim_stores[i]->mdhim_store_stats);
            }

            sendsize = num_items * stat_size;
        }

        //First we send the number of items that we are going to send
        //Allocate the receive buffer size
        recvsize = md->size * sizeof(int);
        recvbuf = (char*)malloc(recvsize);
        memset(recvbuf, 0, recvsize);
        MPI_Barrier(md->comm);
        //All gather the number of items to send
        if ((ret = MPI_Allgather(&num_items, 1, MPI_UNSIGNED, recvbuf, 1,
                                 MPI_INT, md->comm)) != MPI_SUCCESS) {
            mlog(MDHIM_SERVER_CRIT, "Rank %d - "
                 "Error while receiving the number of statistics from each range server",
                 md->rank);
            free(recvbuf);
            goto error;
        }

        num_items = 0;
        displs = (int*)malloc(sizeof(int) * md->size);
        recvcounts = (int*)malloc(sizeof(int) * md->size);
        for (int i = 0; i < md->size; i++) {
            displs[i] = num_items * stat_size;
            num_items += ((int *)recvbuf)[i];
            recvcounts[i] = ((int *)recvbuf)[i] * stat_size;
        }

        num_items_to_recv = (int *)recvbuf;
        recvbuf = nullptr;

        if (sendsize) {
            //Allocate send buffer
            sendbuf = (char*)malloc(sendsize);

            //Pack the stat data I have by iterating through the stats hash table
            ret =  pack_stats(index, i, sendbuf, sendsize,
                              float_type, stat_size, md->comm);
            if (ret != MPI_SUCCESS) {
                goto error;
            }
        } else {
            sendbuf = nullptr;
        }

        recvsize = num_items * stat_size;
        recvbuf = (char*)malloc(recvsize);
        memset(recvbuf, 0, recvsize);

        MPI_Barrier(md->comm);
        //The master server will receive the stat info from each rank in the range server comm
        if ((ret = MPI_Allgatherv(sendbuf, sendsize, MPI_PACKED, recvbuf, recvcounts, displs,
                                  MPI_PACKED, md->comm)) != MPI_SUCCESS) {
            mlog(MDHIM_SERVER_CRIT, "Rank %d - "
                 "Error while receiving range server info",
                 md->rank);
            goto error;
        }

        free(recvcounts);
        free(displs);
        free(sendbuf);

        MPI_Barrier(md->comm);

        //Unpack the receive buffer and populate our index->stats hash table
        recvidx = 0;
        for (int j = 0; j < md->size; j++) {
            if ((ret = is_range_server(md, j, index)) < 1) {
                continue;
            }

            HASH_FIND_INT(index->stats, &j, tmp);
            if (!tmp) {
                mlog(MPI_CRIT, "Rank %d - "
                     "Adding rank: %d to local index stat data",
                     md->rank, j);
                rank_stat = (mdhim_stat_t*)malloc(sizeof(mdhim_stat_t));
                memset(rank_stat, 0, sizeof(mdhim_stat_t));
                rank_stat->key = j;
                rank_stat->stats = nullptr;
                HASH_ADD_INT(index->stats, key, rank_stat);
            } else {
                rank_stat = tmp;
            }

            for (int k = 0; k < num_items_to_recv[i]; k++) {
                tstat = (char*)malloc(stat_size);
                memset(tstat, 0, stat_size);
                if ((ret = MPI_Unpack(recvbuf, recvsize, &recvidx, tstat, stat_size,
                                      MPI_CHAR, md->comm)) != MPI_SUCCESS) {
                    mlog(MPI_CRIT, "Rank %d - "
                         "Error while unpacking stat data",
                         md->rank);
                    free(tstat);
                    goto error;
                }

                stat = (mdhim_stat_t*)malloc(sizeof(mdhim_stat_t));
                stat->dirty = 0;
                if (float_type) {
                    stat->min = (void *) malloc(sizeof(long double));
                    stat->max = (void *) malloc(sizeof(long double));
                    *(long double *)stat->min = ((mdhim_db_fstat_t *)tstat)->dmin;
                    *(long double *)stat->max = ((mdhim_db_fstat_t *)tstat)->dmax;
                    stat->key = ((mdhim_db_fstat_t *)tstat)->slice;
                    stat->num = ((mdhim_db_fstat_t *)tstat)->num;
                } else {
                    stat->min = (void *) malloc(sizeof(uint64_t));
                    stat->max = (void *) malloc(sizeof(uint64_t));
                    *(uint64_t *)stat->min = ((mdhim_db_istat_t *)tstat)->imin;
                    *(uint64_t *)stat->max = ((mdhim_db_istat_t *)tstat)->imax;
                    stat->key = ((mdhim_db_istat_t *)tstat)->slice;
                    stat->num = ((mdhim_db_istat_t *)tstat)->num;
                }

                mlog(MPI_CRIT, "Rank %d - "
                     "Adding rank: %d with stat min: %lu, stat max: %lu, stat key: %u num: %lu"
                     "to local index stat data",
                     md->rank, i, *(uint64_t *)stat->min, *(uint64_t *)stat->max,
                     stat->key, stat->num);
                HASH_FIND_INT(rank_stat->stats, &stat->key, tmp);
                if (!tmp) {
                    HASH_ADD_INT(rank_stat->stats, key, stat);
                } else {
                    //Replace the existing stat
                    HASH_REPLACE_INT(rank_stat->stats, key, stat, tmp);
                    free(tmp);
                }

                free(tstat);
            }
        }

        free(recvbuf);
        free(num_items_to_recv);
    }

    return MDHIM_SUCCESS;

error:
    if (recvbuf) {
        free(recvbuf);
    }

    return MDHIM_ERROR;
}

/**
 * get_stat_flush
 * Receives stat data from all the range servers and populates md->p->stats
 *
 * @param md      in   main MDHIM struct
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int get_stat_flush(mdhim_t *md, index_t *index) {
    int ret;

    pthread_mutex_lock(&md->lock);

    if (index->type != LOCAL_INDEX) {
        ret = get_stat_flush_global(md, index);
    } else {
        ret = get_stat_flush_local(md, index);
    }

    pthread_mutex_unlock(&md->lock);

    return ret;
}

/**
 * get_num_range_servers
 * Gets the number of range servers with a given number of ranks and range server factor
 *
 * @param size                the total number of ranks there are
 * @param range_server_factor how often a rank is a range server
 * @return the total number of range servers there are
 */
uint32_t get_num_range_servers(const int size, const int range_server_factor) {
    if (!range_server_factor) {
        return 0;
    }
    return (size / range_server_factor) + (bool) (size % range_server_factor);
}

/**
 * get_num_databases
 * Generic method for getting the number of databases
 *
 * @param ranks               the total number of ranks there are
 * @param range_server_factor how often a rank is a range server
 * @param dbs_per_server      the number of databases opened by a range server
 * @return the number of databases
 */
uint32_t get_num_databases(const int size, const int range_server_factor, const int dbs_per_server) {
    return dbs_per_server * get_num_range_servers(size, range_server_factor);
}
