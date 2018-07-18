#include "hxhim/options.h"
#include "hxhim/options_private.hpp"

/**
 * hxhim_options_init
 * Allocates the private pointer of an existing hxhim_options_t structure
 *
 * @param opts address of an existing hxhim_options_t structure
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_init(hxhim_options_t *opts) {
    if (!opts) {
        return HXHIM_ERROR;
    }

    if (!(opts->p = new hxhim_options_private_t())) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * valid_opts
 *
 * @param opts
 * @return whether or not opts and opts->p are both non NULL
 */
static bool valid_opts(hxhim_options_t *opts) {
    return (opts && opts->p);
}

/**
 * hxhim_options_set_mpi_bootstrap
 *
 * @param opts the set of options to be modified
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_mpi_bootstrap(hxhim_options_t *opts, MPI_Comm comm) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    if (((opts->p->mpi.comm = comm)                           == MPI_COMM_NULL) ||
        (MPI_Comm_rank(opts->p->mpi.comm, &opts->p->mpi.rank) != MPI_SUCCESS)   ||
        (MPI_Comm_size(opts->p->mpi.comm, &opts->p->mpi.size) != MPI_SUCCESS))   {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_backend
 * Sets the backend of the options
 *
 * @param opts   the set of options to be modified
 * @param config extra configuration data needed to initialize the backedn
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_backend(hxhim_options_t *opts, const hxhim_backend_t backend, hxhim_backend_config_t *config) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    hxhim_options_backend_config_destroy(opts->p->backend_config);

    opts->p->backend = backend;
    opts->p->backend_config = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_subject_type
 *
 * @param opts the set of options to be modified
 * @param type the type the subjects are
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_subject_type(hxhim_options_t *opts, const hxhim_spo_type_t type) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->subject_type = type;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_predicate_type
 *
 * @param opts the set of options to be modified
 * @param type the type the predicate are
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_predicate_type(hxhim_options_t *opts, const hxhim_spo_type_t type) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->predicate_type = type;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_object_type
 *
 * @param opts the set of options to be modified
 * @param type the type the objects are
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_object_type(hxhim_options_t *opts, const hxhim_spo_type_t type) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->object_type = type;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_queued_puts
 * Set the number of bulk PUTs to queue up before flushing in the background thread
 *
 * @param opts  the set of options to be modified
 * @param count the number of PUTs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_queued_bputs(hxhim_options_t *opts, const std::size_t count) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->queued_bputs = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_first_n
 * Set the number of datapoints to use to generate the histogram buckets
 *
 * @param opts  the set of options to be modified
 * @param count the number of datapoints
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_first_n(hxhim_options_t *opts, const std::size_t count) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->histogram_first_n = count;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_histogram_bucket_gen_method
 * Set the name of the bucket generation method. The actual function is not used here
 * because it depends on the object type
 *
 * @param opts   the set of options to be modified
 * @param method the name of the method
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_histogram_bucket_gen_method(hxhim_options_t *opts, const char *method) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    opts->p->histogram_bucket_gen_method = method;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_destroy
 * Destroys the contents of the hxhim_options_t, but not the variable itself
 *
 * @param opts the hxhim_options_t that contains the pointer to deallocate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_destroy(hxhim_options_t *opts) {
    if (!opts) {
        return HXHIM_ERROR;
    }

    hxhim_options_backend_config_destroy(opts->p->backend_config);

    delete opts->p;
    opts->p = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_create_mdhim_config
 *
 * @param path the name prefix for each database
 * @return an pointer to the configuration data, or a nullptr
 */
hxhim_backend_config_t *hxhim_options_create_mdhim_config(char *path) {
    hxhim_mdhim_config_t *config = new hxhim_mdhim_config_t();
    config->path = path;
    return config;
}

/**
 * hxhim_options_create_leveldb_config
 *
 * @param path               the name prefix for each database
 * @param create_if_missing  whether or not leveldb should create new databases if the databases do not already exist
 * @return an pointer to the configuration data, or a nullptr
 */
hxhim_backend_config_t *hxhim_options_create_leveldb_config(char *path, const int create_if_missing) {
    hxhim_leveldb_config_t *config = new hxhim_leveldb_config_t();
    config->path = path;
    config->create_if_missing = create_if_missing;
    return config;
}

/**
 * hxhim_options_create_in_memory_config
 *
 * @return
 * @return an pointer to the configuration data, or a nullptr
 */
hxhim_backend_config_t *hxhim_options_create_in_memory_config() {
    return new hxhim_backend_config_t();
}

// Cleans up config memory, including the config variable itself because the user will never be able to create their own config
void hxhim_options_backend_config_destroy(hxhim_backend_config_t *config) {
    delete config;
}
