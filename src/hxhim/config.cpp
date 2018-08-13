#include <sstream>
#include <vector>
#include <iostream>

#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/options.h"
#include "hxhim/options_private.hpp"
#include "utils/Configuration.hpp"
#include "utils/Histogram.hpp"

static int fill_options(hxhim_options_t *opts, const Config &config) {
    if (!opts || !opts->p ||
        (opts->p->mpi.comm == MPI_COMM_NULL)) {
        return HXHIM_ERROR;
    }

    // Set the bootstrap values
    if (MPI_Comm_rank(opts->p->mpi.comm, &opts->p->mpi.rank) != MPI_SUCCESS) {
        return HXHIM_ERROR;
    }

    if (MPI_Comm_size(opts->p->mpi.comm, &opts->p->mpi.size) != MPI_SUCCESS) {
        return HXHIM_ERROR;
    }

    std::size_t datastores = 0;
    if ((get_value(config, HXHIM_DATASTORES_PER_RANGE_SERVER, datastores) == CONFIG_FOUND) &&
        (hxhim_options_set_datastores_per_range_server(opts, datastores) != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    // Set the datastore
    hxhim_datastore_t datastore;
    if (get_from_map(config, HXHIM_DATASTORE_TYPE, HXHIM_DATASTORES, datastore) == CONFIG_FOUND) {
        switch (datastore) {
            case HXHIM_DATASTORE_LEVELDB:
                {
                    // get the leveldb datastore name prefix
                    Config_it name = config.find(HXHIM_LEVELDB_NAME);
                    if (name == config.end()) {
                        return HXHIM_ERROR;
                    }

                    bool create_if_missing = true; // default to true
                    if (get_bool(config, HXHIM_LEVELDB_CREATE_IF_MISSING, create_if_missing) == CONFIG_ERROR) {
                        return HXHIM_ERROR;
                    }

                    hxhim_options_set_datastore_leveldb(opts, opts->p->mpi.rank, name->second.c_str(), create_if_missing);
                }
                break;
            case HXHIM_DATASTORE_IN_MEMORY:
                {
                    hxhim_options_set_datastore_in_memory(opts);
                }
                break;
            default:
                return HXHIM_ERROR;
        }
    }

    // Get the hash function
    std::string hash;
    if ((get_value(config, HXHIM_HASH, hash) == CONFIG_FOUND) &&
        (hxhim_options_set_hash_name(opts, hash.c_str()) != HXHIM_SUCCESS)) {
        return HXHIM_ERROR;
    }

    // Get the transport type to figure out which variables to search for
    Transport::Type transport_type;
    if (get_from_map(config, HXHIM_TRANSPORT, HXHIM_TRANSPORTS, transport_type) == CONFIG_FOUND) {
        switch (transport_type) {
            case Transport::TRANSPORT_MPI:
                {
                    std::size_t memory_alloc_size, memory_regions, listeners;
                    if ((get_value(config, HXHIM_MPI_MEMORY_ALLOC_SIZE, memory_alloc_size) != CONFIG_FOUND) ||
                        (get_value(config, HXHIM_MPI_MEMORY_REGIONS, memory_regions)       != CONFIG_FOUND) ||
                        (get_value(config, HXHIM_MPI_LISTENERS, listeners)                 != CONFIG_FOUND)) {
                        return HXHIM_ERROR;
                    }

                    hxhim_options_set_transport_mpi(opts, memory_alloc_size, memory_regions, listeners);
                }
                break;
            case Transport::TRANSPORT_THALLIUM:
                {
                    Config_it thallium_module = config.find(HXHIM_THALLIUM_MODULE);
                    if (thallium_module == config.end()) {
                        return HXHIM_ERROR;
                    }

                    hxhim_options_set_transport_thallium(opts, thallium_module->second.c_str());
                }
                break;
            default:
                return HXHIM_ERROR;
        }
    }

    // Add ranks to the endpoint group
    Config_it endpointgroup = config.find(HXHIM_TRANSPORT_ENDPOINT_GROUP);
    if (endpointgroup != config.end()) {
        hxhim_options_clear_endpoint_group(opts);
        if (endpointgroup->second == "ALL") {
            for(int rank = 0; rank < opts->p->mpi.size; rank++) {
                hxhim_options_add_endpoint_to_group(opts, rank);
            }
        }
        else {
            std::stringstream s(endpointgroup->second);
            int id;
            while (s >> id) {
                if (hxhim_options_add_endpoint_to_group(opts, id) != HXHIM_SUCCESS) {
                    // should probably write to mlog and continue instead of returning
                    return HXHIM_ERROR;
                }
            }
        }
    }

    // Set Queued Bulk Puts
    std::size_t queued_bputs = 0;
    if ((get_value(config, HXHIM_QUEUED_BULK_PUTS, queued_bputs) == CONFIG_FOUND) &&
        hxhim_options_set_queued_bputs(opts, queued_bputs) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set Histogram Use First N Values
    std::size_t use_first_n = 0;
    if ((get_value(config, HXHIM_HISTOGRAM_FIRST_N, use_first_n) == CONFIG_FOUND) &&
        hxhim_options_set_histogram_first_n(opts, use_first_n) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Set Histogram Bucket Generation Method
    std::string method;
    if ((get_value(config, HXHIM_HISTOGRAM_BUCKET_GEN_METHOD, method) == CONFIG_FOUND) &&
        hxhim_options_set_histogram_bucket_gen_method(opts, method.c_str()) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * process_config_and_fill_options
 * This function is just a logical separator between
 * setting up the configuration reader and filling out
 * the hxhim_t.
 *
 * This function should be common for all configuration readers.
 *
 * This function should not be modified unless the
 * ConfigReader interface changes.
 *
 * opts should be cleaned up by the calling function.
 *
 * @param config_reader the configuration reader that has been set up
 * @param opts          the options to fill
 * @return whether or not opts was successfully filled
 */
int process_config_and_fill_options(hxhim_options_t *opts, ConfigSequence &config_sequence) {
    if (!opts || !opts->p) {
        return HXHIM_ERROR;
    }

    // Parse the configuration data
    Config config;
    config_sequence.process(config);

    // fill opts with values from configuration
    return fill_options(opts, config);
}

/**
 * hxhim_default_config_reader
 * This function acts as both the default
 * configuration reader as well as an example
 * of how custom configuration readers should
 * be implmented.
 *
 * @param opts the options to fill
 * @return whether or not configuration was completed
 */
int hxhim_default_config_reader(hxhim_options_t *opts, MPI_Comm comm) {
    if (!opts) {
        return HXHIM_ERROR;
    }

    ConfigSequence config_sequence;

    // add default search locations in order of preference: default filename, file environment variable, environment variable overrides
    ConfigFile file(HXHIM_CONFIG_FILE);
    config_sequence.add(&file);

    ConfigFileEnvironment fileenv(HXHIM_CONFIG_ENV);
    config_sequence.add(&fileenv);

    // get the environment variables from the default configuration
    std::vector<ConfigVarEnvironment *> vars;
    for(std::pair<const std::string, std::string> const &default_config : HXHIM_DEFAULT_CONFIG) {
        ConfigVarEnvironment *var = new ConfigVarEnvironment(default_config.first);
        config_sequence.add(var);
        vars.push_back(var);
    }

    int ret = HXHIM_SUCCESS;

    hxhim_options_init(opts);
    hxhim_options_set_mpi_bootstrap(opts, comm);

    if ((fill_options(opts, HXHIM_DEFAULT_CONFIG)               != HXHIM_SUCCESS) || // fill in the options with default values
        (process_config_and_fill_options(opts, config_sequence) != HXHIM_SUCCESS)) { // read the configuration and overwrite default values
        ret = HXHIM_ERROR;
    }

    for(ConfigVarEnvironment *&var : vars) {
        delete var;
    }

    return ret;
}
