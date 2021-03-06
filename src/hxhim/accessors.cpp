#include "hxhim/accessors.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"

/**
 * GetEpoch
 * Gets the epoch from the HXHIM instance
 *
 * @param hx     the HXHIM instance
 * @param epoch  where to copy the epoch into
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetEpoch(hxhim_t *hx, ::Stats::Chronopoint &epoch) {
    epoch = hx->p->epoch;
    return HXHIM_SUCCESS;
}

/**
 * GetEpoch
 * Gets the Epoch information of the HXHIM instance
 *
 * @param hx     the HXHIM instance
 * @param epoch  where to copy the epoch into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetEpoch(hxhim_t *hx, struct timespec *epoch) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    if (epoch) {
        ::Stats::Chronopoint cxx;
        const int rc = hxhim::nocheck::GetEpoch(hx, cxx);

        if (rc == HXHIM_SUCCESS) {
            const std::chrono::nanoseconds timestamp = cxx.time_since_epoch();

            epoch->tv_nsec = timestamp.count() % 1000000000;
            epoch->tv_sec  = timestamp.count() / 1000000000;
        }

        return rc;
    }

    return HXHIM_SUCCESS;
}

/**
 * GetEpoch
 * Gets the Epoch information of the HXHIM instance
 *
 * @param hx     the HXHIM instance
 * @param epoch  where to copy the epoch into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetEpoch(hxhim_t *hx, ::Stats::Chronopoint &epoch) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetEpoch(hx, epoch);
}

/**
 * hxhimGetEpoch
 * Gets the Epoch information of the HXHIM instance
 *
 * @param hx     the HXHIM instance
 * @param epoch  where to copy the epoch into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetEpoch(hxhim_t *hx, struct timespec *epoch) {
    return hxhim::GetEpoch(hx, epoch);
}

/**
 * GetMPI
 * Gets the MPI information of the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param comm where to copy the communicator into
 * @param rank where to copy the rank into
 * @param size where to copy the size into
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size) {
    if (comm) {
        *comm = hx->p->bootstrap.comm;
    }

    if (rank) {
        *rank = hx->p->bootstrap.rank;
    }

    if (size) {
        *size = hx->p->bootstrap.size;
    }

    return HXHIM_SUCCESS;
}

/**
 * GetMPI
 * Gets the MPI information of the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param comm where to copy the communicator into
 * @param rank where to copy the rank into
 * @param size where to copy the size into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetMPI(hx, comm, rank, size);
}

/**
 * hxhimGetMPI
 * Gets the MPI information of the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param comm where to copy the communicator into
 * @param rank where to copy the rank into
 * @param size where to copy the size into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetMPI(hxhim_t *hx, MPI_Comm *comm, int *rank, int *size) {
    return hxhim::GetMPI(hx, comm, rank, size);
}

/**
 * nocheck::GetRangeServerCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetRangeServerCount(hxhim_t *hx, std::size_t *count) {
    if (count) {
        *count = hx->p->range_server.total_range_servers;
    }

    return HXHIM_SUCCESS;
}

/**
 * GetRangeServerCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetRangeServerCount(hxhim_t *hx, std::size_t *count) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetRangeServerCount(hx, count);
}

/**
 * hxhimGetRangeServerCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetRangeServerCount(hxhim_t *hx, size_t *count) {
    return hxhim::GetRangeServerCount(hx, count);
}

/**
 * GetRangeServerClientToServerRatio
 * Gets the ratio of clients to servers
 *
 * @param hx              the HXHIM instance
 * @param client          the client portion of the ratio
 * @param server          the server portion of the ratio
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetRangeServerClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server) {
    if (client) {
        *client = hx->p->range_server.client_ratio;
    }

    if (server) {
        *server = hx->p->range_server.server_ratio;
    }

    return HXHIM_SUCCESS;
}

/**
 * GetRangeServerClientToServerRatio
 * Gets the ratio of clients to servers
 *
 * @param hx              the HXHIM instance
 * @param client          the client portion of the ratio
 * @param server          the server portion of the ratio
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetRangeServerClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetRangeServerClientToServerRatio(hx, client, server);
}

/**
 * hxhimGetRangeServerClientToServerRatio
 * Gets the ratio of clients to servers
 *
 * @param hx              the HXHIM instance
 * @param client          the client portion of the ratio
 * @param server          the server portion of the ratio
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetRangeServerClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server) {
    return hxhim::GetRangeServerClientToServerRatio(hx, client, server);
}

/**
 * GetPrintBufferContents
 * Moves the contents of the private print buffer to the
 * provided ostream and clears out the private buffer
 *
 * @param hx              the HXHIM instance
 * @param stream          the ostream where the private print buffer will be copied to
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetPrintBufferContents(hxhim_t *hx, std::ostream &stream) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    stream << hx->p->print_buffer.rdbuf();
    hx->p->print_buffer.str(std::string());
    hx->p->print_buffer.clear();
    return HXHIM_SUCCESS;
}

/**
 * GetHash
 * Get information about the hash used in the hxhim instance
 *
 * @param hx     the HXHIM instance
 * @param name   the name of the hash function
 * @param func   the hash function
 * @param args   extra arguments to the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::nocheck::GetHash(hxhim_t *hx, const char **name, hxhim_hash_t *func, void **args) {
    if (name) {
        *name = hx->p->hash.name.c_str();
    }

    if (func) {
        *func = hx->p->hash.func;
    }

    if (args) {
        *args = hx->p->hash.args;
    }

    return HXHIM_SUCCESS;
}

/**
 * GetHash
 * Get information about the hash used in the hxhim instance
 *
 * @param hx     the HXHIM instance
 * @param name   the name of the hash function
 * @param func   the hash function
 * @param args   extra arguments to the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetHash(hxhim_t *hx, const char **name, hxhim_hash_t *func, void **args) {
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetHash(hx, name, func, args);
}

/**
 * hxhimGetHash
 * Get information about the hash used in the hxhim instance
 *
 * @param hx     the HXHIM instance
 * @param name   the name of the hash function
 * @param func   the hash function
 * @param args   extra arguments to the hash function
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetHash(hxhim_t *hx, const char **name, hxhim_hash_t *func, void **args) {
    return hxhim::GetHash(hx, name, func, args);
}

/**
 * GetDatastorePrefix
 * Get the prefix for the datastore instance
 * that was found by the configuration parser
 *
 * @param hx          the HXHIM instance
 * @param prefix      a pointer to the prefix
 * @param prefix_len  a pointer to the prefix length
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::nocheck::GetDatastorePrefix(hxhim_t *hx, const char **prefix, std::size_t *prefix_len) {
    if (prefix) {
        *prefix = hx->p->range_server.prefix.data();
    }

    if (prefix_len) {
        *prefix_len = hx->p->range_server.prefix.size();
    }

    return HXHIM_SUCCESS;
}

/**
 * GetDatastorePrefix
 * Get the prefix for the datastore instance
 * that was found by the configuration parser
 *
 * @param hx          the HXHIM instance
 * @param prefix      a pointer to the prefix
 * @param prefix_len  a pointer to the prefix length
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastorePrefix(hxhim_t *hx, const char **prefix, std::size_t *prefix_len) {
    if (!valid(hx) || !hx->p->running) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetDatastorePrefix(hx, prefix, prefix_len);
}

/**
 * hxhimGetDatastorePrefix
 * Get the prefix for the datastore instance
 * that was found by the configuration parser
 *
 * @param hx          the HXHIM instance
 * @param prefix      a pointer to the prefix
 * @param prefix_len  a pointer to the prefix length
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetDatastorePrefix(hxhim_t *hx, const char **prefix, size_t *prefix_len) {
    return hxhim::GetDatastorePrefix(hx, prefix, prefix_len);
}

/**
 * HaveHistogram
 * Whether or not the provided name is a histogram maintained by the range servers
 *
 * @param hx        the HXHIM instance
 * @param name      the name to search for
 * @param name_len  the length of the name
 * @param exists    result variable
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::HaveHistogram(hxhim_t *hx, const char *name, const std::size_t name_len, int *exists) {
    *exists = (hx->p->histograms.names.find(std::string(name, name_len)) != hx->p->histograms.names.end());
    return HXHIM_SUCCESS;
}

/**
 * HaveHistogram
 * Whether or not the provided name is a histogram maintained by the range servers
 *
 * @param hx        the HXHIM instance
 * @param name      the name to search for
 * @param name_len  the length of the name
 * @param exists    result variable
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::HaveHistogram(hxhim_t *hx, const char *name, const std::size_t name_len, int *exists) {
    if (!valid(hx) || !hx->p->running || !exists) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::HaveHistogram(hx, name, name_len, exists);
}

/**
 * hxhimHaveHistogram
 * Whether or not the provided name is a histogram maintained by the range servers
 *
 * @param hx        the HXHIM instance
 * @param name      the name to search for
 * @param name_len  the length of the name
 * @param exists    result variable
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimHaveHistogram(hxhim_t *hx, const char *name, const size_t name_len, int *exists) {
    return hxhim::HaveHistogram(hx, name, name_len, exists);
}
