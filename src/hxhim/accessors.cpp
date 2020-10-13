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
 * hxhim::nocheck::GetRangeServerCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetRangeServerCount(hxhim_t *hx, std::size_t *count) {
    if (count) {
        *count = hx->p->total_range_servers;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim::GetRangeServerCount
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
