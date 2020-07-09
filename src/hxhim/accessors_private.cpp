#include "hxhim/accessors_private.hpp"
#include "hxhim/private.hpp"

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
 * GetDatastoresPerRangeServer
 * Gets the number of datastores inside this process of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastore_count where to copy the count into
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count) {
    if (datastore_count) {
        *datastore_count = hx->p->datastores.size();
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhim::nocheck::GetDatastoreCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetDatastoreCount(hxhim_t *hx, std::size_t *count) {
    if (count) {
        *count = hx->p->total_datastores;
    }

    return HXHIM_SUCCESS;
}

/**
 * GetDatastoreClientToServerRatio
 * Gets the ratio of clients to servers
 *
 * @param hx              the HXHIM instance
 * @param client          the client portion of the ratio
 * @param server          the server portion of the ratio
 * @return HXHIM_SUCCESS
 */
int hxhim::nocheck::GetDatastoreClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server) {
    if (client) {
        *client = hx->p->range_server.client_ratio;
    }

    if (server) {
        *server = hx->p->range_server.server_ratio;
    }

    return HXHIM_SUCCESS;
}
