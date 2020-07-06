#include "hxhim/accessors.h"
#include "hxhim/accessors.hpp"
#include "hxhim/private.hpp"

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
 * GetDatastoresPerRangeServer
 * Gets the number of datastores inside this process of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastore_count where to copy the count into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count) {
    if (!valid(hx) || !datastore_count) {
        return HXHIM_ERROR;
    }

    *datastore_count = hx->p->datastores.size();
    return HXHIM_SUCCESS;
}

/**
 * hxhimGetDatastoresPerRangeServer
 * Gets the number of datastores inside this process of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastore_count where to copy the count into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count) {
    return hxhim::GetDatastoresPerRangeServer(hx, datastore_count);
}


/**
 * hxhim::GetDatastoreCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastoreCount(hxhim_t *hx, std::size_t *count) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    if (count) {
        *count = hx->p->total_datastores;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimGetDatastoreCount
 * Gets the total number of datastores in this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param count           the total number of datastores
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetDatastoreCount(hxhim_t *hx, size_t *count) {
    return hxhim::GetDatastoreCount(hx, count);
}

/**
 * GetDatastoreClientToServerRatio
 * Gets the ratio of clients to servers
 *
 * @param hx              the HXHIM instance
 * @param client          the client portion of the ratio
 * @param server          the server portion of the ratio
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastoreClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    if (client) {
        *client = hx->p->range_server.client_ratio;
    }

    if (server) {
        *server = hx->p->range_server.server_ratio;
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimGetDatastoreClientToServerRatio
 * Gets the ratio of clients to servers
 *
 * @param hx              the HXHIM instance
 * @param client          the client portion of the ratio
 * @param server          the server portion of the ratio
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetDatastoreClientToServerRatio(hxhim_t *hx, std::size_t *client, std::size_t *server) {
    return hxhim::GetDatastoreClientToServerRatio(hx, client, server);
}

/**
 * GetDatastores
 * Gets the datastores from this rank of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastores      pointer to put the datastores array address
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastores(hxhim_t *hx, std::vector<hxhim::datastore::Datastore *> **datastores) {
    if (!valid(hx) || !datastores) {
        return HXHIM_ERROR;
    }

    *datastores = &hx->p->datastores;
    return HXHIM_SUCCESS;
}
