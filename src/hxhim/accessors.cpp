#include "hxhim/accessors.h"
#include "hxhim/accessors.hpp"
#include "hxhim/private.hpp"

/**
 * GetMPIComm
 * Gets the MPI communicator inside the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param comm where to copy the communicator into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetMPIComm(hxhim_t *hx, MPI_Comm *comm) {
    if (!valid(hx) || !comm) {
        return HXHIM_ERROR;
    }

    *comm = hx->p->bootstrap.comm;
    return HXHIM_SUCCESS;
}

/**
 * hxhimGetMPIComm
 * Gets the MPI communicator inside the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param comm where to copy the communicator into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetMPIComm(hxhim_t *hx, MPI_Comm *comm) {
    return hxhim::GetMPIComm(hx, comm);
}

/**
 * GetMPIRank
 * Gets the MPI rank inside the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param rank where to copy the rank into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetMPIRank(hxhim_t *hx, int *rank) {
    if (!valid(hx) || !rank) {
        return HXHIM_ERROR;
    }

    *rank = hx->p->bootstrap.rank;
    return HXHIM_SUCCESS;
}

/**
 * hxhimGetMPIRank
 * Gets the MPI rank inside the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param rank where to copy the rank into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetMPIRank(hxhim_t *hx, int *rank) {
    return hxhim::GetMPIRank(hx, rank);
}

/**
 * GetMPISize
 * Gets the MPI size inside the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param size where to copy the size into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetMPISize(hxhim_t *hx, int *size) {
    if (!valid(hx) || !size) {
        return HXHIM_ERROR;
    }

    *size = hx->p->bootstrap.size;
    return HXHIM_SUCCESS;
}

/**
 * hxhimGetMPISize
 * Gets the MPI size inside the HXHIM instance
 *
 * @param hx   the HXHIM instance
 * @param size where to copy the size into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetMPISize(hxhim_t *hx, int *size) {
    return hxhim::GetMPISize(hx, size);
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

    *datastore_count = hx->p->datastore.count;
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
 * Gets the datastores from this HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastores      pointer to put the datastores array address
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastores(hxhim_t *hx, hxhim::datastore::Datastore ***datastores) {
    if (!valid(hx) || !datastores) {
        return HXHIM_ERROR;
    }

    *datastores = hx->p->datastore.datastores;
    return HXHIM_SUCCESS;
}
