#include "hxhim/accessors.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"

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
 * GetDatastoresPerRangeServer
 * Gets the number of datastores inside this process of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastore_count where to copy the count into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastoresPerRangeServer(hxhim_t *hx, std::size_t *datastore_count) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    return hxhim::nocheck::GetDatastoresPerRangeServer(hx, datastore_count);
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

    return hxhim::nocheck::GetDatastoreCount(hx, count);
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

    return hxhim::nocheck::GetDatastoreClientToServerRatio(hx, client, server);
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
