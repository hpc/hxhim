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
    if (!hx || !hx->p || !comm) {
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
    if (!hx || !hx->p || !rank) {
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
    if (!hx || !hx->p || !size) {
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
 * GetDatastoreCount
 * Gets the number of datastores inside this process of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastore_count where to copy the count into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhim::GetDatastoreCount(hxhim_t *hx, std::size_t *datastore_count) {
    if (!hx || !hx->p || !datastore_count) {
        return HXHIM_ERROR;
    }

    *datastore_count = hx->p->datastore.count;
    return HXHIM_SUCCESS;
}

/**
 * hxhimGetDatastoreCount
 * Gets the number of datastores inside this process of the HXHIM instance
 *
 * @param hx              the HXHIM instance
 * @param datastore_count where to copy the count into
 * @return HXHIM_SUCCESS or HXHIM_ERROR on error
 */
int hxhimGetDatastoreCount(hxhim_t *hx, std::size_t *datastore_count) {
    return hxhim::GetDatastoreCount(hx, datastore_count);
}
