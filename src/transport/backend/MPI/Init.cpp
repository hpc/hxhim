#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "transport/backend/MPI/MPI.hpp"
#include "transport/transport.hpp"
#include "utils/is_range_server.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * init
 * Initializes MPI inside HXHIM
 *
 * @param hx             the HXHIM instance
 * @param opts           the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
Transport::Transport *Transport::MPI::init(hxhim_t *hx,
                                           const std::size_t client_ratio,
                                           const std::size_t server_ratio,
                                           const std::set<int> &endpointgroup,
                                           Options *opts) {
    mlog(MPI_INFO, "Starting MPI Initialization");

    // Do not allow MPI_COMM_NULL
    if (hx->p->bootstrap.comm == MPI_COMM_NULL) {
        return nullptr;
    }

    // create a range server
    RangeServer *rs = nullptr;
    if (is_range_server(hx->p->bootstrap.rank, client_ratio, server_ratio)) {
        rs = construct<RangeServer>(hx, opts->listeners);
        mlog(MPI_INFO, "Created MPI Range Server on rank %d", hx->p->bootstrap.rank);
    }

    EndpointGroup *eg = construct<EndpointGroup>(hx->p->bootstrap.comm,
                                                 hx->p->running);

    // create mapping between unique IDs and ranks
    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        if (is_range_server(i, client_ratio, server_ratio)) {
            // if the rank was specified as part of the endpoint group, add the rank to the endpoint group
            if (endpointgroup.find(i) != endpointgroup.end()) {
                eg->AddID(i, i);
            }
        }
    }

    mlog(MPI_INFO, "Completed MPI Initialization");
    return construct<Transport>(eg, rs);
}
