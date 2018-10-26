#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "transport/backend/MPI/MPI.hpp"
#include "transport/transport.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace MPI {

/**
 * init
 * Initializes MPI inside HXHIM
 *
 * @param hx             the HXHIM instance
 * @param opts           the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int init(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(MPI_INFO, "Starting MPI Initialization");
    if (!hxhim::valid(hx, opts) ||
        !hx->p->transport)       {
        return HXHIM_ERROR;
    }

    // Do not allow MPI_COMM_NULL
    if (hx->p->bootstrap.comm == MPI_COMM_NULL) {
        return TRANSPORT_ERROR;
    }

    Options *config = static_cast<Options *>(opts->p->transport);

    // create a range server
    if (hxhim::range_server::is_range_server(hx->p->bootstrap.rank, opts->p->client_ratio, opts->p->server_ratio)) {
        RangeServer::init(hx, config->listeners);
        hx->p->range_server.destroy = RangeServer::destroy;
        mlog(MPI_INFO, "Created MPI Range Server on rank %d", hx->p->bootstrap.rank);
    }

    EndpointGroup *eg = new EndpointGroup(hx->p->bootstrap.comm,
                                          hx->p->running,
                                          hx->p->memory_pools.packed,
                                          hx->p->memory_pools.responses,
                                          hx->p->memory_pools.arrays,
                                          hx->p->memory_pools.buffers);

    // create mapping between unique IDs and ranks
    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        if (hxhim::range_server::is_range_server(i, opts->p->client_ratio, opts->p->server_ratio)) {
            // MPI ranks map 1:1 with the boostrap MPI rank
            hx->p->transport->AddEndpoint(i, new Endpoint(hx->p->bootstrap.comm, i,
                                                          hx->p->running,
                                                          hx->p->memory_pools.packed,
                                                          hx->p->memory_pools.responses,
                                                          hx->p->memory_pools.arrays,
                                                          hx->p->memory_pools.buffers));

            // if the rank was specified as part of the endpoint group, add the rank to the endpoint group
            if (opts->p->endpointgroup.find(i) != opts->p->endpointgroup.end()) {
                eg->AddID(i, i);
            }
        }
    }

    // remove loopback endpoint
    hx->p->transport->RemoveEndpoint(hx->p->bootstrap.rank);

    // set the endpoint group
    hx->p->transport->SetEndpointGroup(eg);

    mlog(MPI_INFO, "Completed MPI Initialization");
    return TRANSPORT_SUCCESS;
}

}
}
