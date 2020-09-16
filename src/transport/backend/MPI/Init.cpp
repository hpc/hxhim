#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "transport/backend/MPI/MPI.hpp"
#include "transport/transport.hpp"
#include "utils/is_range_server.hpp"
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
Transport *init(hxhim_t *hx, hxhim_options_t *opts) {
    mlog(MPI_INFO, "Starting MPI Initialization");
    if (!hxhim::valid(hx, opts)) {
        return nullptr;
    }

    // Do not allow MPI_COMM_NULL
    if (hx->p->bootstrap.comm == MPI_COMM_NULL) {
        return nullptr;
    }

    Options *config = static_cast<Options *>(opts->p->transport);

    // create a range server
    RangeServer *rs = nullptr;
    if (is_range_server(hx->p->bootstrap.rank, opts->p->client_ratio, opts->p->server_ratio)) {
        rs = construct<RangeServer>(hx, config->listeners);
        mlog(MPI_INFO, "Created MPI Range Server on rank %d", hx->p->bootstrap.rank);
    }

    EndpointGroup *eg = construct<EndpointGroup>(hx->p->bootstrap.comm,
                                                 hx->p->running);

    // create mapping between unique IDs and ranks
    for(int i = 0; i < hx->p->bootstrap.size; i++) {
        if (is_range_server(i, opts->p->client_ratio, opts->p->server_ratio)) {
            // if the rank was specified as part of the endpoint group, add the rank to the endpoint group
            if (opts->p->endpointgroup.find(i) != opts->p->endpointgroup.end()) {
                eg->AddID(i, i);
            }
        }
    }

    mlog(MPI_INFO, "Completed MPI Initialization");
    return construct<Transport>(eg, rs);
}

}
}
