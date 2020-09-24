#include "hxhim/private/hxhim.hpp"
#include "transport/transports.hpp"

int ::Transport::init(hxhim_t *hx,
                      const std::size_t client_ratio,
                      const std::size_t server_ratio,
                      const std::set<int> &endpointgroup,
                      Options *opts) {

    int ret = TRANSPORT_SUCCESS; // can't check hx->p->transport because it will be
                                 // nullptr when transport type is TRANSPORT_NULL

    switch (opts->type) {
        case TRANSPORT_NULL:
            // hash has already been set to SUM_MOD_LOCAL_DATASTORES
            hx->p->transport = nullptr;
            break;
        case TRANSPORT_MPI:
            if (!(hx->p->transport = MPI::init(hx,
                                               client_ratio,
                                               server_ratio,
                                               endpointgroup,
                                               static_cast<MPI::Options *>(opts)))) {
                ret = TRANSPORT_ERROR;
            }
            break;
        #if HXHIM_HAVE_THALLIUM
        case TRANSPORT_THALLIUM:
            if (!(hx->p->transport = Thallium::init(hx,
                                                    client_ratio,
                                                    server_ratio,
                                                    endpointgroup,
                                                    static_cast<Thallium::Options *>(opts)))) {
                ret = TRANSPORT_ERROR;
            }
            break;
        #endif
        default:
            break;
    }

    return ret;
}

int ::Transport::destroy(hxhim_t *hx) {
    if (hxhim::valid(hx)) {
        destruct(hx->p->transport);
        hx->p->transport = nullptr;
        return TRANSPORT_SUCCESS;
    }

    return TRANSPORT_ERROR;
}
