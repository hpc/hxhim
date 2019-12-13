#if HXHIM_HAVE_THALLIUM

#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "transport/Thallium/Thallium.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

/**
 * init
 * Initializes Thallium inside HXHIM
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int init(hxhim_t *hx, hxhim_options_t *opts) {
    // mlog(THALLIUM_INFO, "Starting Thallium Initialization");
    if (!hxhim::valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    Options *config = static_cast<Options *>(opts->p->transport);

    // create the engine (only 1 instance per process)
    Engine_t engine(new thallium::engine(config->module, THALLIUM_SERVER_MODE, true, -1),
                    [](thallium::engine *engine) {
                        // const std::string addr = static_cast<std::string>(engine->self());
                        engine->finalize();
                        delete engine;
                        // mlog(THALLIUM_DBG, "Stopped Thallium engine %s", addr.c_str());
                    });

    // mlog(THALLIUM_DBG, "Created Thallium engine %s", static_cast<std::string>(engine->self()).c_str());

    // wait for every engine to start up
    MPI_Barrier(hx->p->bootstrap.comm);

    // create a range server
    if (hxhim::range_server::is_range_server(hx->p->bootstrap.rank, opts->p->client_ratio, opts->p->server_ratio)) {
        RangeServer::init(hx, engine, config->buffer_size);
        hx->p->range_server.destroy = RangeServer::destroy;
        // mlog(THALLIUM_INFO, "Created Thallium Range Server on rank %d", hx->p->bootstrap.rank);
    }

    // create client to range server RPC
    RPC_t rpc(new thallium::remote_procedure(engine->define(RangeServer::CLIENT_TO_RANGE_SERVER_NAME,
                                                            RangeServer::process)));

    // mlog(THALLIUM_DBG, "Created Thallium RPC");

    // get a mapping of unique IDs to thallium addresses
    std::unordered_map<int, std::string> addrs;
    if (get_addrs(hx->p->bootstrap.comm, *engine, addrs) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    // remove the loopback endpoint
    addrs.erase(hx->p->bootstrap.rank);

    EndpointGroup *eg = new EndpointGroup(engine, rpc, config->buffer_size);

    // create mapping between unique IDs and ranks
    for(decltype(addrs)::value_type const &addr : addrs) {
        if (hxhim::range_server::is_range_server(addr.first, opts->p->client_ratio, opts->p->server_ratio)) {
            Endpoint_t server(new thallium::endpoint(engine->lookup(addr.second)));
            // mlog(THALLIUM_DBG, "Created Thallium endpoint %s", addr.second.c_str());

            // if the rank was specified as part of the endpoint group, add the thallium endpoint to the endpoint group
            if (!opts->p->endpointgroup.size() || (opts->p->endpointgroup.find(addr.first) != opts->p->endpointgroup.end())) {
                eg->AddID(addr.first, server);
                // mlog(THALLIUM_DBG, "Added Thallium endpoint %s to the endpoint group", addr.second.c_str());
            }
        }
    }

    hx->p->transport->SetEndpointGroup(eg);

    // mlog(THALLIUM_INFO, "Completed Thallium transport initialization");
    return TRANSPORT_SUCCESS;
}

}
}

#endif
