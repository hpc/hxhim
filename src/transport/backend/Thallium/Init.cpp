#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "transport/backend/Thallium/Thallium.hpp"
#include "utils/Stats.hpp"
#include "utils/is_range_server.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * init
 * Initializes Thallium inside HXHIM
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::init(hxhim_t *hx, hxhim_options_t *opts) {
    #ifdef PRINT_TIMESTAMPS
    ::Stats::Chronostamp thallium_init;
    thallium_init.start = ::Stats::now();
    #endif

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(THALLIUM_INFO, "Rank %d Starting Thallium Initialization", rank);

    Options *config = static_cast<Options *>(opts->p->transport);
    mlog(THALLIUM_INFO, "Rank %d Configuring Thallium with %s", rank, config->module.c_str());

    #ifdef PRINT_TIMESTAMPS
    ::Stats::Chronostamp thallium_engine;
    thallium_engine.start = ::Stats::now();
    #endif

    // create the engine (only 1 instance per process)
    Engine_t engine(new thallium::engine(config->module, THALLIUM_SERVER_MODE, true, -1),
                    [](thallium::engine *engine) {
                        const std::string addr = static_cast<std::string>(engine->self());
                        mlog(THALLIUM_DBG, "Stopping Thallium engine %s", addr.c_str());
                        engine->finalize();
                        delete engine;
                        mlog(THALLIUM_INFO, "Stopped Thallium engine %s", addr.c_str());
                    });
    #ifdef PRINT_TIMESTAMPS
    thallium_engine.end = ::Stats::now();
    #endif

    mlog(THALLIUM_INFO, "Rank %d Created Thallium engine %s", rank, static_cast<std::string>(engine->self()).c_str());

    // create a range server
    if (is_range_server(rank, opts->p->client_ratio, opts->p->server_ratio)) {
        RangeServer::init(hx, engine);
        hx->p->range_server.destroy = RangeServer::destroy;
        mlog(THALLIUM_INFO, "Rank %d Created Thallium Range Server", rank);
    }

    // create client to range server RPC
    RPC_t process_rpc(new thallium::remote_procedure(engine->define(RangeServer::PROCESS_RPC_NAME,
                                                                    RangeServer::process)));

    RPC_t cleanup_rpc(new thallium::remote_procedure(engine->define(RangeServer::CLEANUP_RPC_NAME,
                                                                    RangeServer::cleanup).disable_response()));

    mlog(THALLIUM_DBG, "Rank %d Created Thallium RPC", rank);

    #ifdef PRINT_TIMESTAMPS
    ::Stats::Chronostamp thallium_addrs;
    thallium_addrs.start = ::Stats::now();
    #endif

    // get a mapping of unique IDs to thallium addresses
    std::unordered_map<int, std::string> addrs;
    if (get_addrs(hx->p->bootstrap.comm, *engine, addrs) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    #ifdef PRINT_TIMESTAMPS
    thallium_addrs.end = ::Stats::now();
    #endif

    // remove the loopback endpoint
    addrs.erase(rank);

    EndpointGroup *eg = new EndpointGroup(engine, process_rpc, cleanup_rpc);

    // create mapping between unique IDs and ranks
    for(decltype(addrs)::value_type const &addr : addrs) {
        if (is_range_server(addr.first, opts->p->client_ratio, opts->p->server_ratio)) {
            Endpoint_t server(new thallium::endpoint(engine->lookup(addr.second)));

            // if the rank was specified as part of the endpoint group, add the thallium endpoint to the endpoint group
            if (!opts->p->endpointgroup.size() || (opts->p->endpointgroup.find(addr.first) != opts->p->endpointgroup.end())) {
                eg->AddID(addr.first, server);
                mlog(THALLIUM_DBG, "Added Thallium endpoint %s to the endpoint group", addr.second.c_str());
            }
        }
    }

    hx->p->transport->SetEndpointGroup(eg);

    mlog(THALLIUM_INFO, "Rank %d Completed Thallium transport initialization", rank);
    #ifdef PRINT_TIMESTAMPS
    thallium_init.end = ::Stats::now();
    Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "thallium_init",   ::Stats::global_epoch, thallium_init);
    Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "thallium_engine", ::Stats::global_epoch, thallium_engine);
    Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "thallium_addrs",  ::Stats::global_epoch, thallium_addrs);
    #endif
    return TRANSPORT_SUCCESS;
}
