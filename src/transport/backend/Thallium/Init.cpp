#include <unordered_map>

#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "hxhim/RangeServer.hpp"
#include "transport/backend/Thallium/Thallium.hpp"
#include "utils/Stats.hpp"
#include "utils/memory.hpp"
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
Transport::Transport *Transport::Thallium::init(hxhim_t *hx,
                                                const std::size_t client_ratio,
                                                const std::size_t server_ratio,
                                                const std::set<int> &endpointgroup,
                                                Options *opts) {
    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp thallium_init;
    thallium_init.start = ::Stats::now();
    #endif

    int rank = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    mlog(THALLIUM_INFO, "Rank %d Starting Thallium Initialization", rank);

    mlog(THALLIUM_INFO, "Rank %d Configuring Thallium with %s", rank, opts->module.c_str());

    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp thallium_engine;
    thallium_engine.start = ::Stats::now();
    #endif

    // create the engine (only 1 instance per process)
    thallium::engine *engine = construct<thallium::engine>(opts->module, THALLIUM_SERVER_MODE, true, -1);

    #if PRINT_TIMESTAMPS
    thallium_engine.end = ::Stats::now();
    #endif

    mlog(THALLIUM_INFO, "Rank %d Created Thallium engine %s", rank, static_cast<std::string>(engine->self()).c_str());

    // Range server is always created, even if this rank is not a range server
    // because RPC function signatures are needed. Datastores are not tied to
    // range servers, so it should not matter that there are extra range servers.
    RangeServer *rs = construct<RangeServer>(hx, engine);

    #if PRINT_TIMESTAMPS
    ::Stats::Chronostamp thallium_addrs;
    thallium_addrs.start = ::Stats::now();
    #endif

    // get a mapping of unique IDs to thallium addresses
    std::unordered_map<int, std::string> addrs;
    if (get_addrs(hx->p->bootstrap.comm, *engine, addrs) != TRANSPORT_SUCCESS) {
        delete rs;
        return nullptr;
    }

    #if PRINT_TIMESTAMPS
    thallium_addrs.end = ::Stats::now();
    #endif

    // remove the loopback endpoint
    addrs.erase(rank);

    EndpointGroup *eg = construct<EndpointGroup>(engine, rs);

    // create mapping between unique IDs and ranks
    for(decltype(addrs)::value_type const &addr : addrs) {
        if (hxhim::RangeServer::is_range_server(addr.first, client_ratio, server_ratio)) {
            // if the rank was specified as part of the endpoint group, add the thallium endpoint to the endpoint group
            if (!endpointgroup.size() || (endpointgroup.find(addr.first) != endpointgroup.end())) {
                eg->AddID(addr.first, addr.second);
                mlog(THALLIUM_DBG, "Added Thallium endpoint %s to the endpoint group", addr.second.c_str());
            }
        }
    }

    mlog(THALLIUM_INFO, "Rank %d Completed Thallium transport initialization", rank);
    #if PRINT_TIMESTAMPS
    thallium_init.end = ::Stats::now();
    Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "thallium_init",   ::Stats::global_epoch, thallium_init);
    Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "thallium_engine", ::Stats::global_epoch, thallium_engine);
    Stats::print_event(hx->p->print_buffer, hx->p->bootstrap.rank, "thallium_addrs",  ::Stats::global_epoch, thallium_addrs);
    #endif
    return construct<Transport>(eg, rs);
}
