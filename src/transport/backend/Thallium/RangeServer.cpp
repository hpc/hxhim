#if HXHIM_HAVE_THALLIUM

#include <memory>
#include <vector>

#include <thallium/serialization/stl/string.hpp>

#include "hxhim/private.hpp"
#include "hxhim/range_server.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/RangeServer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

const std::string RangeServer::CLIENT_TO_RANGE_SERVER_NAME = "process";
hxhim_t *RangeServer::hx_ = nullptr;
Engine_t RangeServer::engine_ = {};

void RangeServer::init(hxhim_t *hx, const Engine_t &engine) {
    hx_ = hx;
    engine_ = engine;
}

void RangeServer::destroy() {
    hx_ = nullptr;
}

void RangeServer::process(const thallium::request &req, thallium::bulk &bulk) {
    thallium::endpoint ep = req.get_endpoint();

    const std::size_t bufsize = 1024;

    #warning need to get buf size programatically
    void *buf = alloc(bufsize);

    // receive request
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, bufsize)};
        thallium::bulk local = engine_->expose(segments, thallium::bulk_mode::write_only);
        bulk.on(ep) >> local(0, bufsize);
    }

    // mlog(THALLIUM_DBG, "Processing %zu bytes of data", bufsize);

    // unpack the request
    Request::Request *request = nullptr;
    if (Unpacker::unpack(&request, buf, bufsize) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        req.respond((std::size_t) 0);
        // mlog(THALLIUM_DBG, "Could not unpack data");
        return;
    }

    // mlog(THALLIUM_DBG, "Unpacked %s data", Message::TypeStr[request->type]);

    // process the request
    Response::Response *response = hxhim::range_server::range_server(hx_, request);
    dealloc(request);

    // mlog(THALLIUM_DBG, "Responding with %s message", Message::TypeStr[response->type]);

    // pack the response
    std::size_t ressize = 0;
    Packer::pack(response, &buf, &ressize);     // do not check for error
    response->server_side_cleanup();
    dealloc(response);

    // mlog(THALLIUM_DBG, "Packed response into %zu byte string", ressize);

    // send the response
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, ressize)};
        thallium::bulk local = engine_->expose(segments, thallium::bulk_mode::read_only);
        bulk.on(ep) << local(0, ressize);
    }

    // respond
    dealloc(buf);
    req.respond(ressize);

    // mlog(THALLIUM_DBG, "Done processing data");
}

}
}

#endif
