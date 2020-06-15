#include <memory>
#include <vector>

#include <thallium/serialization/stl/string.hpp>

#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"
#include "transport/backend/Thallium/RangeServer.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

const std::string RangeServer::CLIENT_TO_RANGE_SERVER_NAME = "process";
hxhim_t *RangeServer::hx_ = nullptr;
Engine_t RangeServer::engine_ = {};
std::size_t RangeServer::bufsize_ = 0;

void RangeServer::init(hxhim_t *hx, const Engine_t &engine, const std::size_t buffer_size) {
    hx_ = hx;
    engine_ = engine;
    bufsize_ = buffer_size;

    mlog(THALLIUM_INFO, "Initialized Thallium Range Server on rank %d", hx_->p->bootstrap.rank);
}

void RangeServer::destroy() {
    const int rank = hx_->p->bootstrap.rank;
    bufsize_ = 0;
    engine_ = nullptr;
    hx_ = nullptr;
    mlog(THALLIUM_INFO, "Stopped Thallium Range Server on rank %d", rank);
}

void RangeServer::process(const thallium::request &req, thallium::bulk &bulk) {
    thallium::endpoint ep = req.get_endpoint();
    mlog(THALLIUM_INFO, "Starting to process data from %s", ((std::string) ep).c_str());

    void *buf = alloc(bufsize_);

    // receive request
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, bufsize_)};
        thallium::bulk local = engine_->expose(segments, thallium::bulk_mode::write_only);
        bulk.on(ep) >> local(0, bufsize_);
    }

    mlog(THALLIUM_DBG, "Receieved %zu byte request", bufsize_);

    // unpack the request
    Request::Request *request = nullptr;
    if (Unpacker::unpack(&request, buf, bufsize_) != TRANSPORT_SUCCESS) {
        dealloc(buf);
        req.respond((std::size_t) 0);
        mlog(THALLIUM_WARN, "Could not unpack request");
        return;
    }

    mlog(THALLIUM_DBG, "Unpacked %zu bytes of %s request", bufsize_, Message::TypeStr[request->type]);

    // process the request
    Response::Response *response = local::range_server(hx_, request);
    dealloc(request);

    mlog(THALLIUM_DBG, "Datastore responded with %s response", Message::TypeStr[response->type]);

    // pack the response
    std::size_t ressize = 0;
    Packer::pack(response, &buf, &ressize);     // do not check for error
    mlog(THALLIUM_DBG, "Responding with %zu byte %s response", ressize, Message::TypeStr[response->type]);
    dealloc(response);

    mlog(THALLIUM_DBG, "Packed response into %zu byte buffer", ressize);

    // send the response
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(buf, ressize)};
        thallium::bulk local = engine_->expose(segments, thallium::bulk_mode::read_only);
        bulk.on(ep) << local(0, ressize);
    }

    mlog(THALLIUM_DBG, "Done sending %zu byte packed response", ressize);

    // respond
    dealloc(buf);
    req.respond(ressize);

    mlog(THALLIUM_INFO, "Done processing request from %s and sending response", ((std::string) ep).c_str());
}

}
}
