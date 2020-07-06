#include <memory>
#include <vector>

#include <thallium/serialization/stl/string.hpp>

#include "hxhim/accessors.hpp"
#include "hxhim/hxhim.hpp"
#include "transport/backend/Thallium/RangeServer.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

const std::string RangeServer::PROCESS_RPC_NAME = "process";
const std::string RangeServer::CLEANUP_RPC_NAME = "cleanup";
hxhim_t *RangeServer::hx_ = nullptr;
Engine_t RangeServer::engine_ = {};

void RangeServer::init(hxhim_t *hx, const Engine_t &engine) {
    hx_ = hx;
    engine_ = engine;

    int rank = -1;
    hxhim::GetMPI(hx_, nullptr, &rank, nullptr);

    mlog(THALLIUM_INFO, "Initialized Thallium Range Server on rank %d", rank);
}

void RangeServer::destroy() {
    int rank = -1;
    hxhim::GetMPI(hx_, nullptr, &rank, nullptr);

    engine_ = nullptr;
    hx_ = nullptr;
    mlog(THALLIUM_INFO, "Stopped Thallium Range Server on rank %d", rank);
}

void RangeServer::process(const thallium::request &req, const std::size_t req_len, thallium::bulk &bulk) {
    int rank = -1;
    hxhim::GetMPI(hx_, nullptr, &rank, nullptr);

    thallium::endpoint ep = req.get_endpoint();
    mlog(THALLIUM_INFO, "Rank %d RangeServer Starting to process data from %s", rank, ((std::string) ep).c_str());

    void *req_buf = alloc(req_len);

    // receive request
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(req_buf, req_len)};
        thallium::bulk local = engine_->expose(segments, thallium::bulk_mode::write_only);
        bulk.on(ep) >> local;
    }

    mlog(THALLIUM_DBG, "Rank %d RangeServer Receieved %zu byte request", rank, req_len);

    // unpack the request
    Request::Request *request = nullptr;
    if (Unpacker::unpack(&request, req_buf, req_len) != TRANSPORT_SUCCESS) {
        dealloc(req_buf);
        req.respond((std::size_t) 0, thallium::bulk());
        mlog(THALLIUM_WARN, "Could not unpack request");
        return;
    }

    mlog(THALLIUM_DBG, "Rank %d RangeServer Unpacked %zu bytes of %s request", rank, req_len, Message::TypeStr[request->type]);

    // process the request
    mlog(THALLIUM_DBG, "Rank %d Sending %s to Local RangeServer", rank, Message::TypeStr[request->type]);
    Response::Response *response = local::range_server(hx_, request);
    dealloc(request);

    mlog(THALLIUM_DBG, "Rank %d Local RangeServer responded with %s response", rank, Message::TypeStr[response->type]);

    // pack the response
    std::size_t res_len = 0;
    void *res_buf = nullptr;
    Packer::pack(response, &res_buf, &res_len);     // do not check for error
    mlog(THALLIUM_DBG, "Rank %d RangeServer Responding with %zu byte %s response", rank, res_len, Message::TypeStr[response->type]);
    dealloc(response);

    mlog(THALLIUM_DBG, "Rank %d RangeServer Packed response into %zu byte buffer", rank, res_len);

    // send the response
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(res_buf, res_len)};
        thallium::bulk local = engine_->expose(segments, thallium::bulk_mode::read_only);
        req.respond(res_len, local, (uintptr_t) res_buf);
    }

    mlog(THALLIUM_DBG, "Rank %d RangeServer Done sending %zu byte packed response", rank, res_len);

    mlog(THALLIUM_INFO, "Rank %d RangeServer Done processing request from %s and sending response", rank, ((std::string) ep).c_str());
}

void RangeServer::cleanup(const thallium::request &, uintptr_t addr) {
    // res_buf is cleaned up here
    // since freeing in process will result in the other side given access to freed memory
    dealloc((void *) addr);
}

}
}
