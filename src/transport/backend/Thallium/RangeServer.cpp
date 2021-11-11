#include <memory>
#include <vector>

#include <thallium/serialization/stl/string.hpp>

#include "hxhim/accessors.hpp"
#include "transport/backend/Thallium/RangeServer.hpp"
#include "transport/backend/local/RangeServer.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const std::string Transport::Thallium::RangeServer::PROCESS_RPC_NAME = "process";
const std::string Transport::Thallium::RangeServer::CLEANUP_RPC_NAME = "cleanup";

Transport::Thallium::RangeServer::RangeServer(hxhim_t *hx, thallium::engine *engine)
    : hx(hx),
      engine(engine),
      rank(-1),
      process_rpc(engine->define(PROCESS_RPC_NAME,
                                 [this](const thallium::request &req,
                                        const std::size_t req_len,
                                        thallium::bulk &bulk) {
                                     return this->process(req, req_len, bulk);
                                 }
                      )

          ),
      cleanup_rpc(engine->define(CLEANUP_RPC_NAME,
                                 [this](const thallium::request &req,
                                        uintptr_t addr) {
                                     return this->cleanup(req, addr);
                                 }
                      ).disable_response()
          )
{
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, nullptr);
    mlog(THALLIUM_INFO, "Initialized Thallium Range Server on rank %d", rank);
}

Transport::Thallium::RangeServer::~RangeServer() {
    mlog(THALLIUM_INFO, "Stopped Thallium Range Server on rank %d", rank);
}

const thallium::remote_procedure &Transport::Thallium::RangeServer::process() const {
    return process_rpc;
}

const thallium::remote_procedure &Transport::Thallium::RangeServer::cleanup() const {
    return cleanup_rpc;
}

void Transport::Thallium::RangeServer::process(const thallium::request &req, const std::size_t req_len, thallium::bulk &bulk) {
    thallium::endpoint ep = req.get_endpoint();
    mlog(THALLIUM_INFO, "Rank %d RangeServer Starting to process data from %s", rank, ((std::string) ep).c_str());

    void *req_buf = alloc(req_len);

    // receive request
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(req_buf, req_len)};
        thallium::bulk local = engine->expose(segments, thallium::bulk_mode::write_only);
        bulk.on(ep) >> local;
    }

    mlog(THALLIUM_DBG, "Rank %d RangeServer Receieved %zu byte request", rank, req_len);

    // unpack the request
    Message::Request::Request *request = nullptr;
    if (Message::Unpacker::unpack(&request, req_buf, req_len) != MESSAGE_SUCCESS) {
        dealloc(req_buf);
        req.respond((std::size_t) 0, thallium::bulk());
        mlog(THALLIUM_WARN, "Could not unpack request");
        return;
    }
    dealloc(req_buf);

    mlog(THALLIUM_DBG, "Rank %d RangeServer Unpacked %zu bytes of %s request", rank, req_len, HXHIM_OP_STR[request->op]);

    // process the request
    mlog(THALLIUM_DBG, "Rank %d Sending %s to Local RangeServer", rank, HXHIM_OP_STR[request->op]);
    Message::Response::Response *response = local::range_server(hx, request);
    destruct(request);

    mlog(THALLIUM_DBG, "Rank %d Local RangeServer responded with %s response", rank, HXHIM_OP_STR[response->op]);

    // pack the response
    std::size_t res_len = 0;
    void *res_buf = nullptr;
    Message::Packer::pack(response, &res_buf, &res_len);     // do not check for error
    mlog(THALLIUM_DBG, "Rank %d RangeServer Responding with %zu byte %s response", rank, res_len, HXHIM_OP_STR[response->op]);
    destruct(response);

    mlog(THALLIUM_DBG, "Rank %d RangeServer Packed response into %zu byte buffer", rank, res_len);

    // send the response
    {
        std::vector<std::pair<void *, std::size_t> > segments = {std::make_pair(res_buf, res_len)};
        thallium::bulk local = engine->expose(segments, thallium::bulk_mode::read_only);
        req.respond(res_len, local, (uintptr_t) res_buf);
    }

    mlog(THALLIUM_DBG, "Rank %d RangeServer Done sending %zu byte packed response", rank, res_len);

    mlog(THALLIUM_INFO, "Rank %d RangeServer Done processing request from %s and sending response", rank, ((std::string) ep).c_str());
}

void Transport::Thallium::RangeServer::cleanup(const thallium::request &, uintptr_t addr) {
    // res_buf is cleaned up here
    // since freeing in process will result in the other side given access to freed memory
    dealloc((void *) addr);
}
