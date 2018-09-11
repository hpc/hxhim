#if HXHIM_HAVE_THALLIUM

#include "hxhim/range_server.hpp"
#include "hxhim/utils.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/RangeServer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

const std::string Transport::Thallium::RangeServer::CLIENT_TO_RANGE_SERVER_NAME = "process";
hxhim_t *Transport::Thallium::RangeServer::hx_ = nullptr;

void Transport::Thallium::RangeServer::init(hxhim_t *hx) {
    hx_ = hx;
}

void Transport::Thallium::RangeServer::destroy() {
    hx_ = nullptr;
}

void Transport::Thallium::RangeServer::process(const thallium::request &req, const std::string &data) {
    const std::size_t len = strlen(data.c_str());
    mlog(THALLIUM_INFO, "Processing %zu bytes of data", len);

    // unpack the request
    Request::Request *request = nullptr;
    if (Unpacker::unpack(&request, data, hxhim::GetRequestFBP(hx_), hxhim::GetArrayFBP(hx_), hxhim::GetBufferFBP(hx_)) != TRANSPORT_SUCCESS) {
        req.respond(TRANSPORT_ERROR);
        return;
    }

    mlog(THALLIUM_DBG, "Unpacked data (type %d)", request->type);

    // process the message
    Response::Response *response = hxhim::range_server::range_server(hx_, request);

    mlog(THALLIUM_DBG, "Responding with message of type %d", response->type);

    // release the requests
    hxhim::GetRequestFBP(hx_)->release(request);

    // pack the response
    std::string str;
    Packer::pack(response, str); // do not check for error

    mlog(THALLIUM_DBG, "Packed response into %zu byte string", str.size());

    // release the responses
    hxhim::GetResponseFBP(hx_)->release(response);

    // respond
    req.respond(str);

    mlog(THALLIUM_INFO, "Done processing data");
}

#endif