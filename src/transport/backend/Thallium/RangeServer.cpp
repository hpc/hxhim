#include "hxhim/range_server.hpp"
#include "transport/backend/Thallium/Packer.hpp"
#include "transport/backend/Thallium/RangeServer.hpp"
#include "transport/backend/Thallium/Unpacker.hpp"

const std::string Transport::Thallium::RangeServer::CLIENT_TO_RANGE_SERVER_NAME = "process";
hxhim_t *Transport::Thallium::RangeServer::hx_ = nullptr;

void Transport::Thallium::RangeServer::init(hxhim_t *hx) {
    hx_ = hx;
}

void Transport::Thallium::RangeServer::destroy() {
    hx_ = nullptr;
}

void Transport::Thallium::RangeServer::process(const thallium::request &req, const std::string &data) {
    // unpack the request
    Request::Request *request = nullptr;
    if (Unpacker::unpack(&request, data) != TRANSPORT_SUCCESS) {
        req.respond(TRANSPORT_ERROR);
        return;
    }

    // process the message
    Response::Response *response = hxhim::range_server::range_server(hx_, request);

    // pack the response
    std::string str;
    Packer::pack(response, str); // do not check for error

    // respond
    req.respond(str);
}
