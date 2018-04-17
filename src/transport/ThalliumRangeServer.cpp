#include "ThalliumRangeServer.hpp"

struct thallium_work_item_t : public work_item_t {
    thallium_work_item_t(const thallium::request &req)
        : work_item_t(),
          request(req)
    {}

    const thallium::request &request;
};

const std::string ThalliumRangeServer::CLIENT_TO_RANGE_SERVER_NAME = "receive_rangesrv_work";
mdhim_private_t *ThalliumRangeServer::mdp_= nullptr;

void ThalliumRangeServer::init(mdhim_private_t *mdp) {
    mdp_ = mdp;
}

void ThalliumRangeServer::destroy() {
    mdp_ = nullptr;
}

void ThalliumRangeServer::receive_rangesrv_work(const thallium::request &req, const std::string &data) {
    TransportMessage *message = nullptr;
    if (ThalliumUnpacker::any(&message, data) != MDHIM_SUCCESS) {
        req.respond(MDHIM_ERROR);
    }

    //Create a new work item
    thallium_work_item_t *item = new thallium_work_item_t(req);

    //Set the new buffer to the new item's message
    item->message = message;

    mdhim_t md;
    md.p = mdp_;

    // Add the new item to the work queue
    range_server_add_work(&md, item);
}

int ThalliumRangeServer::send_client_response(work_item *item, TransportMessage *message, volatile int &shutdown) {
    std::string buf = "";

    if (ThalliumPacker::any(message, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // complete the RPC
    dynamic_cast<thallium_work_item_t *>(item)->request.respond(buf);

    return MDHIM_SUCCESS;
}
