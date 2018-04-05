#include "ThalliumRangeServer.hpp"

const std::string ThalliumRangeServer::CLIENT_TO_RANGE_SERVER_NAME = "receive_rangesrv_work";
const std::string ThalliumRangeServer::RANGE_SERVER_TO_CLIENT_NAME = "receive_response";
mdhim_private_t *ThalliumRangeServer::mdp_= nullptr;

void ThalliumRangeServer::init(mdhim_private_t *mdp) {
    mdp_ = mdp;
}

void ThalliumRangeServer::receive_rangesrv_work(const thallium::request &req, const std::string &data) {
    std::cout << "Got " << data.size() << std::endl;
    TransportMessage *message = nullptr;
    if (ThalliumUnpacker::any(&message, data) != MDHIM_SUCCESS) {
        req.respond(MDHIM_ERROR);
    }

    //Create a new work item
    work_item_t *item = Memory::FBP_MEDIUM::Instance().acquire<work_item>();

    //Set the new buffer to the new item's message
    item->message = message;
    //Set the source in the work item
    item->address = message->src;

    mdhim_t md;
    md.p = mdp_;

    // Add the new item to the work queue
    range_server_add_work(&md, item);

    req.respond((int) MDHIM_SUCCESS);
}

int ThalliumRangeServer::send_client_response(int dest, TransportMessage *message, volatile int &shutdown) {
    return MDHIM_ERROR;
}
