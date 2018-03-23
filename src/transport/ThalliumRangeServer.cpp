#include <thallium/serialization/stl/string.hpp>

#include "ThalliumRangeServer.hpp"

const std::string ThalliumRangeServer::CLIENT_TO_RANGE_SERVER_NAME = "send_range_server_work";
const std::string ThalliumRangeServer::RANGE_SERVER_NAME_TO_CLIENT = "receive_response";
mdhim_private_t *ThalliumRangeServer::mdp_= nullptr;
std::string ThalliumRangeServer::address_ = "";

void ThalliumRangeServer::init(mdhim_private_t *mdp, const std::string &address) {
    mdp_ = mdp;
    address_ = address;
}

void *ThalliumRangeServer::listener_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    mdhim_t *md = (mdhim_t *) data;

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    thallium::engine engine(address_, THALLIUM_SERVER_MODE);
    engine.define("send_rangesrv_work", receive_rangesrv_work);

    // TODO: make sure this loop doesnt spin
    while (md->p->shutdown) {
        //Clean outstanding sends
        range_server_clean_oreqs(md);
    }

    return NULL;
}

int ThalliumRangeServer::send_locally_or_remote(mdhim_t *md, const int dest, TransportMessage *message) {
    int ret = MDHIM_SUCCESS;
    if (md->p->transport->EndpointID() != dest) {
        //Sends the message remotely

        //engine

        // ret = send_client_response(dest, message, md->p->shutdown);
        // if (*size_req) {
        //     range_server_add_oreq(md, *size_req, sizebuf);
        // }

        // if (*msg_req) {
        //     range_server_add_oreq(md, *msg_req, *sendbuf);
        // }
    } else {
        //Sends the message locally
        pthread_mutex_lock(&md->p->receive_msg_mutex);
        md->p->receive_msg = message;
        pthread_cond_signal(&md->p->receive_msg_ready_cv);
        pthread_mutex_unlock(&md->p->receive_msg_mutex);
    }

    return ret;
}

void ThalliumRangeServer::receive_rangesrv_work(const thallium::request &req, const std::string &data) {
    TransportMessage *message = nullptr;
    if (ThalliumUnpacker::any(&message, data) != MDHIM_SUCCESS) {
        req.respond(MDHIM_ERROR);
    }

    //Create a new work item
    work_item *item = new work_item();

    //Set the new buffer to the new item's message
    item->message = message;
    //Set the source in the work item
    item->address = message->src;

    mdhim_t md;
    md.p = mdp_;

    //Add the new item to the work queue
    range_server_add_work(&md, item);

    req.respond(MDHIM_SUCCESS);
}

std::string send_client_response(const thallium::request &req) {

}
