#include "ThalliumRangeServer.hpp"

mdhim_private_t *ThalliumRangeServer::mdp_= nullptr;

void ThalliumRangeServer::init(mdhim_private_t *mdp) {
    mdp_ = mdp;
}

int ThalliumRangeServer::receive_rangesrv_work(const std::string &data) {
    TransportMessage *message = nullptr;
    // if (ThalliumUnpacker::any(&message, data) != MDHIM_SUCCESS) {
    //     return MDHIM_ERROR;
    // }

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

    return MDHIM_SUCCESS;
}

void *ThalliumRangeServer::listener_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    mdhim_t *md = (mdhim_t *) data;

    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    while (1) {
        if (md->p->shutdown) {
            break;
        }

        //Clean outstanding sends
        range_server_clean_oreqs(md);

        //Receive messages sent to this server
        // TransportMessage *message = nullptr;
        // int src = -1;
        // if (receive_rangesrv_work(&src, &message, md->p->shutdown) != MDHIM_SUCCESS) {
        //     continue;
        // }
    }

    return NULL;
}

int ThalliumRangeServer::send_locally_or_remote(mdhim_t *md, const int dest, TransportMessage *message) {
    return MDHIM_ERROR;
}
