#include "transport/backend/local/RangeServer.hpp"

namespace Transport {
namespace local {

int RangeServer::init() {
    return TRANSPORT_SUCCESS;
}

void RangeServer::destroy() {}

/*
 * listener_thread
 * Function for the thread that listens for new messages
 */
void *RangeServer::listener_thread(void *data) {
    //Mlog statements could cause a deadlock on range_server_stop due to canceling of threads
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, nullptr);
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, nullptr);

    return nullptr;
}

}
}
