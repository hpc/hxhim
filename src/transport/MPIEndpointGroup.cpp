#if 0
#include "MPIEndpointGroup.hpp"

MPIEndpointGroup::MPIEndpointGroup(MPI_Comm comm, volatile int &shutdown)
  : TransportEndpointGroup(),
    MPIEndpointBase(comm, shutdown),
    address_(rank_)
{}

MPIEndpointGroup::~MPIEndpointGroup() {}

int MPIEndpointGroup::AddBPutRequest(TransportBPutMessage **messages, int num_srvs) {
    void **sendbufs = (void **)malloc(num_srvs * sizeof(void *));
    int *sizes = new int[num_srvs]();
    int *dests = new int[num_srvs]();
    int ret = MDHIM_SUCCESS;

    // encode each mesage
    for(int i = 0; i < num_srvs; i++) {
        if (messages[i] && MPIPacker::pack(this, messages[i], sendbufs + i, sizes + i) != MDHIM_SUCCESS) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: MPIEndpoint Packing message "
                 "failed before sending.", rank_);
            ret = MDHIM_ERROR;
            sendbufs[i] = nullptr;
            sizes[i] = 0;
        }

        dests[i] = messages[i]->dst;
    }

    // send all of the messages at once
    ret = send_all_rangesrv_work(sendbufs, sizes, dests, num_srvs);

    // cleanup
    for (int i = 0; i < num_srvs; i++) {
        free(sendbufs[i]);
    }

    free(sendbufs);
    delete [] sizes;
    delete [] dests;

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while sending "
             "bput record request", rank_, ret);
    }

    return ret;
}

int MPIEndpointGroup::AddBGetRequest(TransportBGetMessage **messages, int num_srvs) {
    void **sendbufs = (void **)malloc(num_srvs * sizeof(void *));
    int *sizes = new int[num_srvs]();
    int *dests = new int[num_srvs]();
    int ret = MDHIM_SUCCESS;

    // encode each mesage
    for(int i = 0; i < num_srvs; i++) {
        if (messages[i] && MPIPacker::pack(this, messages[i], sendbufs + i, sizes + i) != MDHIM_SUCCESS) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: MPIEndpoint Packing message "
                 "failed before sending.", rank_);
            ret = MDHIM_ERROR;
            sendbufs[i] = nullptr;
            sizes[i] = 0;
        }

        dests[i] = messages[i]->dst;
    }

    // send all of the messages at once
    ret = send_all_rangesrv_work(sendbufs, sizes, dests, num_srvs);

    // cleanup
    for (int i = 0; i < num_srvs; i++) {
        free(sendbufs[i]);
    }

    free(sendbufs);
    delete [] sizes;
    delete [] dests;

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while sending "
             "bget record request", rank_, ret);
    }

    return ret;
}

int MPIEndpointGroup::AddBPutReply(TransportRecvMessage **messages) {
    if (!srcs || !messages) {
        return MDHIM_ERROR;
    }

    void **recvbufs = nullptr;
    int *sizebuf = nullptr;
    int ret = MDHIM_ERROR;

    // get a list of ranks
    // TODO: use src directly
    int *srcsarr = new int[nsrcs];
    for(int i = 0; i < nsrcs; i++) {
        srcsarr[i] = dynamic_cast<const MPIAddress *>(srcs)[i].Rank();
    }

    // Receive packed data
    if ((ret = receive_all_client_responses(srcsarr, nsrcs, &recvbufs, &sizebuf)) == MDHIM_SUCCESS) {
        // Unpack data into objects
        for (int i = 0; i < nsrcs; i++) {
            ret = MPIUnpacker::unpack(this, messages + i, recvbufs[i], sizebuf[i]);
            free(recvbufs[i]);
        }
    }

    free(recvbufs);
    free(sizebuf);
    delete [] srcsarr;

    // If the receives did not succeed then log the error code and return MDHIM_ERROR
    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while receiving "
             "bget record requests", rank_, ret);
    }

    return ret;
}

int MPIEndpointGroup::AddBGetReply(TransportBGetRecvMessage **messages) {
    if (!srcs || !messages) {
        return MDHIM_ERROR;
    }

    void **recvbufs = nullptr;
    int *sizebuf = nullptr;
    int ret = MDHIM_ERROR;

    // get a list of ranks
    // TODO: use src directly
    int *srcsarr = new int[nsrcs];
    for(int i = 0; i < nsrcs; i++) {
        srcsarr[i] = dynamic_cast<const MPIAddress *>(srcs)[i].Rank();
    }

    // Receive packed data
    if ((ret = receive_all_client_responses(srcsarr, nsrcs, &recvbufs, &sizebuf)) == MDHIM_SUCCESS) {
        // Unpack data into objects
        for (int i = 0; i < nsrcs; i++) {
            ret = MPIUnpacker::unpack(this, messages + i, recvbufs[i], sizebuf[i]);
            free(recvbufs[i]);
        }
    }

    free(recvbufs);
    free(sizebuf);
    delete [] srcsarr;

    // If the receives did not succeed then log the error code and return MDHIM_ERROR
    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while receiving "
             "bget record requests", rank_, ret);
    }

    return ret;
}

#endif
