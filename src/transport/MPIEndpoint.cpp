#include "MPIEndpoint.hpp"

#define HXHIM_MPI_REQUEST_TAG 0x311

MPIEndpoint::MPIEndpoint(const MPI_Comm comm, volatile int &shutdown)
    : TransportEndpoint(),
      MPITransportBase(comm, shutdown),
      address_(rank_)
{}

int MPIEndpoint::AddPutRequest(const TransportPutMessage *message) {
    if (!message) {
        return MDHIM_ERROR;
    }

    void *buf = nullptr;
    int bufsize;

    if (MPIPacker::pack(this, message, &buf, &bufsize) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    const int ret = send_rangesrv_work(message->server_rank, buf, bufsize);

    // cleanup
    free(buf);

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while sending "
             "bget record request", rank_, ret);
    }

    return ret;
}

int MPIEndpoint::AddGetRequest(const TransportBGetMessage *message) {
    throw;
    // if (!message) {
    //     return MDHIM_ERROR;
    // }

    // void *buf = nullptr;
    // int size;

    // // encode the mesage
    // if (MPIPacker::pack(this, message, &buf, &size) != MDHIM_SUCCESS) {
    //     mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: MPIEndpoint Packing message "
    //          "failed before sending.", rank_);
    //     return MDHIM_ERROR;
    // }

    // // send the message
    // int dest = message->server_rank;
    // const int ret = send_all_rangesrv_work(&buf, &size, &dest, 1);

    // // cleanup
    // free(buf);

    // if (ret != MDHIM_SUCCESS) {
    //     mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while sending "
    //          "bget record request", rank_, ret);
    // }

    // return ret;
    return MDHIM_ERROR;
}

int MPIEndpoint::AddPutReply(const TransportAddress *src, TransportRecvMessage **message) {
    if (!src || !message) {
        return MDHIM_ERROR;
    }

    void *recvbuf = nullptr;
    int recvsize = 0; // initializing this value helps; someone is probably writing an int instead of a int
    int ret = MDHIM_ERROR;

    if ((ret = receive_client_response(dynamic_cast<const MPIAddress*>(src)->Rank(), &recvbuf, &recvsize)) == MDHIM_SUCCESS) {
        ret = MPIUnpacker::unpack(this, message, recvbuf, recvsize);
    }

    free(recvbuf);

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
             "put record request", rank_, ret);
        (*message)->error = MDHIM_ERROR;
    }

    return ret;
}

int MPIEndpoint::AddGetReply(const TransportAddress *srcs, TransportBGetRecvMessage ***messages) {
    throw;
    // if (!srcs || !messages) {
    //     return MDHIM_ERROR;
    // }

    // char **recvbufs = nullptr;
    // int *sizebuf = nullptr;
    // int ret = MDHIM_ERROR;
    // int src = dynamic_cast<const MPIAddress *>(srcs)->Rank();

    // if ((ret = receive_all_client_responses(&src, nsrcs, &recvbufs, &sizebuf)) == MDHIM_SUCCESS) {
    //     ret = MPIUnpacker::unpack(this, *messages, *recvbufs, *sizebuf);
    //     free(recvbufs[i]);
    // }

    // free(recvbufs);
    // free(sizebuf);

    // // If the receives did not succeed then log the error code and return MDHIM_ERROR
    // if (ret != MDHIM_SUCCESS) {
    //     mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while receiving "
    //          "bget record requests", rank_, ret);
    // }

    // return ret;
    return MDHIM_ERROR;
}

std::size_t MPIEndpoint::PollForMessage(std::size_t timeoutSecs) {
    int nbytes = 0;

    // Poll to see if a message is waiting
    int msgWaiting = 0;
    MPI_Status status = {};
    int rc = MPI_Iprobe(MPI_ANY_SOURCE, HXHIM_MPI_REQUEST_TAG, comm_, &msgWaiting, &status);
    if (rc == 0 && msgWaiting == 1) {
        // Get the message size
        int bytecount;
        rc = MPI_Get_count(&status, MPI_BYTE, &bytecount);
        if (rc == 0) {
            nbytes = (size_t) bytecount;
        } else {
            std::cerr << __FILE__ << ":" << __LINE__ << ":Failed determing message size\n";
        }
    }
    else {
        std::cerr << "No message waiting\n";
    }
   return nbytes;
}

std::size_t MPIEndpoint::WaitForMessage(std::size_t timeoutSecs) {
    return 0;
}

int MPIEndpoint::Flush() {
    return 0;
}

const TransportAddress *MPIEndpoint::Address() const {
    return &address_;
}
