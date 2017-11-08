//
// Created by bws on 8/30/17.
//

#include "comm_mpi.h"
#include <cassert>
#include <iostream>
using namespace std;

#define HXHIM_MPI_REQUEST_TAG 0x311

/** Singleton accessor */
const MPIInstance& MPIInstance::instance() {
    static MPIInstance instance;
    return instance;
}

/** Constructor */
MPIInstance::MPIInstance() {
    int mpiIsInitialized = 0;
    int rc = MPI_Initialized(&mpiIsInitialized);
    if (!mpiIsInitialized) {
        int argc = 0;
        char** argv = 0;
        MPI_Init(&argc, &argv);
        wasInitializedHere_ = 1;
    }

    // Retrieve world rank and size
    rc = MPI_Comm_rank(MPI_COMM_WORLD, &worldRank_);
    assert(0 == rc);
    rc = MPI_Comm_size(MPI_COMM_WORLD, &worldSize_);

    // Create a communicator so that all address spaces join it
    instanceComm_ = MPI_COMM_WORLD;
    instanceRank_ = worldRank_;
    instanceSize_ = worldSize_;
}

/** Destructor */
MPIInstance::~MPIInstance() {
    MPI_Finalize();
}

/** Constructor */
MPIEndpoint::MPIEndpoint() :
        CommEndpoint(),
        mpi_(MPIInstance::instance()) {
    int rc = MPI_Comm_rank(MPI_COMM_WORLD, &rank_);
    if (rc != 0) {
        MPI_Abort(MPI_COMM_WORLD, rc);
    }
}

MPIEndpoint::MPIEndpoint(int rank) :
        CommEndpoint(),
        mpi_(MPIInstance::instance()),
        rank_(rank) {
    assert(rank >= 0 && rank < mpi_.Size());
}

int MPIEndpoint::AddPutRequestImpl(void* kbuf, std::size_t kbytes, void* vbuf, std::size_t vbytes) {
    MPI_Request req;
    void* pbuf = 0;
    int pbytes = PackRequest(&pbuf, CommMessage::PUT, kbuf, kbytes, vbuf, vbytes);
    int rc = MPI_Isend(pbuf, pbytes, MPI_PACKED, rank_, HXHIM_MPI_REQUEST_TAG, mpi_.Comm(), &req);
    return rc;
}

int MPIEndpoint::AddGetRequestImpl(void *kbuf, std::size_t kbytes, void *vbuf, std::size_t vbytes) {
    MPI_Request req;
    void* pbuf = 0;
    int pbytes = PackRequest(&pbuf, CommMessage::GET, kbuf, kbytes, 0, 0);
    int rc = MPI_Isend(pbuf, pbytes, MPI_PACKED, rank_, HXHIM_MPI_REQUEST_TAG, mpi_.Comm(), &req);
    return rc;
}

int MPIEndpoint::AddGetReplyImpl(void *buf, std::size_t bytes) {
    return -1;
}

int MPIEndpoint::AddPutReplyImpl(void *buf, std::size_t bytes) {
    return -1;
}

int MPIEndpoint::ReceiveRequestImpl(size_t rbytes, CommMessage::Type* requestType, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes) {
    MPI_Status status;
    void* rbuf = malloc(rbytes);
    int rc = MPI_Recv(rbuf, (int)rbytes, MPI_PACKED, MPI_ANY_SOURCE, HXHIM_MPI_REQUEST_TAG, mpi_.Comm(), &status);
    UnpackRequest(rbuf, rbytes, requestType, kbuf, kbytes, vbuf, vbytes);
    return rc;
}

size_t MPIEndpoint::PollForMessageImpl(size_t timeoutSecs) {
    std::size_t nbytes = 0;

    // Poll to see if a message is waiting
    int msgWaiting = 0;
    MPI_Status status;
    int rc = MPI_Iprobe(MPI_ANY_SOURCE, HXHIM_MPI_REQUEST_TAG, mpi_.Comm(), &msgWaiting, &status);
    if (rc == 0 && msgWaiting == 1) {
        // Get the message size
        int bytecount;
        rc = MPI_Get_count(&status, MPI_BYTE, &bytecount);
        if (rc == 0) {
            nbytes = bytecount;
        } else {
            cerr << __FILE__ << ":" << __LINE__ << ":Failed determing message size\n";
        }
    }
    else {
        cerr << "No message waiting\n";
    }
    return nbytes;
}

size_t MPIEndpoint::WaitForMessageImpl(size_t timeoutSecs) {
    return -1;
}

int MPIEndpoint::FlushImpl() {
    return -1;
}

size_t MPIEndpoint::PackRequest(void** pbuf, CommMessage::Type request, void* kbuf, size_t kbytes, void* vbuf, size_t vbytes) {
    size_t pbytes = sizeof(request) + sizeof(kbytes) + kbytes + sizeof(vbytes) + vbytes;
    *pbuf = malloc(pbytes);
    int ppos = 0;
    int rc = 0;
    rc = MPI_Pack(&request, 1, MPI_BYTE, *pbuf, pbytes, &ppos, mpi_.Comm());
    rc = MPI_Pack(&kbytes, 1, MPI_UNSIGNED_LONG, *pbuf, pbytes, &ppos, mpi_.Comm());
    rc = MPI_Pack(&vbytes, 1, MPI_UNSIGNED_LONG, *pbuf, pbytes, &ppos, mpi_.Comm());
    rc = MPI_Pack(kbuf, kbytes, MPI_BYTE, *pbuf, pbytes, &ppos, mpi_.Comm());
    rc = MPI_Pack(vbuf, vbytes, MPI_BYTE, *pbuf, pbytes, &ppos, mpi_.Comm());
    return ppos;
}

void MPIEndpoint::UnpackRequest(void* buf, size_t bytes, CommMessage::Type* request, void** kbuf, size_t* kbytes, void** vbuf, size_t* vbytes) {
    int rc = 0;
    int bpos = 0;
    rc = MPI_Unpack(buf, bytes, &bpos, request, 1, MPI_BYTE, mpi_.Comm());
    rc = MPI_Unpack(buf, bytes, &bpos, kbytes, 1, MPI_UNSIGNED_LONG, mpi_.Comm());
    rc = MPI_Unpack(buf, bytes, &bpos, vbytes, 1, MPI_UNSIGNED_LONG, mpi_.Comm());
    rc = MPI_Unpack(buf, bytes, &bpos, *kbuf, *kbytes, MPI_BYTE, mpi_.Comm());
    rc = MPI_Unpack(buf, bytes, &bpos, *vbuf, *vbytes, MPI_BYTE, mpi_.Comm());
}
