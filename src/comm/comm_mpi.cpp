//
// Created by bws on 8/30/17.
//

#include "comm_mpi.h"
#include <cassert>
#include <iostream>
using namespace std;

#define HXHIM_MPI_TAG 0x311

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

int MPIEndpoint::PutMessageImpl(void* buf, std::size_t nbytes) {
    MPI_Request req;
    int rc = MPI_Isend(buf, (int)nbytes, MPI_BYTE, rank_, HXHIM_MPI_TAG, mpi_.Comm(), &req);
    return rc;
}

int MPIEndpoint::GetMessageImpl(void* buf, std::size_t& nbytes) {
    return -1;
}

int MPIEndpoint::ReceiveMessageImpl(void* buf, std::size_t nbytes) {
    MPI_Status status;
    int rc = MPI_Recv(buf, (int)nbytes, MPI_BYTE, MPI_ANY_SOURCE, HXHIM_MPI_TAG, mpi_.Comm(), &status);
    return rc;
}

size_t MPIEndpoint::PollForMessageImpl(size_t timeoutSecs) {
    std::size_t nbytes = 0;

    // Poll to see if a message is waiting
    int msgWaiting = 0;
    MPI_Status status;
    int rc = MPI_Iprobe(MPI_ANY_SOURCE, HXHIM_MPI_TAG, mpi_.Comm(), &msgWaiting, &status);
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
