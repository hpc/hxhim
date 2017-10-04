//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_COMM_MPI_H
#define HXHIM_COMM_MPI_H

#include "comm.h"
#include "mpi.h"

/**
 * Singleton helper class that initializes MPI exactly once if it is not initialized
 */
class MPIInstance {
public:
    /** @return the unique instance of MPIInstance */
    static const MPIInstance& instance();

    /** @return communicator for the HXHIM instance */
    int Comm() const { return instanceComm_; }

    /** @return processes rank in HXHIM instance */
    int Rank() const { return instanceRank_; }

    /** @return processes size in HXHIM instance */
    int Size() const { return instanceSize_; }

    /** @return processes rank in MPI_COMM_WORLD */
    int WorldRank() const { return worldRank_; }

    /** @return processes size in MPI_COMM_WORLD */
    int WorldSize() const { return worldSize_; }

protected:
    /** Construct and initialize an MPI instance */
    MPIInstance();

    /** Disable the copy constructor */
    MPIInstance(const MPIInstance& other);

    /** Destructor called as part of atexit() */
    ~MPIInstance();

private:
    int instanceComm_;
    int instanceRank_;
    int instanceSize_;
    int wasInitializedHere_;
    int worldRank_;
    int worldSize_;
};

class MPIAddress : public CommAddress {
public:
    /** */
    MPIAddress();

    /** */
    MPIAddress(int rank) : CommAddress(), rank_(rank) {}

    /** */
    MPIAddress(const MPIAddress& other) : CommAddress(other), rank_(other.rank_) {}

    /** */
    ~MPIAddress() {}

    /** */
    virtual int Native(void* buf, std::size_t nbytes) const { return -1; }

    /** */
    int Rank() const { return rank_; }

    /** */
    void Rank(const int& rank ) { rank_ = rank; }

private:
    int rank_;
};

class MPIEndpoint : public CommEndpoint {
public:
    /** Create the CommEndpoint for this process */
    MPIEndpoint();

    /** Create a CommEndpoint for a specified process rank */
    MPIEndpoint(int rank);

    /** Destructor */
    ~MPIEndpoint() {}

protected:
    virtual int PutMessageImpl(void* buf, std::size_t nbytes);

    virtual int GetMessageImpl(void* buf, std::size_t& nbytes);

    virtual int ReceiveMessageImpl(void* buf, std::size_t nbytes);

    virtual std::size_t PollForMessageImpl(std::size_t timeoutSecs);

    virtual std::size_t WaitForMessageImpl(std::size_t timeoutSecs);

    virtual int FlushImpl();

private:
    const MPIInstance& mpi_;

    int rank_;
};

class MPIEndpointGroup : public CommEndpointGroup {
public:
    MPIEndpointGroup(int mpiComm);
    ~MPIEndpointGroup();
    int scatterImpl();
    int receiveImpl();
private:
    const MPIInstance& mpi_;
    int rank_;
    int comm_;
};

/**
 * @description An HXHIM communication transport implemented via MPI
 *
 */
typedef CommTransportSpecialization<MPIAddress, MPIEndpoint, MPIEndpointGroup> MPITransport;

#endif //HXHIM_COMM_MPI_H
