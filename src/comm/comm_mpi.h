//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_COMM_MPI_H
#define HXHIM_COMM_MPI_H

#include "comm.h"
#include "mpi.h"

class MPIInstance {
public:
    static MPIInstance* Snstance();
protected:
    MPIInstance();
    ~MPIInstance();

};

class MPIAddress : public CommAddress {
public:
    MPIAddress(int rank) : CommAddress(), rank_(rank) {}
    MPIAddress(const MPIAddress& other) : CommAddress(other), rank_(other.rank_) {}
    ~MPIAddress() {}
    virtual int Native(void* buf, std::size_t nbytes) const { return 0; }
    int Rank() const { return rank_; }
    void Rank(const int& rank ) { rank_ = rank; }

private:
    int rank_;
};

class MPIEndpoint : public CommEndpoint {
public:
    MPIEndpoint() : CommEndpoint() {}
    ~MPIEndpoint() {}

protected:
    virtual int PutMessageImpl(const CommEndpoint& dest);

    virtual int GetMessageImpl(const CommEndpoint& src);

    virtual int PollForMessageImpl(std::size_t timeoutSecs);

    virtual int WaitForMessageImpl(std::size_t timeoutSecs);

    int rank_;
};

class MPIEndpointGroup : public CommEndpointGroup {
public:
    MPIEndpointGroup(int mpiComm);
    ~MPIEndpointGroup();
    int scatterImpl();
    int receiveImpl();
private:
    int comm_;
};

/**
 * @description An HXHIM communication transport implemented via MPI
 *
 */
typedef CommTransportSpecialization<MPIAddress, MPIEndpoint, MPIEndpointGroup> MPITransport;

#endif //HXHIM_COMM_MPI_H
