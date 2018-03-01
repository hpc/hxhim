//
// Created by bws on 8/24/17.
//

#ifndef HXHIM_COMM_MPI_H
#define HXHIM_COMM_MPI_H

#include <stdexcept>
#include <pthread.h>

#include "comm.h"
#include "mpi.h"
#include "mdhim.h"

/**
 * Singleton helper class that initializes MPI exactly once if it is not initialized
 */
class MPIInstance {
public:
    /** @return the unique instance of MPIInstance */
    static const MPIInstance& instance();

    /** @return communicator for the HXHIM instance */
    MPI_Comm Comm() const { return instanceComm_; }

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
    MPIInstance(const MPIInstance& other) = default;

    /** Destructor called as part of atexit() */
    ~MPIInstance();

private:
    MPI_Comm instanceComm_;
    int instanceRank_;
    int instanceSize_;
    int wasInitializedHere_;
    int worldRank_;
    int worldSize_;
};

/*
 * MPIAddress
 * Class for extracting a MPI rank
*/
class MPIAddress: public CommAddress {
    public:
        MPIAddress(const int rank);

        operator std::string() const;
        operator int() const;

        int Rank() const;

    private:
        int rank_;
};

/*
 * MPIBase
 * Simple base class containing common values for MPI Comm related classes
 */
class MPIBase {
    public:
        MPIBase(MPI_Comm comm, const bool should_clean = false);
        ~MPIBase();
        int Comm() const;
        int Rank() const;
        int Size() const;

        int encode(struct mdhim_basem_t *message, char **buf, int *size);
        int decode(const int mtype, char *buf, int size, struct mdhim_basem_t **message);

    protected:
        MPI_Comm comm_;
        pthread_mutex_t mutex_;

        bool should_clean_; // whether or not this communicator should be freed in the destructor
        int rank_;
        int size_;

    private:
        int pack_put_message(struct mdhim_putm_t *pm, void **sendbuf, int *sendsize);
        int unpack_put_message(void *message, int mesg_size, void **putm);
};

class MPIEndpoint : public CommEndpoint, protected MPIBase {
public:
    MPIEndpoint() = delete;

    /** Create a CommEndpoint for a specified process rank */
    MPIEndpoint(MPI_Comm comm, const bool should_clean = false);

    /** Destructor */
    ~MPIEndpoint() {}

    virtual int AddPutRequest(struct mdhim_putm_t *pm);

    virtual int AddGetRequest(void* kbuf, std::size_t kbytes, void* vbuf, std::size_t vbytes);

    virtual int AddPutReply(const CommAddress *src, void **message);

    virtual int AddGetReply(void* buf, std::size_t nbytes);

    virtual int ReceiveRequest(std::size_t rbytes, CommMessage::Type* requestType, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes);

    virtual std::size_t PollForMessage(std::size_t timeoutSecs);

    virtual std::size_t WaitForMessage(std::size_t timeoutSecs);

    virtual int Flush(); // no-op

    CommEndpoint *Dup() const;

    const CommAddress *Address() const;

private:
    /**
     * @return the number of bytes in the packed buffer
     */
    std::size_t PackRequest(void** pbuf, CommMessage::Type request, void* kbuf, std::size_t kbytes, void* vbuf, std::size_t vbytes);

    void UnpackRequest(void* pbuf, std::size_t pbytes, CommMessage::Type* request, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes);

    const MPIAddress address_;

    void Flush(MPI_Request *req);
};

class MPIEndpointGroup : public CommEndpointGroup, protected MPIBase {
public:
    MPIEndpointGroup(MPI_Comm comm, const bool should_clean = false)
      : CommEndpointGroup(),
        MPIBase(comm, should_clean),
        address_(rank_)
    {}

    ~MPIEndpointGroup() {};

    int BulkPutMessage() { return -1; };
    int BulkGetMessage() { return -1; };

    CommEndpointGroup *Dup() const;

    const CommAddress *Address() const;

private:
    const MPIAddress address_;
};

#endif //HXHIM_COMM_MPI_H
