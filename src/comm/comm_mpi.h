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
 * Base class containing common values and functions for MPI Comm related classes
 */
class MPIBase {
    public:
        MPIBase(MPI_Comm comm, const bool should_free_comm = false);
        ~MPIBase();
        int Comm() const;
        int Rank() const;
        int Size() const;

    protected:
        /** wrappers around private pack and unpack functions */
        int encode(struct mdhim_basem_t *message, char **buf, int *size);
        int decode(const int mtype, char *buf, int size, struct mdhim_basem_t **message);

        /** functions that actually call MPI */
        int send_rangesrv_work(int dest, const char *buf, const int size);
        int send_all_rangesrv_work(char **messages, int *sizes, int num_srvs);
        int receive_rangesrv_work(int *src, char **recvbuf, int *recvsize);
        int send_client_response(int dest, int *sizebuf, char **sendbuf, MPI_Request **size_req, MPI_Request **msg_req);
        int receive_client_response(int src, char **recvbuf, int *recvsize);
        int receive_all_client_responses(int *srcs, int nsrcs, char ***recvbufs, int **sizebuf);

        void Flush(MPI_Request *req);

    protected:
        MPI_Comm comm_;
        pthread_mutex_t mutex_;

        bool should_free_comm_; // whether or not this communicator should be freed in the destructor
        int rank_;
        int size_;

    private:
        /** encoding and decoding functions, accessed through encode and decode
            TODO: Move these functions into MPIMessage(s)
         */
        int pack_put_message(struct mdhim_putm_t *pm, void **sendbuf, int *sendsize);
        int pack_bput_message(struct mdhim_bputm_t *bpm, void **sendbuf, int *sendsize);
        int unpack_put_message(void *message, int mesg_size, void **pm);
        int unpack_bput_message(void *message, int mesg_size, void **bpm);

        int pack_get_message(struct mdhim_getm_t *gm, void **sendbuf, int *sendsize);
        int pack_bget_message(struct mdhim_bgetm_t *bgm, void **sendbuf, int *sendsize);
        int unpack_get_message(void *message, int mesg_size, void **gm);
        int unpack_bget_message(void *message, int mesg_size, void **bgm);

        int pack_bgetrm_message(struct mdhim_bgetrm_t *bgrm, void **sendbuf, int *sendsize);
        int unpack_bgetrm_message(void *message, int mesg_size, void **bgrm);

        int pack_del_message(struct mdhim_delm_t *dm, void **sendbuf, int *sendsize);
        int pack_bdel_message(struct mdhim_bdelm_t *bdm, void **sendbuf, int *sendsize);
        int unpack_del_message(void *message, int mesg_size, void **dm);
        int unpack_bdel_message(void *message, int mesg_size, void **bdm);

        int pack_return_message(struct mdhim_rm_t *rm, void **sendbuf, int *sendsize);
        int unpack_return_message(void *message, void **rm);

        int pack_base_message(struct mdhim_basem_t *cm, void **sendbuf, int *sendsize);
};

class MPIEndpoint : public CommEndpoint, protected MPIBase {
    public:
        MPIEndpoint() = delete;

        /** Create a CommEndpoint for a specified process rank */
        MPIEndpoint(MPI_Comm comm, const bool should_clean = false);

        /** Destructor */
        ~MPIEndpoint() {}

        int AddPutRequest(struct mdhim_putm_t *pm);
        int AddGetRequest(struct mdhim_bgetm_t **messages, int num_srvs);

        int AddPutReply(const CommAddress *src, struct mdhim_rm_t **message);
        int AddGetReply(int *srcs, int nsrcs, struct mdhim_bgetrm_t ***messages);

        int ReceiveRequest(std::size_t rbytes, CommMessage::Type* requestType, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes);

        std::size_t PollForMessage(std::size_t timeoutSecs);

        std::size_t WaitForMessage(std::size_t timeoutSecs);

        int Flush(); // no-op

        CommEndpoint *Dup() const;

        const CommAddress *Address() const;

    private:
        /**
         * @return the number of bytes in the packed buffer
         */
        std::size_t PackRequest(void** pbuf, CommMessage::Type request, void* kbuf, std::size_t kbytes, void* vbuf, std::size_t vbytes);

        void UnpackRequest(void* pbuf, std::size_t pbytes, CommMessage::Type* request, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes);

        const MPIAddress address_;
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
