//
// Created by bws on 8/30/17.
//

#include "comm_mpi.h"

#include <cassert>
#include <iostream>
#include <sstream>
#include <unistd.h>

#define HXHIM_MPI_REQUEST_TAG 0x311

/** Singleton accessor */
const MPIInstance& MPIInstance::instance() {
    static MPIInstance instance;
    return instance;
}

/** Constructor */
MPIInstance::MPIInstance() {
    int mpiIsInitialized = 0;

    // If mpi is not initialized, try to initialize it
    int rc = MPI_Initialized(&mpiIsInitialized);
    if (rc == 0 && !mpiIsInitialized) {
        // Retrieve the command line arguments (not portable)
        int argc = 0;
        char **argv = nullptr;
        //GetCommandLineArguments(&argc, &argv);

        // Perform initialization
        int provided = 0;
        rc = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
        if (rc == 0 && provided >= MPI_THREAD_MULTIPLE) {
            wasInitializedHere_ = 1;
            mpiIsInitialized = 1;
        }
        else {
            std::cerr << __FILE__ << ":" << __LINE__
                 << ":MPI Initialization failed:" << rc << "-" << provided << std::endl;
        }
    }

    // If initialization succeeded, finish retrieving runtime info
    if (mpiIsInitialized) {
        // Retrieve world rank and size
        worldRank_ = worldSize_ = -1;
        rc += MPI_Comm_rank(MPI_COMM_WORLD, &worldRank_);
        rc += MPI_Comm_size(MPI_COMM_WORLD, &worldSize_);
        if (rc != 0)
            std::cerr << __FILE__ << ":" << __LINE__ << ":Failed getting rank and size" << std::endl;

        // Create a communicator so that all address spaces join it
        instanceComm_ = MPI_COMM_WORLD;
        instanceRank_ = worldRank_;
        instanceSize_ = worldSize_;
    }
}

/** Destructor */
MPIInstance::~MPIInstance() {
    if (wasInitializedHere_)
        MPI_Finalize();
}


MPIAddress::MPIAddress(const int rank)
  : rank_(rank)
{}

MPIAddress::operator std::string() const {
    std::stringstream s;
    s << rank_;
    return s.str();
}

MPIAddress::operator int() const {
    return rank_;
}

int MPIAddress::Rank() const {
    return rank_;
}

MPIBase::MPIBase(MPI_Comm comm, const bool should_clean)
  : comm_(comm), mutex_(PTHREAD_MUTEX_INITIALIZER), should_clean_(should_clean)
{
  if (comm == MPI_COMM_NULL) {
      throw std::runtime_error("Received MPI_COMM_NULL as communicator");
  }

  if (MPI_Comm_rank(comm_, &rank_) != MPI_SUCCESS) {
      throw std::runtime_error("Failed to get rank within MPI communicator");
  }

  if (MPI_Comm_size(comm_, &size_) != MPI_SUCCESS) {
      throw std::runtime_error("Failed to get the size of the MPI communicator");
  }
}

MPIBase::~MPIBase() {
    if (should_clean_) {
        MPI_Comm_free(&comm_);
    }
}

int MPIBase::Comm() const {
    return comm_;
}

int MPIBase::Rank() const {
    return rank_;
}

int MPIBase::Size() const {
    return size_;
}

int MPIBase::encode(struct mdhim_basem_t *message, char **buf, int *size) {
    if (!message || !buf || !size) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    switch (message->mtype) {
    case MDHIM_PUT:
      ret = pack_put_message((struct mdhim_putm_t *)message, (void **)buf, size);
      break;
    }

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: Packing message failed.", rank_);
    }

    return ret;
}

int MPIBase::decode(const int mtype, char *buf, int size, struct mdhim_basem_t **message) {
    if (size==0 || mtype<MDHIM_PUT || mtype>MDHIM_COMMIT) {
      mlog(MDHIM_SERVER_CRIT, "Rank %d - Got empty/invalid message in receive_rangesrv_work.", rank_);
      return MDHIM_ERROR;
    }

    int return_code = MPI_SUCCESS;
    int ret = MDHIM_ERROR;

    switch(mtype) {
    case MDHIM_PUT:
        return_code = unpack_put_message((void *)buf, size, (void **)message);
        break;
    }

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error unpacking message in receive_rangesrv_work",
             rank_);
        ret = MDHIM_ERROR;
    }

    return ret;
}

///------------------------

/**
 * pack_put_message
 * Packs a put message structure into contiguous memory for message passing
 *
 * @param pm       in   structure put_message which will be packed into the sendbuf
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer to size of sendbuf
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_putm_t {
       int mtype;
       void *key;
       int key_len;
       void *data;
       int data_len;
       int server_rank;
   };
*/
int MPIBase::pack_put_message(struct mdhim_putm_t *pm, void **sendbuf, int *sendsize) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_putm_t); // Generous variable for size calculation
    int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;  // Variable for incremental pack
    void *outbuf;

    // Add to size the length of the key and data fields
    m_size += pm->key_len + pm->value_len;
    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: put message too large."
             " Put is over Maximum size allowed of %d.", rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }

    //Set output variable for the size to send
    mesg_size = (int) m_size;
    *sendsize = mesg_size;
    pm->basem.size = mesg_size;

        // Is the computed message size of a safe value? (less than a max message size?)
    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == NULL) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack put message.", rank_);
        return MDHIM_ERROR;
    }

    outbuf = *sendbuf;
    // pack the message first with the structure and then followed by key and data values.
    return_code = MPI_Pack(pm, sizeof(struct mdhim_putm_t), MPI_CHAR, outbuf, mesg_size,
                           &mesg_idx, comm_);
    return_code += MPI_Pack(pm->key, pm->key_len, MPI_CHAR, outbuf, mesg_size, &mesg_idx,
                            comm_);
    return_code += MPI_Pack(pm->value, pm->value_len, MPI_CHAR, outbuf, mesg_size, &mesg_idx,
                            comm_);

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
                     "the put message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_put_message
 * Unpacks a put message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param putm       out  put message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_putm_t {
       int mtype;
       void *key;
       int key_len;
       void *data;
       int data_len;
       int server_rank;
   };
*/
int MPIBase::unpack_put_message(void *message, int mesg_size, void **putm) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    struct mdhim_putm_t *pm;

    if ((*((struct mdhim_putm_t **) putm) = (mdhim_putm_t*)malloc(sizeof(struct mdhim_putm_t))) == NULL) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack put message.", rank_);
        return MDHIM_ERROR;
    }

    pm = *((struct mdhim_putm_t **) putm);
    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, pm,
                             sizeof(struct mdhim_putm_t), MPI_CHAR,
                             comm_);

    // Unpack key by first allocating memory and then extracting the values from message
    if ((pm->key = malloc(pm->key_len * sizeof(char))) == NULL) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack put message.", rank_);
        return MDHIM_ERROR;
    }
    return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->key, pm->key_len,
                              MPI_CHAR, comm_);

    // Unpack data by first allocating memory and then extracting the values from message
    if ((pm->value = malloc(pm->value_len * sizeof(char))) == NULL) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack put message.", rank_);
        return MDHIM_ERROR;
    }
    return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->value, pm->value_len,
                              MPI_CHAR, comm_);

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the put message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/** Constructor */
MPIEndpoint::MPIEndpoint(MPI_Comm comm, const bool should_clean)
  : CommEndpoint(),
    MPIBase(comm, should_clean),
    address_(rank_)
{}

int MPIEndpoint::AddPutRequest(struct mdhim_putm_t *pm) {
    char *buf = NULL;
    int size = 0;
    int ret = MDHIM_ERROR;
    int return_code = MPI_SUCCESS;
    MPI_Request req;

    if ((ret = encode((struct mdhim_basem_t *)pm, &buf, &size)) == MDHIM_SUCCESS) {
        // send message size
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Isend(&size, 1, MPI_INT, pm->basem.server_rank, RANGESRV_WORK_SIZE_MSG,comm_, &req);
        pthread_mutex_unlock(&mutex_);
        Flush(&req);

        if (return_code != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error sending work size message in MPIEndpoint::AddPutRequest",
                 rank_);
            free(buf);
            return MDHIM_ERROR;
        }

        // send message
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Isend(buf, size, MPI_PACKED, pm->basem.server_rank, RANGESRV_WORK_MSG, comm_, &req);
        pthread_mutex_unlock(&mutex_);
        Flush(&req);

        if (return_code != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error sending work message in MPIEndpoint::AddPutRequest",
                 rank_);
            free(buf);
            return MDHIM_ERROR;
        }
    }
    else {
        mlog(MPI_CRIT, "Rank %d - "
             "Error encoding message in MPIEndpoint::AddPutRequest",
             rank_);
    }

    free(buf);

    return ret;
}

int MPIEndpoint::AddGetRequest(void *kbuf, std::size_t kbytes, void *vbuf, std::size_t vbytes) {
    return -1;
}

int MPIEndpoint::AddPutReply(const CommAddress *src, void **message) {
    int recvsize;
    MPI_Request req;

    // receive message size
    pthread_mutex_lock(&mutex_);
    if (MPI_Irecv(&recvsize, 1, MPI_INT, ((MPIAddress*) src)->Rank(), CLIENT_RESPONSE_SIZE_MSG, comm_, &req) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // receive message
    char *recvbuf = (char *)calloc(recvsize, sizeof(char));

    pthread_mutex_lock(&mutex_);
    if (MPI_Irecv(recvbuf, recvsize, MPI_PACKED, ((MPIAddress*) src)->Rank(), CLIENT_RESPONSE_MSG, comm_, &req) != MPI_SUCCESS) {
        free(recvbuf);
        return MDHIM_ERROR;
    }
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    struct mdhim_basem_t bm;
    int mesg_idx = 0;
    int ret = MPI_Unpack(recvbuf, recvsize, &mesg_idx, &bm,
                     sizeof(struct mdhim_basem_t), MPI_CHAR,
                     comm_);

    ret = decode(bm.mtype, recvbuf, bm.size, (mdhim_basem_t **)message);

    free(recvbuf);
    return ret;
}

int MPIEndpoint::AddGetReply(void *buf, std::size_t bytes) {
    return -1;
}

int MPIEndpoint::ReceiveRequest(size_t rbytes, CommMessage::Type* requestType, void** kbuf, std::size_t* kbytes, void** vbuf, std::size_t* vbytes) {
    MPI_Status status = {};
    void* rbuf = malloc(rbytes);
    int rc = MPI_Recv(rbuf, (int)rbytes, MPI_PACKED, MPI_ANY_SOURCE, HXHIM_MPI_REQUEST_TAG, comm_, &status);
    UnpackRequest(rbuf, rbytes, requestType, kbuf, kbytes, vbuf, vbytes);
    return rc;
}

size_t MPIEndpoint::PollForMessage(size_t timeoutSecs) {
    std::size_t nbytes = 0;

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

size_t MPIEndpoint::WaitForMessage(size_t timeoutSecs) {
    return 0;
}

int MPIEndpoint::Flush() {
    return 0;
}

size_t MPIEndpoint::PackRequest(void** pbuf, CommMessage::Type request, void* kbuf, size_t kbytes, void* vbuf, size_t vbytes) {
    size_t pbytes = sizeof(request) + sizeof(kbytes) + kbytes + sizeof(vbytes) + vbytes;
    *pbuf = malloc(pbytes);
    int ppos = 0;
    int rc = 0;
    rc += MPI_Pack(&request, 1, MPI_BYTE, *pbuf, (int)pbytes, &ppos, comm_);
    rc += MPI_Pack(&kbytes, 1, MPI_UNSIGNED_LONG, *pbuf, (int)pbytes, &ppos, comm_);
    rc += MPI_Pack(&vbytes, 1, MPI_UNSIGNED_LONG, *pbuf, (int)pbytes, &ppos, comm_);
    rc += MPI_Pack(kbuf, (int)kbytes, MPI_BYTE, *pbuf, (int)pbytes, &ppos, comm_);
    rc += MPI_Pack(vbuf, (int)vbytes, MPI_BYTE, *pbuf, (int)pbytes, &ppos, comm_);
    if (rc != 0) {
        std::cerr << __FILE__ << ":" << __LINE__ << "Message packing failed" << std::endl;
    }
    return (size_t)ppos;
}

void MPIEndpoint::UnpackRequest(void* buf, size_t bytes, CommMessage::Type* request, void** kbuf, size_t* kbytes, void** vbuf, size_t* vbytes) {
    int rc = 0;
    int bpos = 0;
    rc += MPI_Unpack(buf, (int)bytes, &bpos, request, 1, MPI_BYTE, comm_);
    rc += MPI_Unpack(buf, (int)bytes, &bpos, kbytes, 1, MPI_UNSIGNED_LONG, comm_);
    rc += MPI_Unpack(buf, (int)bytes, &bpos, vbytes, 1, MPI_UNSIGNED_LONG, comm_);
    rc += MPI_Unpack(buf, (int)bytes, &bpos, *kbuf, (int)*kbytes, MPI_BYTE, comm_);
    rc += MPI_Unpack(buf, (int)bytes, &bpos, *vbuf, (int)*vbytes, MPI_BYTE, comm_);
    if (rc != 0) {
        std::cerr << __FILE__ << ":" << __LINE__ << "Message unpacking failed" << std::endl;
    }
}

CommEndpoint *MPIEndpoint::Dup() const {
    MPI_Comm newcomm;
    if (MPI_Comm_dup(comm_, &newcomm) != MPI_SUCCESS) {
       throw std::runtime_error("Failed to duplicate MPI_Comm");
    }
    return new MPIEndpoint(newcomm, true);
}

const CommAddress *MPIEndpoint::Address() const {
    return &address_;
}

CommEndpointGroup *MPIEndpointGroup::Dup() const {
    MPI_Comm newcomm;
    if (MPI_Comm_dup(comm_, &newcomm) != MPI_SUCCESS) {
       throw std::runtime_error("Failed to duplicate MPI_Comm");
    }
    return new MPIEndpointGroup(newcomm, true);
}

const CommAddress *MPIEndpointGroup::Address() const {
    return &address_;
}

void MPIEndpoint::Flush(MPI_Request *req) {
    if (!req) {
        return;
    }

    int flag = 0;
    MPI_Status status;

    while (!flag) {
        pthread_mutex_lock(&mutex_);
        MPI_Test(req, &flag, &status);
        pthread_mutex_unlock(&mutex_);

        if (!flag) {
            usleep(100);
        }
    }
}
