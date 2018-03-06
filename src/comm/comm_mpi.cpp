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

MPIBase::MPIBase(MPI_Comm comm, const bool should_free_comm)
    : comm_(comm), mutex_(PTHREAD_MUTEX_INITIALIZER), should_free_comm_(should_free_comm)
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
    if (should_free_comm_) {
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

/**
 * encode
 * encodes a given mdhim_*_t message into an char buffer
 *
 * @param message pointer to message struct to encode
 * @param buf     pointer to nullptr (will be allocated by one of the pack functions)
 * @param size    pointer size of packed message (set by one of the pack functions)
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::encode(struct mdhim_basem_t *message, char **buf, int *size) {
    int ret = MDHIM_ERROR;

    if (!message || !buf || !size) {
        return ret;
    }

    switch(message->mtype) {
    case MDHIM_PUT:
        ret = pack_put_message   ((struct mdhim_putm_t *)  message, (void **)buf, size);
        break;
    case MDHIM_BULK_PUT:
        ret = pack_bput_message  ((struct mdhim_bputm_t *) message, (void **)buf, size);
        break;
    case MDHIM_BULK_GET:
        ret = pack_bget_message  ((struct mdhim_bgetm_t *) message, (void **)buf, size);
        break;
    case MDHIM_DEL:
        ret = pack_del_message   ((struct mdhim_delm_t *)  message, (void **)buf, size);
        break;
    case MDHIM_BULK_DEL:
        ret = pack_bdel_message  ((struct mdhim_bdelm_t *) message, (void **)buf, size);
        break;
    case MDHIM_COMMIT:
        ret = pack_base_message  ((struct mdhim_basem_t *) message, (void **)buf, size);
        break;
    case MDHIM_CLOSE:
        ret = pack_base_message  ((struct mdhim_basem_t *) message, (void **)buf, size);
        break;
    case MDHIM_RECV:
        ret = pack_return_message((struct mdhim_rm_t *)    message, (void **)buf, size);
        break;
    case MDHIM_RECV_BULK_GET:
        ret = pack_bgetrm_message((struct mdhim_bgetrm_t *)message, (void **)buf, size);
        break;
    default:
        break;
    }

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: Packing message failed.", rank_);
    }

    return ret;
}

/**
 * decode
 * decodes a given char buffer into a mdhim_*_t message
 *
 * @param buf     pointer to packed message
 * @param size    size of packed message
 * @param message double pointer to message struct to decode
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::decode(const int mtype, char *buf, int size, struct mdhim_basem_t **message) {
    // Checks for valid message, if error inform and ignore message
    if (size==0 || mtype<MDHIM_PUT || mtype>MDHIM_COMMIT) {
        mlog(MDHIM_SERVER_CRIT, "Rank %d - Got empty/invalid message in receive_rangesrv_work.", rank_);
        return MDHIM_ERROR;
    }

    int return_code = MDHIM_SUCCESS;
    int ret = MDHIM_SUCCESS;
    switch(mtype) {
    case MDHIM_PUT:
        return_code = unpack_put_message ((void *)buf, size, (void **)message);
        break;
    case MDHIM_BULK_PUT:
        return_code = unpack_bput_message((void *)buf, size, (void **)message);
        break;
    case MDHIM_BULK_GET:
        return_code = unpack_bget_message((void *)buf, size, (void **)message);
        break;
    case MDHIM_DEL:
        return_code = unpack_del_message ((void *)buf, size, (void **)message);
        break;
    case MDHIM_BULK_DEL:
        return_code = unpack_bdel_message((void *)buf, size, (void **)message);
        break;
    case MDHIM_COMMIT:
        ret = MDHIM_COMMIT;
        break;
    case MDHIM_CLOSE:
        ret = MDHIM_CLOSE;
        break;
    case MDHIM_RECV:
        ret = unpack_return_message      ((void *)buf,       (void **)message);
        break;
    case MDHIM_RECV_BULK_GET:
        ret = unpack_bgetrm_message      ((void *)buf, size, (void **)message);
        break;
    default:
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

/**
 * send_rangesrv_work
 * Sends a packed message (char buffer) to the range server at the given destination
 *
 * @param dest    destination to send to
 * @param buf     data to be sent to dest
 * @param size    size of data to be sent to dest
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::send_rangesrv_work(int dest, const char *buf, const int size) {
    int return_code = MDHIM_ERROR;
    MPI_Request req;

    //Send the size of the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(&size, 1, MPI_INT, dest, RANGESRV_WORK_SIZE_MSG,
                            comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending work size message in send_rangesrv_work",
             rank_);
        return MDHIM_ERROR;
    }

    //Send the message
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(buf, size, MPI_PACKED, dest, RANGESRV_WORK_MSG,
                            comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending work message in send_rangesrv_work",
             rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * send_all_rangesrv_work
 * Sends multiple messages (char buffers)  simultaneously and waits for them to all complete
 *
 * @param messages double pointer to array of packed messages to send
 * @param sizes    size of each message
 * @param num_srvs number of different servers
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::send_all_rangesrv_work(char **messages, int *sizes, int num_srvs) {
    int return_code = MDHIM_ERROR;
    MPI_Request **reqs = (MPI_Request**)calloc(num_srvs, sizeof(MPI_Request *)),
                **size_reqs = (MPI_Request**)calloc(num_srvs, sizeof(MPI_Request *));
    int num_msgs = 0;
    int ret = MDHIM_SUCCESS;

    //Send all messages at once
    for (int i = 0; i < num_srvs; i++) {
        char *mesg = *(messages + i);
        if (!mesg) {
            continue;
        }

        int dest = ((struct mdhim_basem_t *) mesg)->server_rank;
        size_reqs[i] = (MPI_Request*)malloc(sizeof(MPI_Request));;

        // send size
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Isend(&sizes[i], 1, MPI_INT, dest, RANGESRV_WORK_SIZE_MSG,
                                comm_, size_reqs[i]);
        pthread_mutex_unlock(&mutex_);

        if (return_code != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error sending work message in send_rangesrv_work",
                 rank_);
            ret = MDHIM_ERROR;
        }

        reqs[i] = (MPI_Request*)malloc(sizeof(MPI_Request));

        // data
        pthread_mutex_lock(&mutex_);
        return_code = MPI_Isend(messages[i], sizes[i], MPI_PACKED, dest, RANGESRV_WORK_MSG,
                                comm_, reqs[i]);
        pthread_mutex_unlock(&mutex_);

        if (return_code != MPI_SUCCESS) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error sending work message in send_rangesrv_work",
                 rank_);
            ret = MDHIM_ERROR;
        }

        num_msgs++;
    }

    //Wait for messages to complete
    const int total_msgs = num_msgs * 2;
    int done;
    while (done != total_msgs) {
        int flag;
        MPI_Status status;

        for (int i = 0; i < num_srvs; i++) {
            if (!size_reqs[i]) {
                continue;
            }

            pthread_mutex_lock(&mutex_);
            ret = MPI_Test(size_reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (flag) {
                free(size_reqs[i]);
                size_reqs[i] = nullptr;
                done++;
            }
        }
        for (int i = 0; i < num_srvs; i++) {
            if (!reqs[i]) {
                continue;
            }

            pthread_mutex_lock(&mutex_);
            ret = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (flag) {
                free(reqs[i]);
                reqs[i] = nullptr;
                done++;
            }
        }

        if (done != total_msgs) {
            usleep(100);
        }
    }

    free(size_reqs);
    free(reqs);

    return ret;
}

/**
 * receive_rangesrv_work message
 * Receives a message (char buf) from the given source
 *
 * @param src      out  pointer to source of message received
 * @param recvbuf  out  double pointer for message received
 * @param recvsize out  pointer for the size of the message received
 * @return MDHIM_SUCCESS, MDHIM_CLOSE, MDHIM_COMMIT, or MDHIM_ERROR on error
 */
int MPIBase::receive_rangesrv_work(int *src, char **recvbuf, int *recvsize) {
    MPI_Status status;
    int return_code;
    MPI_Request req;
    int flag = 0;
    int ret = MDHIM_SUCCESS;

    // Receive a message from any client
    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(recvsize,1, MPI_INT, MPI_ANY_SOURCE, RANGESRV_WORK_SIZE_MSG,
                            comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: %d "
             "receive size message failed.", rank_, return_code);
        return MDHIM_ERROR;
    }

    if (return_code == MPI_ERR_IN_STATUS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
             " while receiving work message size", rank_, status.MPI_ERROR);
    }

    *recvbuf = (char *)malloc(*recvsize);
    memset(*recvbuf, 0, *recvsize);
    flag = 0;

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv((void *)*recvbuf, *recvsize, MPI_PACKED, status.MPI_SOURCE,
                            RANGESRV_WORK_MSG, comm_, &req);
    pthread_mutex_unlock(&mutex_);
    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: %d "
             "receive message failed.", rank_, return_code);
        return MDHIM_ERROR;
    }
    Flush(&req);

    if (return_code == MPI_ERR_IN_STATUS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
             " while receiving work message size", rank_, status.MPI_ERROR);
    }
    if (!*recvbuf) {
        return MDHIM_ERROR;
    }

    *src = status.MPI_SOURCE;

    return ret;
}

/**
 * send_client_response
 * Sends a char buffer to a client
 *
 * @param dest    destination to send to
 * @param sendbuf double pointer to packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::send_client_response(int dest, int *sizebuf,
                                  char **sendbuf, MPI_Request **size_req, MPI_Request **msg_req) {
    int return_code = 0;
    int mtype;
    int ret = MDHIM_SUCCESS;

    //Send the size message
    *size_req = (MPI_Request*)malloc(sizeof(MPI_Request));

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(sizebuf, 1, MPI_INT, dest, CLIENT_RESPONSE_SIZE_MSG,
                            comm_, *size_req);
    pthread_mutex_unlock(&mutex_);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending client response message size in send_client_response",
             rank_);
        ret = MDHIM_ERROR;
        free(*size_req);
        *size_req = nullptr;
    }

    *msg_req = (MPI_Request*)malloc(sizeof(MPI_Request));
    //Send the actual message

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Isend(*sendbuf, *sizebuf, MPI_PACKED, dest, CLIENT_RESPONSE_MSG,
                            comm_, *msg_req);
    pthread_mutex_unlock(&mutex_);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error sending client response message in send_client_response",
             rank_);
        ret = MDHIM_ERROR;
        free(*msg_req);
        *msg_req = nullptr;
    }

    return ret;
}

/**
 * receive_client_response message
 * Receives a message from the given source
 *
 * @param src     in   source to receive from
 * @param message out  double pointer for message received
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::receive_client_response(int src, char **recvbuf, int *recvsize) {
    int return_code;
    MPI_Request req;

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(recvsize, 1, MPI_INT, src, CLIENT_RESPONSE_SIZE_MSG,
                            comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error receiving message in receive_client_response",
             rank_);
        return MDHIM_ERROR;
    }

    *recvbuf = (char *)malloc(*recvsize);
    memset(*recvbuf, 0, *recvsize);

    pthread_mutex_lock(&mutex_);
    return_code = MPI_Irecv(*recvbuf, *recvsize, MPI_PACKED, src, CLIENT_RESPONSE_MSG,
                            comm_, &req);
    pthread_mutex_unlock(&mutex_);
    Flush(&req);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MPI_CRIT, "Rank %d - "
             "Error receiving message in receive_client_response",
             rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * receive_all_client_responses
 * Receives messages from multiple sources sources
 *
 * @param srcs          in  sources to receive from
 * @param nsrcs         in  number of sources to receive from
 * @param messages out  array of messages to receive
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int MPIBase::receive_all_client_responses(int *srcs, int nsrcs, char ***recvbufs, int **sizebuf) {
    MPI_Status status;
    int return_code;
    int mtype;
    int ret = MDHIM_SUCCESS;
    MPI_Request **reqs;
    int done = 0;

    *sizebuf = (int *)calloc(nsrcs, sizeof(int));
    reqs = (MPI_Request **)calloc(nsrcs, sizeof(MPI_Request *));
    *recvbufs = (char **)calloc(nsrcs, sizeof(char *));

    // Receive a size message from the servers in the list
    for (int i = 0; i < nsrcs; i++) {
        reqs[i] = (MPI_Request*)malloc(sizeof(MPI_Request));

        pthread_mutex_lock(&mutex_);
        return_code = MPI_Irecv(&sizebuf[i], 1, MPI_INT,
                                srcs[i], CLIENT_RESPONSE_SIZE_MSG,
                                comm_, reqs[i]);
        pthread_mutex_unlock(&mutex_);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error receiving message in receive_client_response",
                 rank_);
            ret = MDHIM_ERROR;
        }
    }

    // Wait for size messages to complete
    while (done != nsrcs) {
        for (int i = 0; i < nsrcs; i++) {
            if (!reqs[i]) {
                continue;
            }

            int flag = 0;

            pthread_mutex_lock(&mutex_);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (return_code == MPI_ERR_REQUEST) {
                mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
                     " while receiving client response message size",
                     rank_, status.MPI_ERROR);
            }
            if (!flag) {
                continue;
            }
            free(reqs[i]);
            reqs[i] = nullptr;
            done++;
        }

        if (done != nsrcs) {
            usleep(100);
        }
    }

    done = 0;
    for (int i = 0; i < nsrcs; i++) {
        // Receive a message from the servers in the list
        *recvbufs[i] = (char *) malloc(*sizebuf[i]);
        reqs[i] = (MPI_Request*)malloc(sizeof(MPI_Request));

        pthread_mutex_lock(&mutex_);
        return_code = MPI_Irecv(*recvbufs[i], *sizebuf[i], MPI_PACKED,
                                srcs[i], CLIENT_RESPONSE_MSG,
                                comm_, reqs[i]);
        pthread_mutex_unlock(&mutex_);

        // If the receive did not succeed then return the error code back
        if ( return_code != MPI_SUCCESS ) {
            mlog(MPI_CRIT, "Rank %d - "
                 "Error receiving message in receive_client_response",
                 rank_);
            ret = MDHIM_ERROR;
        }
    }

    //Wait for messages to complete
    while (done != nsrcs) {
        for (int i = 0; i < nsrcs; i++) {
            if (!reqs[i]) {
                continue;
            }

            int flag = 0;

            pthread_mutex_lock(&mutex_);
            return_code = MPI_Test(reqs[i], &flag, &status);
            pthread_mutex_unlock(&mutex_);

            if (return_code == MPI_ERR_REQUEST) {
                mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Received an error status: %d "
                     " while receiving work message size", rank_, status.MPI_ERROR);
            }
            if (!flag) {
                continue;
            }
            free(reqs[i]);
            reqs[i] = nullptr;
            done++;
        }

        if (done != nsrcs) {
            usleep(100);
        }
    }

    free(reqs);

    return ret;
}

void MPIBase::Flush(MPI_Request *req) {
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

///------------------------

/**
 * pack_put_message
 * Packs a put message structure into contiguous memory for message passing
 *
 * @param md       in   main MDHIM struct
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
             " Put is over Maximum size allowed of %d.",rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }

    //Set output variable for the size to send
    mesg_size = (int) m_size;
    *sendsize = mesg_size;
    pm->basem.size = mesg_size;

    // Is the computed message size of a safe value? (less than a max message size?)
    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
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
 * pack_bput_message
 * Packs a bulk put message structure into contiguous memory for message passing
 *
 * @param md        in   main MDHIM struct
 * @param bpm       in   structure bput_message which will be packed into the sendbuf
 * @param sendbuf   out  double pointer for packed message to send
 * @param sendsize  out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bputm_t {
 int mtype;
 void **keys;
 int *key_lens;
 void **data;
 int *data_lens;
 int num_records;
 int server_rank;
 };
*/
int MPIBase::pack_bput_message(struct mdhim_bputm_t *bpm, void **sendbuf, int *sendsize) {
    int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_bputm_t);  // Generous variable for size calc
    int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;
    int i;

    // Add the sizes of the length arrays (key_lens and data_lens)
    m_size += 2 * bpm->num_keys * sizeof(int);

    // For the each of the keys and data add enough chars.
    for (i=0; i < bpm->num_keys; i++)
        m_size += bpm->key_lens[i] + bpm->value_lens[i];

    // Is the computed message size of a safe value? (less than a max message size?)
    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: bulk put message too large."
             " Bput is over Maximum size allowed of %d.", rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }
    mesg_size = m_size;  // Safe size to use in MPI_pack
    *sendsize = mesg_size;
    bpm->basem.size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack bulk put message.", rank_);
        return MDHIM_ERROR;
    }

    // pack the message first with the structure and then followed by key and data values (plus lengths).
    return_code = MPI_Pack(bpm, sizeof(struct mdhim_bputm_t), MPI_CHAR, *sendbuf,
                           mesg_size, &mesg_idx, comm_);

    // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
    for (i=0; i < bpm->num_keys; i++) {
        return_code += MPI_Pack(&bpm->key_lens[i], 1, MPI_INT,
                                *sendbuf, mesg_size, &mesg_idx, comm_);
        return_code += MPI_Pack(bpm->keys[i], bpm->key_lens[i], MPI_CHAR,
                                *sendbuf, mesg_size, &mesg_idx, comm_);
        return_code += MPI_Pack(&bpm->value_lens[i], 1, MPI_INT,
                                *sendbuf, mesg_size, &mesg_idx, comm_);
        return_code += MPI_Pack(bpm->values[i], bpm->value_lens[i], MPI_CHAR,
                                *sendbuf, mesg_size, &mesg_idx, comm_);
    }

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the bulk put message.", rank_);
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
int MPIBase::unpack_put_message(void *message, int mesg_size,  void **putm) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    struct mdhim_putm_t *pm;

    if ((*((struct mdhim_putm_t **) putm) = (mdhim_putm_t*)malloc(sizeof(struct mdhim_putm_t))) == nullptr) {
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
    if ((pm->key = malloc(pm->key_len * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack put message.", rank_);
        return MDHIM_ERROR;
    }
    return_code += MPI_Unpack(message, mesg_size, &mesg_idx, pm->key, pm->key_len,
                              MPI_CHAR, comm_);

    // Unpack data by first allocating memory and then extracting the values from message
    if ((pm->value = malloc(pm->value_len * sizeof(char))) == nullptr) {
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

/**
 * unpack_bput_message
 * Unpacks a bulk put message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param bput       out  bulk put message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bputm_t {
 int mtype;
 void **keys;
 int *key_lens;
 void **data;
 int *data_lens;
 int num_records;
 int server_rank;
 };
*/
int MPIBase::unpack_bput_message(void *message, int mesg_size, void **bput) {

    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    int i;
    int num_records;

    if ((*((struct mdhim_bputm_t **) bput) = (struct mdhim_bputm_t *)malloc(sizeof(struct mdhim_bputm_t))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bput message.", rank_);
        return MDHIM_ERROR;
    }

    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, *((struct mdhim_bputm_t **) bput),
                             sizeof(struct mdhim_bputm_t),
                             MPI_CHAR, comm_);

    num_records = (*((struct mdhim_bputm_t **) bput))->num_keys;
    // Allocate memory for key pointers, to be populated later.
    if (((*((struct mdhim_bputm_t **) bput))->keys =
         (void**)malloc(num_records * sizeof(void *))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bput message.", rank_);
        return MDHIM_ERROR;
    }

    // Allocate memory for value pointers, to be populated later.
    if (((*((struct mdhim_bputm_t **) bput))->values =
         (void**)malloc(num_records * sizeof(void *))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bput message.", rank_);
        return MDHIM_ERROR;
    }

    // Allocate memory for key_lens, to be populated later.
    if (((*((struct mdhim_bputm_t **) bput))->key_lens =
         (int *)malloc(num_records * sizeof(int))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bput message.", rank_);
        return MDHIM_ERROR;
    }

    // Allocate memory for value_lens, to be populated later.
    if (((*((struct mdhim_bputm_t **) bput))->value_lens =
         (int *)malloc(num_records * sizeof(int))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bput message.", rank_);
        return MDHIM_ERROR;
    }

    // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
    for (i=0; i < num_records; i++) {
        // Unpack the key_lens[i]
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  &(*((struct mdhim_bputm_t **) bput))->key_lens[i], 1, MPI_INT,
                                  comm_);

        // Unpack key by first allocating memory and then extracting the values from message
        if (((*((struct mdhim_bputm_t **) bput))->keys[i] =
             malloc((*((struct mdhim_bputm_t **) bput))->key_lens[i] *
                    sizeof(char))) == nullptr) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
                 "memory to unpack bput message.", rank_);
            return MDHIM_ERROR;
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  (*((struct mdhim_bputm_t **) bput))->keys[i],
                                  (*((struct mdhim_bputm_t **) bput))->key_lens[i],
                                  MPI_CHAR, comm_);

        // Unpack the data_lens[i]
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  &(*((struct mdhim_bputm_t **) bput))->value_lens[i], 1,
                                  MPI_INT,
                                  comm_);

        // Unpack data by first allocating memory and then extracting the values from message
        if (((*((struct mdhim_bputm_t **) bput))->values[i] =
             malloc((*((struct mdhim_bputm_t **) bput))->value_lens[i] *
                    sizeof(char))) == nullptr) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
                 "memory to unpack bput message.", rank_);
            return MDHIM_ERROR;
        }

        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  (*((struct mdhim_bputm_t **) bput))->values[i],
                                  (*((struct mdhim_bputm_t **) bput))->value_lens[i],
                                  MPI_CHAR, comm_);
    }

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the bput message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_get_message
 * Packs a get message structure into contiguous memory for message passing
 *
 * @param gm        in   structure get_message which will be packed into the sendbuf
 * @param sendbuf   out  double pointer for packed message to send
 * @param sendsize  out  pointer to sendbuf's size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_getm_t {
 int mtype;
 int op;
 void *key;
 int key_len;
 int server_rank;
 };
*/
int MPIBase::pack_get_message(struct mdhim_getm_t *gm, void **sendbuf, int *sendsize) {

    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_getm_t); // Generous variable for size calculation
    int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;  // Variable for incremental pack
    void *outbuf;

    // Add to size the length of the key and data fields
    m_size += gm->key_len;

    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: get message too large."
             " Get is over Maximum size allowed of %d.", rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }
    mesg_size = m_size;
    *sendsize = mesg_size;
    gm->basem.size = mesg_size;

    // Is the computed message size of a safe value? (less than a max message size?)
    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack get message.", rank_);
        return MDHIM_ERROR;
    }

    outbuf = *sendbuf;
    // pack the message first with the structure and then followed by key and data values.
    return_code = MPI_Pack(gm, sizeof(struct mdhim_getm_t), MPI_CHAR, outbuf, mesg_size,
                           &mesg_idx, comm_);
    return_code += MPI_Pack(gm->key, gm->key_len, MPI_CHAR, outbuf, mesg_size,
                            &mesg_idx, comm_);

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the get message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * pack_bget_message
 * Packs a bget message structure into contiguous memory for message passing
 *
 * @param bgm      in   structure bget_message which will be packed into the sendbuf
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for sendbuf size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bgetm_t {
 int mtype;
 int op;
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int MPIBase::pack_bget_message(struct mdhim_bgetm_t *bgm, void **sendbuf, int *sendsize) {
    int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_bgetm_t);  // Generous variable for size calc
    int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;

    // Calculate the size of the message to send
    m_size += bgm->num_keys * sizeof(int) * 2;

    // For the each of the keys add the size to the length
    for (int i=0; i < bgm->num_keys; i++)
        m_size += bgm->key_lens[i];

    // Is the computed message size of a safe value? (less than a max message size?)
    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: bulk get message too large."
             " Bget is over Maximum size allowed of %d.", rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }
    mesg_size = m_size;  // Safe size to use in MPI_pack
    *sendsize = mesg_size;
    bgm->basem.size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack bulk get message.", rank_);
        return MDHIM_ERROR;
    }

    // pack the message first with the structure and then followed by key and
    // data values (plus lengths).
    return_code = MPI_Pack(bgm, sizeof(struct mdhim_bgetm_t), MPI_CHAR,
                           *sendbuf, mesg_size,
                           &mesg_idx, comm_);

    // For the each of the keys and data pack the chars plus one int for key_len.
    for (int i=0; i < bgm->num_keys; i++) {
        return_code += MPI_Pack(&bgm->key_lens[i], 1, MPI_INT,
                                *sendbuf, mesg_size,
                                &mesg_idx, comm_);
        return_code += MPI_Pack(bgm->keys[i], bgm->key_lens[i], MPI_CHAR,
                                *sendbuf, mesg_size,
                                &mesg_idx, comm_);
    }

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the bulk get message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_get_message
 * Unpacks a get message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param getm       out  get message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_getm_t {
 int mtype;
 //Operation type e.g., MDHIM_GET_VAL, MDHIM_GET_NEXT, MDHIM_GET_PREV
 int op;
 void *key;
 int key_len;
 int server_rank;
 };
*/
int MPIBase::unpack_get_message(void *message, int mesg_size, void **getm) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack

    if ((*((struct mdhim_getm_t **) getm) = (struct mdhim_getm_t *)malloc(sizeof(struct mdhim_getm_t))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack get message.", rank_);
        return MDHIM_ERROR;
    }

    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, *((struct mdhim_getm_t **) getm),
                             sizeof(struct mdhim_getm_t), MPI_CHAR, comm_);

    // Unpack key by first allocating memory and then extracting the values from message
    if (((*((struct mdhim_getm_t **) getm))->key =
         malloc((*((struct mdhim_getm_t **) getm))->key_len * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack get message.", rank_);
        return MDHIM_ERROR;
    }
    return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                              (*((struct mdhim_getm_t **) getm))->key,
                              (*((struct mdhim_getm_t **) getm))->key_len,
                              MPI_CHAR, comm_);

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the get message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_bget_message
 * Unpacks a bulk get message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message we received
 * @param mesg_size  in   size of the incoming message
 * @param bgetm      out  bulk get message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bgetm_t {
 int mtype;
 int op;
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int MPIBase::unpack_bget_message(void *message, int mesg_size, void **bgetm) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    int i;
    int num_records;

    if ((*((struct mdhim_bgetm_t **) bgetm) = (struct mdhim_bgetm_t *)malloc(sizeof(struct mdhim_bgetm_t))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bget message.", rank_);
        return MDHIM_ERROR;
    }

    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, *((struct mdhim_bgetm_t **) bgetm),
                             sizeof(struct mdhim_bgetm_t),
                             MPI_CHAR, comm_);

    num_records = (*((struct mdhim_bgetm_t **) bgetm))->num_keys;
    // Allocate memory for key pointers, to be populated later.
    if (((*((struct mdhim_bgetm_t **) bgetm))->keys =
         (void**)malloc(num_records * sizeof(void *))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bget message.", rank_);
        return MDHIM_ERROR;
    }
    // Allocate memory for key_lens, to be populated later.
    if (((*((struct mdhim_bgetm_t **) bgetm))->key_lens =
         (int *)malloc(num_records * sizeof(int))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bget message.", rank_);
        return MDHIM_ERROR;
    }

    memset((*((struct mdhim_bgetm_t **) bgetm))->key_lens, 0, num_records * sizeof(int));
    // For the each of the keys and data unpack the chars plus an int for key_lens[i].
    for (i=0; i < num_records; i++) {
        // Unpack the key_lens[i]
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  &(*((struct mdhim_bgetm_t **) bgetm))->key_lens[i],
                                  1, MPI_INT, comm_);
        // Unpack key by first allocating memory and then extracting the values from message
        if (((*((struct mdhim_bgetm_t **) bgetm))->keys[i] =
             (struct mdhim_bgetm_t *)malloc((*((struct mdhim_bgetm_t **) bgetm))->key_lens[i] *
                                            sizeof(char))) == nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
                 "memory to unpack bget message.", rank_);
            return MDHIM_ERROR;
        }

        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  (*((struct mdhim_bgetm_t **) bgetm))->keys[i],
                                  (*((struct mdhim_bgetm_t **) bgetm))->key_lens[i],
                                  MPI_CHAR, comm_);
    }

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the bget message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * pack_bgetrm_message
 * Packs a bulk get return message structure into contiguous memory for message passing
 *
 * @param bgrm     in   structure bget_return_message which will be packed into the message
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer to sendbuf's size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bgetrm_t {
 int mtype;
 int error;
 void **keys;
 int *key_lens;
 void **values;
 int *value_lens;
 int num_records;
 };
*/
int MPIBase::pack_bgetrm_message(struct mdhim_bgetrm_t *bgrm, void **sendbuf, int *sendsize) {
    int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_bgetrm_t);  // Generous variable for size calc
    int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;
    int i;
    void *outbuf;

    // For each of the lens (key_lens and data_lens)
    // WARNING We are treating ints as the same size as char for packing purposes
    m_size += 2 * bgrm->num_keys * sizeof(int);

    // For the each of the keys and data add enough chars.
    for (i=0; i < bgrm->num_keys; i++)
        m_size += bgrm->key_lens[i] + bgrm->value_lens[i];

    // Is the computed message size of a safe value? (less than a max message size?)
    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: bulk get return message too large."
             " Bget return message is over Maximum size allowed of %d.", rank_,
             MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }
    mesg_size = m_size;  // Safe size to use in MPI_pack
    *sendsize = mesg_size;
    bgrm->basem.size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack bulk get return message.", rank_);
        return MDHIM_ERROR;
    }

    outbuf = *sendbuf;
    // pack the message first with the structure and then followed by key and data values (plus lengths).
    return_code = MPI_Pack(bgrm, sizeof(struct mdhim_bgetrm_t), MPI_CHAR, outbuf, mesg_size,
                           &mesg_idx, comm_);

    // For the each of the keys and data pack the chars plus two ints for key_len and data_len.
    for (i=0; i < bgrm->num_keys; i++) {
        return_code += MPI_Pack(&bgrm->key_lens[i], 1, MPI_INT, outbuf, mesg_size,
                                &mesg_idx, comm_);
        if (bgrm->key_lens[i] > 0) {
            return_code += MPI_Pack(bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, outbuf,
                                    mesg_size, &mesg_idx, comm_);
        }

        return_code += MPI_Pack(&bgrm->value_lens[i], 1, MPI_INT, outbuf, mesg_size,
                                &mesg_idx, comm_);
        /* Pack the value retrieved from the db
           There is a chance that the key didn't exist in the db */
        if (bgrm->value_lens[i] > 0) {
            return_code += MPI_Pack(bgrm->values[i], bgrm->value_lens[i],
                                    MPI_CHAR, outbuf, mesg_size,
                                    &mesg_idx, comm_);
        }
    }

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the bulk get return message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_bgetrm_message
 * Unpacks a bulk get return message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param bgetrm     out  bulk get return message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bgetrm_t {
 int mtype;
 int error;
 void **keys;
 int *key_lens;
 void **values;
 int *value_lens;
 int num_records;
 };
*/
int MPIBase::unpack_bgetrm_message(void *message, int mesg_size, void **bgetrm) {

    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    int i;
    struct mdhim_bgetrm_t *bgrm;

    if ((*((struct mdhim_bgetrm_t **) bgetrm) = (struct mdhim_bgetrm_t *)malloc(sizeof(struct mdhim_bgetrm_t))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bget return message.", rank_);
        return MDHIM_ERROR;
    }

    bgrm = *((struct mdhim_bgetrm_t **) bgetrm);
    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, bgrm, sizeof(struct mdhim_bgetrm_t),
                             MPI_CHAR, comm_);

    // Allocate memory for key pointers, to be populated later.
    if ((bgrm->keys = (void**)malloc(bgrm->num_keys * sizeof(void *))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bgetrm message.", rank_);
        return MDHIM_ERROR;
    }
    memset(bgrm->keys, 0, sizeof(void *) * bgrm->num_keys);

    // Allocate memory for key_lens, to be populated later.
    if ((bgrm->key_lens = (int*)malloc(bgrm->num_keys * sizeof(int))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bget return message.", rank_);
        return MDHIM_ERROR;
    }
    memset(bgrm->key_lens, 0, sizeof(int) * bgrm->num_keys);

    // Allocate memory for value pointers, to be populated later.
    if ((bgrm->values = (void**)malloc(bgrm->num_keys * sizeof(void *))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bgetrm message.", rank_);
        return MDHIM_ERROR;
    }
    memset(bgrm->values, 0, sizeof(void *) * bgrm->num_keys);

    // Allocate memory for value_lens, to be populated later.
    if ((bgrm->value_lens = (int *)malloc(bgrm->num_keys * sizeof(int))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bget return message.", rank_);
        return MDHIM_ERROR;
    }
    memset(bgrm->value_lens, 0, sizeof(int) * bgrm->num_keys);

    // For the each of the keys and data unpack the chars plus two ints for key_lens[i] and data_lens[i].
    for (i=0; i < bgrm->num_keys; i++) {
        // Unpack the key_lens[i]
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgrm->key_lens[i], 1,
                                  MPI_INT, comm_);

        // Unpack key by first allocating memory and then extracting the values from message
        bgrm->keys[i] = nullptr;
        if (bgrm->key_lens[i] &&
            (bgrm->keys[i] = (char *)malloc(bgrm->key_lens[i] * sizeof(char))) == nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
                 "memory to unpack bget return message.", rank_);
            return MDHIM_ERROR;
        }
        if (bgrm->keys[i]) {
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->keys[i], bgrm->key_lens[i],
                                      MPI_CHAR, comm_);
        }

        // Unpack the value_lens[i]
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx, &bgrm->value_lens[i], 1,
                                  MPI_INT, comm_);

        //There wasn't a value found for this key
        if (!bgrm->value_lens[i]) {
            bgrm->values[i] = nullptr;
            continue;
        }

        // Unpack data by first allocating memory and then extracting the values from message
        bgrm->values[i] = nullptr;
        if (bgrm->value_lens[i] &&
            (bgrm->values[i] = (char *)malloc(bgrm->value_lens[i] * sizeof(char))) == nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
                 "memory to unpack bget return message.", rank_);
            return MDHIM_ERROR;
        }
        if (bgrm->values[i]) {
            return_code += MPI_Unpack(message, mesg_size, &mesg_idx, bgrm->values[i],
                                      bgrm->value_lens[i],
                                      MPI_CHAR, comm_);
        }
    }

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the bget return message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_base_message
 * Packs a base message structure into contiguous memory for message passing
 *
 * @param cm       in   structure base message which will be packed into the sendbuf
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_basem_t {
 int mtype;
 };
*/
int MPIBase::pack_base_message(struct mdhim_basem_t *cm, void **sendbuf, int *sendsize) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_basem_t); // Generous variable for size calculation
    int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;  // Variable for incremental pack
    void *outbuf;

    mesg_size = m_size;
    *sendsize = mesg_size;
    cm->size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack del message.", rank_);
        return MDHIM_ERROR;
    }

    outbuf = *sendbuf;
    // pack the message first with the structure and then followed by key and data values.
    return_code = MPI_Pack(cm, sizeof(struct mdhim_basem_t), MPI_CHAR, outbuf, mesg_size,
                           &mesg_idx, comm_);

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the del message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

///------------------------

/**
 * pack_del_message
 * Packs a delete message structure into contiguous memory for message passing
 *
 * @param dm       in   structure del_message which will be packed into the sendbuf
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_delm_t {
 int mtype;
 void *key;
 int key_len;
 int server_rank;
 };
*/
int MPIBase::pack_del_message(struct mdhim_delm_t *dm, void **sendbuf, int *sendsize) {

    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_delm_t); // Generous variable for size calculation
    int mesg_size;  // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;  // Variable for incremental pack

    // Add to size the length of the key and data fields
    m_size += dm->key_len;

    // Is the computed message size of a safe value? (less than a max message size?)
    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: del message too large."
             " Del is over Maximum size allowed of %d.", rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }
    mesg_size = m_size;
    *sendsize = mesg_size;
    dm->basem.size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack del message.", rank_);
        return MDHIM_ERROR;
    }

    // pack the message first with the structure and then followed by key and data values.
    return_code = MPI_Pack(dm, sizeof(struct mdhim_delm_t), MPI_CHAR, *sendbuf,
                           mesg_size, &mesg_idx, comm_);
    return_code += MPI_Pack(dm->key, dm->key_len, MPI_CHAR, *sendbuf,
                            mesg_size, &mesg_idx, comm_);

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the del message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * pack_bdel_message
 * Packs a bdel message structure into contiguous memory for message passing
 *
 * @param bdm      in   structure bdel_message which will be packed into the message
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bdelm_t {
 int mtype;
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int MPIBase::pack_bdel_message(struct mdhim_bdelm_t *bdm, void **sendbuf,
                               int *sendsize) {

    int return_code = MPI_SUCCESS; // MPI_SUCCESS = 0
    int64_t m_size = sizeof(struct mdhim_bdelm_t);  // Generous variable for size calc
    int mesg_size;   // Variable to be used as parameter for MPI_pack of safe size
    int mesg_idx = 0;

    // Add up the size of message
    m_size += bdm->num_keys * sizeof(int);

    // For the each of the keys add enough chars.
    for (int i = 0; i < bdm->num_keys; i++)
        m_size += bdm->key_lens[i];

    // Is the computed message size of a safe value? (less than a max message size?)
    if (m_size > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: bulk del message too large."
             " Bdel is over Maximum size allowed of %d.", rank_, MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }
    mesg_size = m_size;  // Safe size to use in MPI_pack
    *sendsize = mesg_size;
    bdm->basem.size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack bulk del message.", rank_);
        return MDHIM_ERROR;
    }

    // pack the message first with the structure and then followed by key (plus lengths).
    return_code = MPI_Pack(bdm, sizeof(struct mdhim_bdelm_t), MPI_CHAR, *sendbuf,
                           mesg_size, &mesg_idx, comm_);

    // For the each of the keys and data pack the chars plus one int for key_len.
    for (int i = 0; i < bdm->num_keys; i++) {
        return_code += MPI_Pack(&bdm->key_lens[i], 1, MPI_INT, *sendbuf,
                                mesg_size, &mesg_idx, comm_);
        return_code += MPI_Pack(bdm->keys[i], bdm->key_lens[i], MPI_CHAR,
                                *sendbuf, mesg_size, &mesg_idx,
                                comm_);
    }

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the bulk del message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_del_message
 * Unpacks a del message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param delm         out  structure get_message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_delm_t {
 int mtype;
 void *key;
 int key_len;
 int server_rank;
 };
*/
int MPIBase::unpack_del_message(void *message, int mesg_size, void **delm) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    struct mdhim_delm_t *dm;

    if ((*((struct mdhim_delm_t **) delm) = (mdhim_delm_t*)malloc(sizeof(struct mdhim_delm_t))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack del message.", rank_);
        return MDHIM_ERROR;
    }

    dm = *((struct mdhim_delm_t **) delm);
    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, dm, sizeof(struct mdhim_delm_t),
                             MPI_CHAR, comm_);

    // Unpack key by first allocating memory and then extracting the values from message
    if ((dm->key = (char *)malloc(dm->key_len * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack del message.", rank_);
        return MDHIM_ERROR;
    }
    return_code += MPI_Unpack(message, mesg_size, &mesg_idx, dm->key, dm->key_len,
                              MPI_CHAR, comm_);

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the del message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_bdel_message
 * Unpacks a bulk del message structure into contiguous memory for message passing
 *
 * @param message    in   pointer for packed message
 * @param mesg_size  in   size of the incoming message
 * @param bdelm        out  structure bulk_del_message which will be unpacked from the message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_bdelm_t {
 int mtype;
 void **keys;
 int *key_lens;
 int num_records;
 int server_rank;
 };
*/
int MPIBase::unpack_bdel_message(void *message, int mesg_size, void **bdelm) {

    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_idx = 0;  // Variable for incremental unpack
    int i;
    int num_records;

    if ((*((struct mdhim_bdelm_t **) bdelm) = (mdhim_bdelm_t*)malloc(sizeof(struct mdhim_bdelm_t))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bdel message.", rank_);
        return MDHIM_ERROR;
    }

    // Unpack the message first with the structure and then followed by key and data values.
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx,
                             (*((struct mdhim_bdelm_t **) bdelm)), sizeof(struct mdhim_bdelm_t),
                             MPI_CHAR, comm_);

    num_records = (*((struct mdhim_bdelm_t **) bdelm))->num_keys;
    // Allocate memory for keys, to be populated later.
    if (((*((struct mdhim_bdelm_t **) bdelm))->keys =
         (void**)malloc(num_records * sizeof(void *))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bdel message.", rank_);
        return MDHIM_ERROR;
    }
    // Allocate memory for key_lens, to be populated later.
    if (((*((struct mdhim_bdelm_t **) bdelm))->key_lens =
         (int *)malloc(num_records * sizeof(int))) == nullptr) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack bdel message.", rank_);
        return MDHIM_ERROR;
    }

    // For the each of the keys and data unpack the chars plus an int for key_lens[i].
    for (i=0; i < num_records; i++) {
        // Unpack the key_lens[i]
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  &(*((struct mdhim_bdelm_t **) bdelm))->key_lens[i], 1,
                                  MPI_INT, comm_);

        // Unpack key by first allocating memory and then extracting the values from message
        if (((*((struct mdhim_bdelm_t **) bdelm))->keys[i] =
             (char *)malloc((*((struct mdhim_bdelm_t **) bdelm))->key_lens[i] *
                            sizeof(char))) == nullptr) {
            mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to allocate "
                 "memory to unpack bdel message.", rank_);
            return MDHIM_ERROR;
        }
        return_code += MPI_Unpack(message, mesg_size, &mesg_idx,
                                  (*((struct mdhim_bdelm_t **) bdelm))->keys[i],
                                  (*((struct mdhim_bdelm_t **) bdelm))->key_lens[i],
                                  MPI_CHAR, comm_);
    }

    // If the unpack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the bdel message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

///------------------------


/**
 * pack_return_message
 * Packs a return message structure into contiguous memory for message passing
 *
 * @param rm       in   structure which will be packed into the sendbuf
 * @param sendbuf  out  double pointer for packed message to send
 * @param sendsize out  pointer for packed message size
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_rm_t {
 int mtype;
 int error;
 };
*/
int MPIBase::pack_return_message(struct mdhim_rm_t *rm, void **sendbuf, int *sendsize) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_size = sizeof(struct mdhim_rm_t);
    int mesg_idx = 0;
    void *outbuf;

    *sendsize = mesg_size;
    rm->basem.size = mesg_size;

    if ((*sendbuf = malloc(mesg_size * sizeof(char))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to pack return message.", rank_);
        return MDHIM_ERROR;
    }

    outbuf = *sendbuf;
    // Pack the message from the structure
    return_code = MPI_Pack(rm, sizeof(struct mdhim_rm_t), MPI_CHAR, outbuf, mesg_size, &mesg_idx,
                           comm_);

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to pack "
             "the return message.", rank_);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * unpack_return_message
 * unpacks a return message structure into contiguous memory for message passing
 *
 * @param message out  pointer for buffer to unpack message to
 * @param retm    in   return message that will be unpacked into message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 *
 * struct mdhim_rm_t {
 int mtype;
 int error;
 };
*/
int MPIBase::unpack_return_message(void *message, void **retm) {
    int return_code = MPI_SUCCESS;  // MPI_SUCCESS = 0
    int mesg_size = sizeof(struct mdhim_rm_t);
    int mesg_idx = 0;
    struct mdhim_rm_t *rm;

    if (((*(struct mdhim_rm_t **) retm) = (mdhim_rm_t*)malloc(sizeof(struct mdhim_rm_t))) == nullptr) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to allocate "
             "memory to unpack return message.", rank_);
        return MDHIM_ERROR;
    }

    rm = *((struct mdhim_rm_t **) retm);

    // Unpack the structure from the message
    return_code = MPI_Unpack(message, mesg_size, &mesg_idx, rm, sizeof(struct mdhim_rm_t),
                             MPI_CHAR, comm_);

    // If the pack did not succeed then log the error and return the error code
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: unable to unpack "
             "the return message.", rank_);
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
    char *sendbuf = nullptr;
    int sendsize = 0;
    int ret = MDHIM_ERROR;

    if ((ret = encode((struct mdhim_basem_t *) pm, &sendbuf, &sendsize)) == MDHIM_SUCCESS) {
        ret = send_rangesrv_work(pm->basem.server_rank, sendbuf, sendsize);
    }

    free(sendbuf);

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while sending "
             "put record request", rank_, ret);
    }

    return ret;
}

int MPIEndpoint::AddGetRequest(struct mdhim_bgetm_t **messages, int num_srvs) {
    char **sendbufs = (char **) calloc(num_srvs, sizeof(char *));    // pointers to sendbuf
    int *sizes = (int *) calloc(num_srvs, sizeof(int));              // size of each sendbuf
    int ret = MDHIM_SUCCESS;

    // encode each mesage
    for(int i = 0; i < num_srvs; i++) {
        void *mesg = *(messages + i);
        if (!mesg) {
            continue;
        }

        if (encode((struct mdhim_basem_t *)mesg, sendbufs + i, sizes + i) != MDHIM_SUCCESS) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: Packing message "
                 "failed before sending.", rank_);
            ret = MDHIM_ERROR;
            free(*(sendbufs + i));
            *(sendbufs + i) = 0;
            *(sizes + i) = 0;
            continue;
        }
    }

    // send all of the messages at once
    ret = send_all_rangesrv_work(sendbufs, sizes, num_srvs);

    // cleanup
    for (int i = 0; i < num_srvs; i++) {
        free(sendbufs[i]);
    }

    free(sendbufs);
    free(sizes);

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while sending "
             "bget record request", rank_, ret);
    }

    return ret;
}

int MPIEndpoint::AddPutReply(const CommAddress *src, struct mdhim_rm_t **message) {
    char *recvbuf = nullptr;
    int recvsize;
    int ret = MDHIM_ERROR;
    if ((ret = receive_client_response(((MPIAddress*)src)->Rank(), &recvbuf, &recvsize)) == MDHIM_SUCCESS) {
        struct mdhim_basem_t bm;
        int mesg_idx = 0;

        // TODO: process the return value
        ret = MPI_Unpack(recvbuf, recvsize, &mesg_idx, &bm,
                         sizeof(struct mdhim_basem_t), MPI_CHAR,
                         comm_);

        ret = decode(bm.mtype, recvbuf, bm.size, (mdhim_basem_t **)message);
    }

    free(recvbuf);

    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - Error: %d from server while receiving "
             "put record request", rank_, ret);
        (*message)->error = MDHIM_ERROR;
    }

    return ret;
}

int MPIEndpoint::AddGetReply(int *srcs, int nsrcs, struct mdhim_bgetrm_t ***messages) {
    char **recvbufs = nullptr;
    int *sizebuf = nullptr;
    int ret = MDHIM_ERROR;

    if ((ret = receive_all_client_responses(srcs, nsrcs, &recvbufs, &sizebuf)) == MDHIM_SUCCESS) {
        for (int i = 0; i < nsrcs; i++) {
            *(*messages + i) = nullptr;

            //Unpack buffer to get the message type
            struct mdhim_basem_t bm;
            int mesg_idx = 0;
            ret = MPI_Unpack(recvbufs[i], sizebuf[i], &mesg_idx, &bm,
                             sizeof(struct mdhim_basem_t), MPI_CHAR,
                             comm_);

            ret = decode(bm.mtype, recvbufs[i], bm.size, (struct mdhim_basem_t **) (*messages + i));

            free(recvbufs[i]);
        }
    }

    free(recvbufs);
    free(sizebuf);

    // If the receives did not succeed then log the error code and return MDHIM_ERROR
    if (ret != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: %d from server while receiving "
             "bget record requests", rank_, ret);
    }

    return ret;
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
