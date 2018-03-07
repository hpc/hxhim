#include <unistd.h>

#include "partitioner.h"
#include "mdhim_private.h"
#include "messages.h"
#include "transport_mpi.hpp"

static void test_req_and_wait(struct mdhim *md, MPI_Request *req, int *flag, MPI_Status *status) {
    while (!*flag && !md->p->shutdown) {
        usleep(100);

        pthread_mutex_lock(&md->p->mdhim_comm_lock);
        MPI_Test(req, flag, status);
        pthread_mutex_unlock(&md->p->mdhim_comm_lock);
    }
}

static void test_req_and_wait(struct mdhim *md, MPI_Request *req) {
    int flag = 0;
    MPI_Status status;

    test_req_and_wait(md, req, &flag, &status);
}

/**
 * only_send_client_response
 * Sends a char buffer to a client
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to
 * @param sendbuf double pointer to packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
static int only_send_client_response(struct mdhim *md, int dest, int *sizebuf,
                                     void **sendbuf, MPI_Request **size_req, MPI_Request **msg_req) {
    int return_code = 0;
    int mtype;
    int ret = MDHIM_SUCCESS;

    //Send the size message
    *size_req = (MPI_Request*)malloc(sizeof(MPI_Request));

    pthread_mutex_lock(&md->p->mdhim_comm_lock);
    return_code = MPI_Isend(sizebuf, sizeof(*sizebuf), MPI_CHAR, dest, CLIENT_RESPONSE_SIZE_MSG,
                            md->p->mdhim_comm, *size_req);
    pthread_mutex_unlock(&md->p->mdhim_comm_lock);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %s - "
             "Error sending client response message size in send_client_response",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        ret = MDHIM_ERROR;
        free(*size_req);
        *size_req = NULL;
    }

    *msg_req = (MPI_Request*)malloc(sizeof(MPI_Request));

    //Send the actual message
    pthread_mutex_lock(&md->p->mdhim_comm_lock);
    return_code = MPI_Isend(*sendbuf, *sizebuf, MPI_PACKED, dest, CLIENT_RESPONSE_MSG,
                            md->p->mdhim_comm, *msg_req);
    pthread_mutex_unlock(&md->p->mdhim_comm_lock);

    if (return_code != MPI_SUCCESS) {
        mlog(MPI_CRIT, "Rank %s - "
             "Error sending client response message in send_client_response",
             ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
        ret = MDHIM_ERROR;
        free(*msg_req);
        *msg_req = NULL;
    }

    return ret;
}

/**
 * send_client_response
 * Sends a message to a client
 *
 * @param md      main MDHIM struct
 * @param dest    destination to send to
 * @param message pointer to message to send
 * @param sendbuf double pointer to packed message
 * @return MDHIM_SUCCESS or MDHIM_ERROR on error
 */
int send_client_response(struct mdhim *md, int dest, TransportMessage *message, int *sizebuf,
                         void **sendbuf, MPI_Request **size_req, MPI_Request **msg_req) {
    int ret = MDHIM_ERROR;
    if ((ret = MPIPacker::any(dynamic_cast<MPIEndpoint *>(md->p->transport->Endpoint()), message, sendbuf, sizebuf)) == MDHIM_SUCCESS) {
        ret = only_send_client_response(md, dest, sizebuf, sendbuf, size_req, msg_req);
    }

    return ret;
}

/**
 * only_receive_rangesrv_work message
 * Receives a message (char buf) from the given source
 *
 * @param md       in   main MDHIM struct
 * @param src      out  pointer to source of message received
 * @param recvbuf  out  double pointer for message received
 * @param recvsize out  pointer for the size of the message received
 * @return MDHIM_SUCCESS, MDHIM_CLOSE, MDHIM_COMMIT, or MDHIM_ERROR on error
 */
static int only_receive_rangesrv_work(struct mdhim *md, int *src, void **recvbuf, int *recvsize) {
    int return_code;
    MPI_Status status;
    MPI_Request req;
    int flag = 0;
    int ret = MDHIM_SUCCESS;

    // force the srouce rank to be bad
    status.MPI_SOURCE = -1;

    // Receive a message size from any client
    pthread_mutex_lock(&md->p->mdhim_comm_lock);
    return_code = MPI_Irecv(recvsize, sizeof(*recvsize), MPI_CHAR, MPI_ANY_SOURCE, RANGESRV_WORK_SIZE_MSG,
                            md->p->mdhim_comm, &req);
    pthread_mutex_unlock(&md->p->mdhim_comm_lock);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - Error: %d "
             "receive size message failed.", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
        return MDHIM_ERROR;
    }
    test_req_and_wait(md, &req, &flag, &status);

    if (return_code == MPI_ERR_IN_STATUS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - Received an error status: %d "
             " while receiving work message size", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), status.MPI_ERROR);
    }

    *recvbuf = (char *)calloc(*recvsize, sizeof(char));
    flag = 0;

    // Receive the message from the client
    pthread_mutex_lock(&md->p->mdhim_comm_lock);
    return_code = MPI_Irecv((void *)*recvbuf, *recvsize, MPI_PACKED, status.MPI_SOURCE,
                            RANGESRV_WORK_MSG, md->p->mdhim_comm, &req);
    pthread_mutex_unlock(&md->p->mdhim_comm_lock);

    // If the receive did not succeed then return the error code back
    if ( return_code != MPI_SUCCESS ) {
                 mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - Error: %d "
                     "receive message failed.", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
        return MDHIM_ERROR;
    }
    test_req_and_wait(md, &req, &flag, &status);

    if (return_code == MPI_ERR_IN_STATUS) {
        mlog(MDHIM_SERVER_CRIT, "MDHIM Rank %s - Received an error status: %d "
                     " while receiving work message", ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), status.MPI_ERROR);
    }
    if (!*recvbuf) {
        return MDHIM_ERROR;
    }

    *src = status.MPI_SOURCE;

    return ret;
}

/**
 * receive_rangesrv_work message
 * Receives a message from the given source
 *
 * @param md      in   main MDHIM struct
 * @param src     out  pointer to source of message received
 * @param message out  double pointer for message received
 * @return MDHIM_SUCCESS, MDHIM_CLOSE, MDHIM_COMMIT, or MDHIM_ERROR on error
 */
int receive_rangesrv_work(struct mdhim *md, int *src, TransportMessage **message) {
    void *recvbuf = nullptr;
    int recvsize = 0;

    int ret = MDHIM_ERROR;
    if ((ret = only_receive_rangesrv_work(md, src, &recvbuf, &recvsize)) == MDHIM_SUCCESS) {
        ret = MPIUnpacker::any(dynamic_cast<MPIEndpoint *>(md->p->transport->Endpoint()), message, recvbuf, recvsize);
    }

    free(recvbuf);
    return ret;
}
