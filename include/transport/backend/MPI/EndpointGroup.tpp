#include "transport/backend/MPI/Packer.hpp"
#include "transport/backend/MPI/Unpacker.hpp"
#include "transport/backend/MPI/constants.h"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * parallel_send
 * This function packs data and sends it in parallel.
 * Each array is filled so that only the first n elements (from the previous operation) are valid.
 * This function does not error. It only sends as much as possible.
 *
 * @param num_srvs the maximum number of messages there are
 * @tparm messages the array of messages to send
 * @return the number of messages successfully sent
 */
template <typename Send_t, typename>
std::size_t Transport::MPI::EndpointGroup::parallel_send(const std::size_t num_srvs, Send_t **messages) {
    // This value is 1 too many since local sends will not happen
    mlog(MPI_DBG, "Attempting to send %zu messages", num_srvs - 1);

    if (!num_srvs || !messages) {
        mlog(MPI_ERR, "No messages to send");
        return 0;
    }

    if (!srvs) {
        mlog(MPI_ERR, "Nowhere to store srvs");
        return 0;
    }

    // pack the data
    void **bufs = ptrs->acquire_array<void *>(num_srvs);
    std::size_t pack_count = 0;
    for(std::size_t i = 0; i < num_srvs; i++) {
        Send_t *msg = messages[i];
        if (!msg) {
            mlog(MPI_DBG, "No message to pack at index %zu", i);
            continue;
        }

        mlog(MPI_DBG, "Attempting to pack message[%zu] (type %s, size %zu, %d -> %d)", i, Message::TypeStr[msg->type], msg->size(), msg->src, msg->dst);

        if (Packer::pack(comm, msg, &bufs[pack_count], &lens[pack_count], packed.get()) == TRANSPORT_SUCCESS) {
            dsts[pack_count] = msg->dst;
            pack_count++;
            mlog(MPI_DBG, "Successfully packed message[%zu] (type %s, size %zu, %d -> %d)", i, Message::TypeStr[msg->type], msg->size(), msg->src, msg->dst);
        }
        else {
            mlog(MPI_ERR, "Failed to pack message[%zu] (type %s, size %zu, %d -> %d)", i, Message::TypeStr[msg->type], msg->size(), msg->src, msg->dst);
        }
    }

    mlog(MPI_DBG, "Successfully packed %zu messages", pack_count);

    // send sizes and data in parallel
    MPI_Request **size_reqs = ptrs->acquire_array<MPI_Request *>(pack_count);
    MPI_Request **data_reqs = ptrs->acquire_array<MPI_Request *>(pack_count);
    std::size_t size_count = 0;
    std::size_t data_count = 0;

    mlog(MPI_DBG, "Starting to send messages asynchronously");
    for(std::size_t i = 0; i < pack_count; i++) {
        // this is not really necessary
        std::map<int, int>::const_iterator dst_it = ranks.find(dsts[i]);

        if (dst_it == ranks.end()) {
            continue;
        }

        // send size
        size_reqs[size_count] = mpi_requests->acquire<MPI_Request>();

        mlog(MPI_DBG, "Attempting to send packed message[%zu] (size %zu, %d -> %d)", i, lens[i], rank, dst_it->second);

        if (MPI_Isend(&lens[i], sizeof(lens[i]), MPI_CHAR, dst_it->second, TRANSPORT_MPI_SIZE_REQUEST_TAG, comm, size_reqs[size_count]) == MPI_SUCCESS) {
            mlog(MPI_DBG, "Successfully started sending size %zu to server %d", lens[i], dst_it->second);

            size_count++;

            // send data
            data_reqs[data_count] = mpi_requests->acquire<MPI_Request>();
            if (MPI_Isend(bufs[i], lens[i], MPI_CHAR, dst_it->second, TRANSPORT_MPI_DATA_REQUEST_TAG, comm, data_reqs[data_count]) == MPI_SUCCESS) {
                mlog(MPI_DBG, "Successfully started data of size %zu to server %d", lens[i], dst_it->second);
                srvs[data_count] = dst_it->second;
                data_count++;
            }
            else {
                mlog(MPI_ERR, "Errored while sending data of size %zu to server %d", lens[i], dst_it->second);
                mpi_requests->release(data_reqs[i]);
                data_reqs[data_count] = nullptr;
            }
        }
        else {
            mlog(MPI_ERR, "Errored while sending size %zu to server %d", lens[i], dst_it->second);
            mpi_requests->release(size_reqs[i]);
            size_reqs[size_count] = nullptr;
        }
    }
    mlog(MPI_DBG, "Done sending messages asynchronously");

    mlog(MPI_DBG, "Waiting for messages to complete");

    //Wait for messages to complete
    int rank;
    MPI_Comm_rank(comm, &rank);
    const std::size_t total_msgs = size_count + data_count;
    std::size_t done = 0;
    while (running && (done != total_msgs)) {
        for(std::size_t i = 0; i < size_count; i++) {
            if (!size_reqs[i]) {
                continue;
            }

            int flag = 0;
            MPI_Status status;
            MPI_Test(size_reqs[i], &flag, &status);

            if (flag) {
                mpi_requests->release(size_reqs[i]);
                size_reqs[i] = nullptr;
                done++;
            }
        }

        for(std::size_t i = 0; i < data_count; i++) {
            if (!data_reqs[i]) {
                continue;
            }

            int flag = 0;
            MPI_Status status;

            MPI_Test(data_reqs[i], &flag, &status);

            if (flag) {
                mpi_requests->release(data_reqs[i]);
                data_reqs[i] = nullptr;
                done++;
            }
        }
    }

    // Free any remaining requests
    for(std::size_t i = 0; i < size_count; i++) {
        if (size_reqs[i]) {
            MPI_Request_free(size_reqs[i]);
            mpi_requests->release(size_reqs[i]);
        }
    }

    for(std::size_t i = 0; i < data_count; i++) {
        if (data_reqs[i]) {
            MPI_Request_free(data_reqs[i]);
            mpi_requests->release(data_reqs[i]);
        }
    }

    ptrs->release_array(data_reqs, pack_count);
    ptrs->release_array(size_reqs, pack_count);
    for(std::size_t i = 0; i < pack_count; i++) {
        packed->release(bufs[i], lens[i]);
    }
    ptrs->release_array(bufs, num_srvs);

    mlog(MPI_DBG, "Messages completed: %zu", data_count);

    return data_count;
}

/**
 * parallel_recv
 * This function receives as much data as possible.
 * Each array is fill so that only the first n elements (from the previous operation) are valid.
 * This function does not error.
 *
 * @param  nsrcs     the number of source range servers that are expected
 * @param  srcs      the array of source range servers
OD * @tparam messages  A pointer to the array of messages that are received
 * @return the number of valid messages
 */
template <typename Recv_t, typename>
std::size_t Transport::MPI::EndpointGroup::parallel_recv(const std::size_t nsrcs, int *srcs, Recv_t ***messages) {
    if (!nsrcs || !srcs) {
        mlog(MPI_DBG, "No messages to receive");
        return 0;
    }

    if (!messages) {
        mlog(MPI_DBG, "Nowhere to store messages");
        return 0;
    }

    mlog(MPI_DBG, "Waiting to receive %zu messages", nsrcs);

    MPI_Request **reqs = ptrs->acquire_array<MPI_Request *>(nsrcs);

    // use reqs to receive size messages from the servers in the list
    std::size_t size_req_count = 0;
    for(std::size_t i = 0; i < nsrcs; i++) {
        //this is not really necessary
        std::map<int, int>::const_iterator src_it = ranks.find(srcs[i]);

        if (src_it != ranks.end()) {
            reqs[size_req_count] = mpi_requests->acquire<MPI_Request>();
            if (MPI_Irecv(&lens[i], sizeof(lens[i]), MPI_CHAR,
                          src_it->second, TRANSPORT_MPI_SIZE_RESPONSE_TAG, comm, reqs[size_req_count]) == MPI_SUCCESS) {
                mlog(MPI_DBG, "Receiving size[%zu] from %d %d", i, src_it->second, srcs[i]);
                size_req_count++;
            }
            else {
                mlog(MPI_DBG, "Failed to start receiving size[%zu]", i);
                mpi_requests->release(reqs[size_req_count]);
                reqs[size_req_count] = nullptr;
            }
        }
    }

    // Wait for size messages to complete
    mlog(MPI_DBG, "Waiting for size to be received");

    std::size_t done = 0;
    while (running && (done != size_req_count)) {
        for(std::size_t i = 0; i < size_req_count; i++) {
            // if there is a request
            if (reqs[i]) {
                int flag = 0;
                MPI_Status status;

                // test the request
                MPI_Test(reqs[i], &flag, &status);

                // if the request completed, add 1
                if (flag) {
                    // mlog(MPI_DBG, "size received: %zu", i);
                    mpi_requests->release(reqs[i]);
                    reqs[i] = nullptr;
                    done++;
                }
                else {
                    // mlog(MPI_DBG, "size[%zu] not completed yet %zu", i, sizeof(lens[i]));
                }
            }
        }
    }

    // Free any remaining requests
    for(std::size_t i = 0; i < size_req_count; i++) {
        if (reqs[i]) {
            MPI_Request_free(reqs[i]);
            mpi_requests->release(reqs[i]);
        }
    }

    mlog(MPI_DBG, "Received %zu sizes", done);

    // reuse reqs to receive data messages from the servers
    std::size_t data_req_count = 0;
    void **recvbufs = ptrs->acquire_array<void *>(size_req_count);
    for(std::size_t i = 0; i < size_req_count; i++) {
        std::map<int, int>::const_iterator src_it = ranks.find(srcs[i]);
        if (src_it != ranks.end()) {
            // Receive a message from the servers in the list
            reqs[data_req_count] = mpi_requests->acquire<MPI_Request>();
            recvbufs[data_req_count] = packed->acquire(lens[i]);
            if (MPI_Irecv(recvbufs[data_req_count], lens[i], MPI_CHAR,
                          src_it->second, TRANSPORT_MPI_DATA_RESPONSE_TAG, comm, reqs[data_req_count]) == MPI_SUCCESS) {
                data_req_count++;
            }
            else {
                mpi_requests->release(reqs[data_req_count]);
                reqs[data_req_count] = nullptr;
                packed->release(recvbufs[data_req_count], lens[data_req_count]);
            }
        }
    }

    mlog(MPI_DBG, "Completed receiving %zu sizes", done);

    // Wait for messages to complete
    mlog(MPI_DBG, "Waiting for data to be received");

    done = 0;
    while (running && (done != data_req_count)) {
        for(std::size_t i = 0; i < data_req_count; i++) {
            if (!reqs[i]) {
                continue;
            }

            int flag = 0;
            MPI_Status status;

            MPI_Test(reqs[i], &flag, &status);

            if (!flag) {
                continue;
            }

            mpi_requests->release(reqs[i]);
            reqs[i] = nullptr;
            done++;
        }
    }

    mlog(MPI_DBG, "Data received");

    // Free any remaining requests
    for(std::size_t i = 0; i < data_req_count; i++) {
        if (reqs[i]) {
            MPI_Request_free(reqs[i]);
            mpi_requests->release(reqs[i]);
        }
    }

    ptrs->release_array(reqs, nsrcs);

    // unpack the data
    std::size_t valid = 0;
    *messages = arrays->acquire_array<Recv_t *>(data_req_count);
    for(std::size_t i = 0; i < data_req_count; i++) {
        if (Unpacker::unpack(comm, &((*messages)[valid]), recvbufs[i], lens[i], responses, arrays, buffers) == TRANSPORT_SUCCESS) {
            valid++;
        }

        packed->release(recvbufs[i], lens[i]);
    }

    ptrs->release_array(recvbufs, size_req_count);

    // return how many messages were successfully unpacked
    return valid;
}

/**
 * return_msgs
 * Send Transport::B* messages and waits for their responses.
 * The responses are chained together into a list.
 *
 * @param num_rangesrvs the number of range servers there are
 * @param messages      an array of messages to send
 * @treturn a linked list of return messages
 */
template<typename Recv_t, typename Send_t, typename>
Recv_t *Transport::MPI::EndpointGroup::return_msgs(const std::size_t num_rangesrvs, Send_t **messages) {
    mlog(MPI_DBG, "Maximum number of messages: %zu", num_rangesrvs);

    // return value here is not useful
    const std::size_t sent = parallel_send(num_rangesrvs, messages);

    mlog(MPI_DBG, "Sent to %zu servers:", sent);
    for(std::size_t i = 0; i < sent; i++) {
        mlog(MPI_DBG, "    Server %d", srvs[i]);
    }

    mlog(MPI_DBG, "Waiting for %zu responses", sent);

    // wait for responses
    Recv_t **recv_list = nullptr;
    const std::size_t recvd = parallel_recv(sent, srvs, &recv_list);
    mlog(MPI_DBG, "Received from %zu servers", recvd);

    // convert the responses into a list
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;
    for (std::size_t i = 0; i < recvd; i++) {
        Recv_t *brm = dynamic_cast<Recv_t *>(recv_list[i]);
        if (brm) {
            mlog(MPI_DBG, "    Server %d", brm->src);
            //Build the linked list to return
            if (!head) {
                head = brm;
                tail = brm;
            } else {
                tail->next = brm;
                tail = brm;
            }
        }
    }

    arrays->release_array(recv_list, recvd);

    mlog(MPI_DBG, "Completed return_msgs");

    // Return response list
    return head;
}
