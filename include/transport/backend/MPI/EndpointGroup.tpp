#include "transport/backend/MPI/Packer.hpp"
#include "transport/backend/MPI/Unpacker.hpp"
#include "transport/backend/MPI/constants.h"

/**
 * parallel_send
 * This function packs data and sends it in parallel.
 * Each array is fill so that only the first n elements (from the previous operation) are valid.
 * This function does not error. It only sends as much as possible.
 *
 * @param num_srvs the maximum number of messages there are
 * @tparm messages the array of messages to send
 * @return the number of messages successfully sent
 */
template <typename Send_t, typename>
std::size_t Transport::MPI::EndpointGroup::parallel_send(const std::size_t num_srvs, Send_t **messages) {
    if (!messages) {
        return 0;
    }

    // pack the data
    void **bufs = new void *[num_srvs]();
    std::size_t *lens = new std::size_t[num_srvs]();
    int *dsts = new int[num_srvs]();
    std::size_t pack_count = 0;
    for(std::size_t i = 0; i < num_srvs; i++) {
        if (Packer::pack(comm, messages[i], &bufs[pack_count], &lens[pack_count], packed) == TRANSPORT_SUCCESS) {
            dsts[pack_count] = messages[i]->dst;
            pack_count++;
        }
    }

    // send sizes and data in parallel
    MPI_Request **size_reqs = new MPI_Request *[pack_count]();
    MPI_Request **data_reqs = new MPI_Request *[pack_count]();
    std::size_t size_count = 0;
    std::size_t data_count = 0;

    for(std::size_t i = 0; i < pack_count; i++) {
        // this should not be necessary
        if (!bufs[i]) {
            continue;
        }

        std::map<int, int>::const_iterator dst_it = ranks.find(dsts[i]);
        if (dst_it == ranks.end()) {
            continue;
        }

        // send size
        size_reqs[i] = new MPI_Request();
        if (MPI_Isend(&lens[i], sizeof(lens[i]), MPI_CHAR, dst_it->second, TRANSPORT_MPI_SIZE_REQUEST_TAG, comm, size_reqs[i]) == MPI_SUCCESS) {
            size_count++;

            // send data
            data_reqs[i] = new MPI_Request();
            if (MPI_Isend(bufs[i], lens[i], MPI_CHAR, dst_it->second, TRANSPORT_MPI_DATA_REQUEST_TAG, comm, data_reqs[i]) == MPI_SUCCESS) {
                data_count++;
            }
            else {
                delete data_reqs[i];
                data_reqs[i] = nullptr;
            }
        }
        else {
            delete size_reqs[i];
            size_reqs[i] = nullptr;
        }
    }

    //Wait for messages to complete
    const std::size_t total_msgs = size_count + data_count;
    std::size_t done = 0;
    while (done != total_msgs) {
        for(std::size_t i = 0; i < size_count; i++) {
            if (!size_reqs[i]) {
                continue;
            }

            int flag = 0;
            MPI_Status status;
            MPI_Test(size_reqs[i], &flag, &status);

            if (flag) {
                delete size_reqs[i];
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
                delete data_reqs[i];
                data_reqs[i] = nullptr;
                done++;
            }
        }
    }

    delete [] data_reqs;
    delete [] size_reqs;
    delete [] dsts;
    for(std::size_t i = 0; i < pack_count; i++) {
        ::operator delete(bufs[i]);
    }
    delete [] bufs;

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
 * @tparam messages  A pointer to the array of messages that are received
 * @return the number of valid messages
 */
template <typename Recv_t, typename>
std::size_t Transport::MPI::EndpointGroup::parallel_recv(const std::size_t nsrcs, int *srcs, Recv_t ***messages) {
    if (!messages) {
        return 0;
    }

    MPI_Request **reqs = new MPI_Request *[nsrcs]();
    std::size_t *sizebufs = new std::size_t[nsrcs]();

    // use reqs to receive size messages from the servers in the list
    std::size_t size_req_count = 0;
    for(std::size_t i = 0; i < nsrcs; i++) {
        std::map<int, int>::const_iterator src_it = ranks.find(srcs[i]);
        if (src_it != ranks.end()) {
            reqs[size_req_count] = new MPI_Request();

            if (MPI_Irecv(&sizebufs[i], sizeof(sizebufs[i]), MPI_CHAR,
                          src_it->second, TRANSPORT_MPI_SIZE_RESPONSE_TAG, comm, reqs[i]) == MPI_SUCCESS) {
                size_req_count++;
            }
            else {
                delete reqs[size_req_count];
                reqs[size_req_count] = nullptr;
            }
        }
    }

    // Wait for size messages to complete
    std::size_t done = 0;
    while (done != size_req_count) {
        for(std::size_t i = 0; i < size_req_count; i++) {
            // if there is a request
            if (reqs[i]) {
                int flag = 0;
                MPI_Status status;

                // test the request
                MPI_Test(reqs[i], &flag, &status);

                // if the request completed, add 1
                if (flag) {
                    delete reqs[i];
                    reqs[i] = nullptr;
                    done++;
                }
            }
        }
    }

    // use reqs to receive data messages from the servers
    std::size_t data_req_count = 0;
    void **recvbufs = new void *[size_req_count]();
    for(std::size_t i = 0; i < size_req_count; i++) {
        std::map<int, int>::const_iterator src_it = ranks.find(srcs[i]);
        if (src_it != ranks.end()) {
            // Receive a message from the servers in the list
            recvbufs[data_req_count] = packed->acquire(sizebufs[data_req_count]);
            reqs[data_req_count] = new MPI_Request();

            if (MPI_Irecv(recvbufs[data_req_count], sizebufs[data_req_count], MPI_CHAR,
                          src_it->second, TRANSPORT_MPI_DATA_RESPONSE_TAG, comm, reqs[i]) == MPI_SUCCESS) {
                data_req_count++;
            }
            else {
                delete reqs[data_req_count];
                reqs[data_req_count] = nullptr;
                packed->release(recvbufs[data_req_count]);
            }
        }
    }

    //Wait for messages to complete
    done = 0;
    while (done != data_req_count) {
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

            delete reqs[i];
            reqs[i] = nullptr;
            done++;
        }
    }

    delete [] reqs;

    // unpack the data
    std::size_t valid = 0;
    *messages = new Recv_t *[data_req_count]();
    for(std::size_t i = 0; i < data_req_count; i++) {
        if (Unpacker::unpack(comm, &((*messages)[valid]), recvbufs[i], sizebufs[i], buffers) == TRANSPORT_SUCCESS) {
            valid++;
        }

        ::operator delete(recvbufs[i]);
    }

    delete [] sizebufs;
    delete [] recvbufs;

    // return how many messages were succesfully unpacked
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
    int *srvs = nullptr;
    const std::size_t num_srvs = get_num_srvs(messages, num_rangesrvs, &srvs);
    if (!num_srvs) {
        delete [] srvs;
        return nullptr;
    }

    // return value here is not useful
    parallel_send(num_srvs, messages);

    // wait for responses
    Recv_t **recv_list = nullptr;
    const std::size_t recvd = parallel_recv(num_srvs, srvs, &recv_list);

    // convert the responses into a list
    Recv_t *head = nullptr;
    Recv_t *tail = nullptr;
    for (std::size_t i = 0; i < recvd; i++) {
        Recv_t *brm = dynamic_cast<Recv_t *>(recv_list[i]);
        if (brm) {
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

    // Return response list
    delete [] recv_list;
    delete [] srvs;
    return head;
}
