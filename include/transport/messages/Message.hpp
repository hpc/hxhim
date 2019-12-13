#ifndef TRANSPORT_MESSAGE_HPP
#define TRANSPORT_MESSAGE_HPP

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <type_traits>

#include "utils/memory.hpp"
#include "transport/messages/constants.hpp"

namespace Transport {

template <typename Data_t>
class Message {
    protected:
        Message(const Type type, const Direction dir, const std::size_t max_count)
            : type(type),
              dir(dir),
              count(0),
              max_count(max_count),
              data(alloc_array<Data_t *>(max_count)),
              next(nullptr)
        {}

    public:
        Message(void *buf, std::size_t bufsize) {
            if (!buf || !bufsize) {
                throw std::runtime_error("Message received bad buffer");
            }

            memcpy(&src, buf, sizeof(src));
            buf = static_cast <char *> (buf) + sizeof(src);

            memcpy(&dst, buf, sizeof(dst));
            buf = static_cast <char *> (buf) + sizeof(dst);

            memcpy(&type, buf, sizeof(type));
            buf = static_cast <char *> (buf) + sizeof(type);

            memcpy(&dir, buf, sizeof(dir));
            buf = static_cast <char *> (buf) + sizeof(dir);

            memcpy(&count, buf, sizeof(count));
            buf = static_cast <char *> (buf) + sizeof(count);

            // the buffer defines the maximum amount of data in this message
            max_count = count;

            if (std::is_same<void, Data_t>::value) {
                return;
            }

            data = alloc_array<Data_t *>(max_count);

            // unpack data
            for(std::size_t i = 0; i < count; i++) {
                data[i] = construct <Data_t> ();
                if (data[i]->unpack(buf, bufsize) != TRANSPORT_SUCCESS) {
                    dealloc_array(data, i + 1); // destruct the current set of data as well
                    throw std::runtime_error("Could not unpack BPUT data");
                }
            }
        }

        virtual ~Message() {
            if (std::is_same<void, Data_t>::value) {
                return;
            }

            for(std::size_t i = 0; i < count; i++) {
                destruct(data[i]);
            }
            dealloc_array(data, count);
        }

        std::size_t size() const {
            std::size_t octets =
                sizeof(src) +
                sizeof(dst) +
                sizeof(type) +
                sizeof(dir) +
                sizeof(count);
            for(std::size_t i = 0; i < count; i++) {
                octets += data[i]->size();
            }
            return octets;
        }

        int add(Data_t *ptr) {
            if (count == max_count) {
                return TRANSPORT_ERROR;
            }

            data[count++] = ptr;
            return TRANSPORT_SUCCESS;
        }

        int pack(void *buf, std::size_t bufsize) const {
            if (!buf || !bufsize) {
                return TRANSPORT_ERROR;
            }

            if (Message::size() > bufsize) {
                return TRANSPORT_ERROR;
            }

            memcpy(buf, &src, sizeof(src));
            buf = static_cast <char *> (buf) + sizeof(src);

            memcpy(buf, &dst, sizeof(dst));
            buf = static_cast <char *> (buf) + sizeof(dst);

            memcpy(buf, &type, sizeof(type));
            buf = static_cast <char *> (buf) + sizeof(type);

            memcpy(buf, &dir, sizeof(dir));
            buf = static_cast <char *> (buf) + sizeof(dir);

            memcpy(buf, &count, sizeof(count));
            buf = static_cast <char *> (buf) + sizeof(count);

            bufsize -= Message::size();

            // pack data
            for(std::size_t i = 0; i < count; i++) {
                if (data[i]->pack(buf, bufsize) != TRANSPORT_SUCCESS) {
                    return TRANSPORT_ERROR;
                }
            }

            return TRANSPORT_SUCCESS;
        }

        int src;                 // sending server
        int dst;                 // receiving server
        Type type;
        Direction dir;           // request or response
        std::size_t count;       // current number of elements
        std::size_t max_count;   // maximum allowed number of elements
        Data_t **data;

        Message <Data_t> *next;
};

}

#endif
