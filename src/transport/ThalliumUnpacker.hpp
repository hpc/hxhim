#ifndef HXHIM_TRANSPORT_THALLIUM_UNPACKER
#define HXHIM_TRANSPORT_THALLIUM_UNPACKER

#include <string>

#include <thallium.hpp>

#include "transport.hpp"

/**
 * ThalliumUnpacker
 * A collection of functions that pack TransportMessages
 * using thallium
 *
 * @param *message  povoider to the mesage that will be packed
 */
template <typename A>
void load(A &ar, TransportPutMessage &msg) {
    load(ar, &msg);
    // ar & std::string(msg.key, msg.key_len);
    // ar & msg.key_len;
    // ar & std::string(msg.value, msg.value_len);
    // ar & msg.value_len;
}

template <typename A>
void load(A &ar, TransportGetMessage &msg) {
    load(ar, &msg);
    // ar & std::string(msg.key, msg.key_len);
    // ar & msg.key_len;
}

template <typename A>
void load(A &ar, TransportMessage *msg) {
    ar & msg->mtype;
    ar & msg->dst;
    ar & msg->index;
    ar & msg->index_type;
}

#endif
