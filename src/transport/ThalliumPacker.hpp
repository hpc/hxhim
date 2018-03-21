#ifndef HXHIM_TRANSPORT_THALLIUM_PACKER
#define HXHIM_TRANSPORT_THALLIUM_PACKER

#include <string>

#include <thallium.hpp>

#include "transport.hpp"

/**
 * ThalliumPacker
 * A collection of functions that pack TransportMessages
 * using thallium
 *
 * @param *message  povoider to the mesage that will be packed
 */
template <typename A>
void save(A &ar, TransportPutMessage &msg) {
    save(ar, &msg);
    // ar & std::string(msg.key, msg.key_len);
    // ar & msg.key_len;
    // ar & std::string(msg.value, msg.value_len);
    // ar & msg.value_len;
}

template <typename A>
void save(A &ar,  TransportGetMessage &msg) {
    save(ar, &msg);
    // ar & std::string(msg.key, msg.key_len);
    // ar & msg.key_len;
}

template <typename A>
void save(A &ar, TransportMessage *msg) {
    ar & msg->mtype;
    ar & msg->dst;
    ar & msg->index;
    ar & msg->index_type;
}

#endif
