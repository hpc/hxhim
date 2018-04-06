#include "transport.hpp"

TransportMessage::TransportMessage(const TransportMessageType type)
  : mtype(type),
    dst(-1),
    index(-1),
    index_type(-1),
    index_name(nullptr)
{}

TransportMessage::~TransportMessage() {
    cleanup();
}

int TransportMessage::size() const {
    // intentional error with sizeof(char *)
    return sizeof(mtype) + sizeof(src) + sizeof(dst) + sizeof(index) + sizeof(index_type) + sizeof(char *);
}

void TransportMessage::cleanup() {
    ::operator delete(index_name);
    index_name = nullptr;
}

TransportPutMessage::TransportPutMessage()
  : TransportMessage(TransportMessageType::PUT),
    key(nullptr), key_len(0), value(nullptr), value_len(0)
{}

TransportPutMessage::~TransportPutMessage() {
    cleanup();
}

int TransportPutMessage::size() const {
    return TransportMessage::size() + key_len + sizeof(key_len) + value_len + sizeof(value_len);
}

void TransportPutMessage::cleanup() {
    TransportMessage::cleanup();

    ::operator delete(key);
    key = nullptr;

    ::operator delete(value);
    value = nullptr;

    key_len = 0;
    value_len = 0;
}

TransportBPutMessage::TransportBPutMessage()
  : TransportMessage(TransportMessageType::RECV),
    keys(nullptr), key_lens(nullptr),
    values(nullptr), value_lens(nullptr),
    num_keys(0)
{}

TransportBPutMessage::~TransportBPutMessage() {
    cleanup();
}

int TransportBPutMessage::size() const {
    int ret = TransportMessage::size() + sizeof(num_keys) + (num_keys * sizeof(*key_lens)) + (num_keys * sizeof(*value_lens));
    for(int i = 0; i < num_keys; i++) {
        ret += key_lens[i] + value_lens[i];
    }
    return ret;
}

void TransportBPutMessage::cleanup() {
    TransportMessage::cleanup();

    for(int i = 0; i < num_keys; i++) {
        ::operator delete(keys[i]);
        ::operator delete(values[i]);
    }

    delete [] keys;
    keys = nullptr;

    delete [] key_lens;
    key_lens = nullptr;

    delete [] values;
    values = nullptr;

    delete [] value_lens;
    value_lens = nullptr;

    num_keys = 0;
}

TransportGet::TransportGet(const TransportMessageType type)
  : TransportMessage(type),
    op(TransportGetMessageOp::GET_OP_MAX),
    num_keys(0)
{}

TransportGet::~TransportGet() {}

TransportGetMessage::TransportGetMessage()
  : TransportGet(TransportMessageType::GET)
{}

TransportGetMessage::~TransportGetMessage() {
    cleanup();
}

int TransportGetMessage::size() const {
    return TransportMessage::size() + sizeof(op) + key_len + sizeof(key_len) + sizeof(num_keys);
}

void TransportGetMessage::cleanup() {
    TransportMessage::cleanup();

    ::operator delete(key);
    key = nullptr;

    key_len = 0;

    num_keys = 0;
}

TransportBGetMessage::TransportBGetMessage()
  : TransportGet(TransportMessageType::BGET),
    keys(nullptr), key_lens(nullptr),
    num_recs(0)
{}

TransportBGetMessage::~TransportBGetMessage() {
    cleanup();
}

int TransportBGetMessage::size() const {
    int ret = TransportMessage::size() + sizeof(op) + sizeof(num_keys) + sizeof(num_recs) + (num_keys * sizeof(*key_lens));
    for(int i = 0; i < num_keys; i++) {
        ret += key_lens[i];
    }

    return ret;
}

void TransportBGetMessage::cleanup() {
    TransportMessage::cleanup();

    for(int i = 0; i < num_keys; i++) {
        ::operator delete(keys[i]);
    }

    delete [] keys;
    keys = nullptr;

    delete [] key_lens;
    key_lens = nullptr;

    num_keys = 0;
}

TransportDeleteMessage::TransportDeleteMessage()
  : key(nullptr), key_len(0)
{}

TransportDeleteMessage::~TransportDeleteMessage() {
    cleanup();
}

int TransportDeleteMessage::size() const {
    return TransportMessage::size() + sizeof(key_len) + key_len;
}

void TransportDeleteMessage::cleanup() {
    TransportMessage::cleanup();
    ::operator delete(key);
    key = nullptr;

    key_len = 0;
}

TransportBDeleteMessage::TransportBDeleteMessage()
  : keys(nullptr), key_lens(nullptr),
    num_keys(0)
{}

TransportBDeleteMessage::~TransportBDeleteMessage() {
    cleanup();
}

int TransportBDeleteMessage::size() const {
    int ret = TransportMessage::size() + sizeof(num_keys) + (num_keys * sizeof(*key_lens));
    for(int i = 0; i < num_keys; i++) {
        ret += key_lens[i];
    }
    return ret;
}

void TransportBDeleteMessage::cleanup() {
    TransportMessage::cleanup();
    for(int i = 0; i < num_keys; i++) {
        ::operator delete(keys[i]);
    }

    delete [] keys;
    keys = nullptr;

    delete [] key_lens;
    key_lens = nullptr;

    num_keys = 0;
}

TransportRecvMessage::TransportRecvMessage()
  : TransportMessage(TransportMessageType::RECV),
    error(0)
{}

TransportRecvMessage::~TransportRecvMessage() {
    cleanup();
}

int TransportRecvMessage::size() const {
    return TransportMessage::size() + sizeof(error);
}

void TransportRecvMessage::cleanup() {
    TransportMessage::cleanup();
}

TransportGetRecvMessage::TransportGetRecvMessage()
  : TransportMessage(TransportMessageType::GET),
    error(MDHIM_SUCCESS),
    key(nullptr), key_len(0),
    value(nullptr), value_len(0)
{}

TransportGetRecvMessage::~TransportGetRecvMessage() {
    cleanup();
}

int TransportGetRecvMessage::size() const {
    return TransportMessage::size() + sizeof(error) + key_len + sizeof(key_len) + value_len + sizeof(value_len);
}

void TransportGetRecvMessage::cleanup() {
    TransportMessage::cleanup();

    ::operator delete(key);
    key = nullptr;

    // free here because the value comes from leveldb
    // ::operator delete(value);
    free(value);
    value = nullptr;

    key_len = 0;

    value_len = 0;
}

TransportBGetRecvMessage::TransportBGetRecvMessage()
  : TransportMessage(TransportMessageType::BGET),
    error(MDHIM_SUCCESS),
    keys(nullptr), key_lens(nullptr),
    values(nullptr), value_lens(nullptr),
    num_keys(0), next(nullptr)
{}

TransportBGetRecvMessage::~TransportBGetRecvMessage() {
    cleanup();
}

int TransportBGetRecvMessage::size() const {
    int ret = TransportMessage::size() + sizeof(error) + sizeof(num_keys) + (num_keys * sizeof(*key_lens)) + (num_keys * sizeof(*value_lens));
    for(int i = 0; i < num_keys; i++) {
        ret += key_lens[i] + value_lens[i];
    }
    return ret;
}

void TransportBGetRecvMessage::cleanup() {
    TransportMessage::cleanup();

    for(int i = 0; i < num_keys; i++) {
        ::operator delete(keys[i]);
        ::operator delete(values[i]);
    }

    delete [] keys;
    keys = nullptr;

    delete [] values;
    values = nullptr;

    delete [] key_lens;
    key_lens = nullptr;

    delete [] value_lens;
    value_lens = nullptr;

    delete next;
    next = nullptr;

    num_keys = 0;
}

TransportBRecvMessage::TransportBRecvMessage()
  : TransportMessage(TransportMessageType::RECV_BGET),
    error(0), next(nullptr)
{}

TransportBRecvMessage::~TransportBRecvMessage() {
    cleanup();
}

int TransportBRecvMessage::size() const {
    return TransportMessage::size() + sizeof(error);
}

void TransportBRecvMessage::cleanup() {
    TransportMessage::cleanup();

    ::operator delete(next);
    next = nullptr;
}
