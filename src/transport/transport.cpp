#include "transport.hpp"

TransportMessage::TransportMessage(const TransportMessageType type)
  : mtype(type),
    server_rank(-1),
    index(-1),
    index_type(-1),
    index_name(nullptr)
{}

TransportMessage::~TransportMessage() {
    cleanup();
}

int TransportMessage::size() const {
    // intentional error with sizeof(char *)
    return sizeof(mtype) + sizeof(server_rank) + sizeof(index) + sizeof(index_type) + sizeof(char *);
}

void TransportMessage::cleanup() {
    // free(index_name);
    // index_name = nullptr;
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
    // TransportMessage::cleanup();

    // free(key);
    // key = nullptr;

    // free(value);
    // value = nullptr;

    // key_len = 0;
    // value_len = 0;
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
    // TransportMessage::cleanup();

    // for(int i = 0; i < num_keys; i++) {
    //     free(keys[i]);
    //     free(values[i]);
    // }

    // free(keys);
    // keys = nullptr;

    // free(key_lens);
    // key_lens = nullptr;

    // free(values);
    // values = nullptr;

    // free(value_lens);
    // value_lens = nullptr;

    // num_keys = 0;
}

TransportGet::TransportGet()
    : TransportMessage(TransportMessageType::BGET),
      op(TransportGetMessageOp::GET_OP_MAX),
      num_keys(0)
{}

TransportGet::~TransportGet() {}

TransportGetMessage::TransportGetMessage()
  : TransportGet()
{}

TransportGetMessage::~TransportGetMessage() {
    cleanup();
}

int TransportGetMessage::size() const {
    return TransportMessage::size() + sizeof(op) + key_len + sizeof(key_len) + sizeof(num_keys);
}

void TransportGetMessage::cleanup() {
    // TransportMessage::cleanup();

    // free(key);
    // key_len = 0;

    // num_keys = 0;
}

TransportBGetMessage::TransportBGetMessage()
    : TransportGet(),
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
    // TransportMessage::cleanup();

    // for(int i = 0; i < num_keys; i++) {
    //     free(keys[i]);
    // }

    // free(keys);
    // keys = nullptr;

    // free(key_lens);
    // key_lens = nullptr;

    // num_keys = 0;
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
    // TransportMessage::cleanup();
    // free(key);
    // key = nullptr;

    // key_len = 0;
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
    // TransportMessage::cleanup();
    // for(int i = 0; i < num_keys; i++) {
    //     free(keys[i]);
    // }

    // free(keys);
    // keys = nullptr;

    // free(key_lens);
    // key_lens = nullptr;

    // num_keys = 0;
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
    // TransportMessage::cleanup();
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
    // TransportMessage::cleanup();

    // for(int i = 0; i < num_keys; i++) {
    //     if (keys) {
    //         free(keys[i]);
    //     }

    //     if (values) {
    //         free(values[i]);
    //     }
    // }

    // free(keys);
    // keys = nullptr;

    // free(values);
    // values = nullptr;

    // free(key_lens);
    // key_lens = nullptr;

    // free(value_lens);
    // value_lens = nullptr;

    // delete next;
    // next = nullptr;

    // num_keys = 0;
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
    // TransportMessage::cleanup();

    // delete next;
    // next = nullptr;
}
