#include "ThalliumUnpacker.hpp"

int ThalliumUnpacker::any (TransportMessage **msg, const std::string &buf) {
    TransportMessage *basemsg = nullptr;
    if (unpack(&basemsg, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    switch (basemsg->mtype) {
        case TransportMessageType::PUT:
            {
                TransportPutMessage *put = nullptr;
                ret = unpack(&put, buf);
                *msg = put;
            }
            break;
        // case TransportMessageType::BPUT:
        //     {
        //         TransportBPutMessage *bput = nullptr;
        //         ret = unpack(&bput, buf);
        //         *msg = bput;
        //     }
        //     break;
        case TransportMessageType::GET:
            {
                TransportGetMessage *get = nullptr;
                ret = unpack(&get, buf);
                *msg = get;
            }
            break;
        // case TransportMessageType::BGET:
        //     {
        //         TransportBGetMessage *bget = nullptr;
        //         ret = unpack(&bget, buf);
        //         *msg = bget;
        //     }
        //     break;
        // case TransportMessageType::DELETE:
        //     {
        //         TransportDeleteMessage *dm = nullptr;
        //         ret = unpack(&dm, buf);
        //         *msg = dm;
        //     }
        //     break;
        // case TransportMessageType::BDELETE:
        //     {
        //         TransportBDeleteMessage *bdm = nullptr;
        //         ret = unpack(&bdm, buf);
        //         *msg = bdm;
        //     }
        //     break;
        // close meesages are not sent across the network
        // case TransportMessageType::CLOSE:
        //     break;
        case TransportMessageType::RECV:
            {
                TransportRecvMessage *rm = nullptr;
                ret = unpack(&rm, buf);
                *msg = rm;
            }
            break;
        case TransportMessageType::RECV_GET:
            {
                TransportGetRecvMessage *grm = nullptr;
                ret = unpack(&grm, buf);
                *msg = grm;
            }
            break;
        // case TransportMessageType::RECV_BGET:
        //     {
        //         TransportBGetRecvMessage *bgrm = nullptr;
        //         ret = unpack(&bgrm, buf);
        //         *msg = bgrm;
        //     }
        //     break;
        // commit messages are not sent across the network
        // case TransportMessageType::COMMIT:
        //     break;
        default:
            break;
    }

    Memory::FBP_MEDIUM::Instance().release(basemsg);

    return ret;
}

int ThalliumUnpacker::unpack(TransportPutMessage **pm, const std::string &buf) {
    TransportPutMessage *out = Memory::FBP_MEDIUM::Instance().acquire<TransportPutMessage>();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportMessage *>(out), s) != MDHIM_SUCCESS) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->key_len, sizeof(out->key_len))
        .read((char *) &out->value_len, sizeof(out->value_len))) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!(out->key = Memory::FBP_MEDIUM::Instance().acquire(out->key_len))         ||
        !(out->value = Memory::FBP_MEDIUM::Instance().acquire(out->value_len))     ||
        !s
        .read((char *) out->key, out->key_len)
        .read((char *) out->value, out->value_len)) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    *pm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportGetMessage **gm, const std::string &buf) {
    TransportGetMessage *out = Memory::FBP_MEDIUM::Instance().acquire<TransportGetMessage>();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportMessage *>(out), s) != MDHIM_SUCCESS) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->op, sizeof(out->op))
        .read((char *) &out->num_keys, sizeof(out->num_keys))
        .read((char *) &out->key_len, sizeof(out->key_len)) ) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!(out->key = Memory::FBP_MEDIUM::Instance().acquire(out->key_len))     ||
        !s
        .read((char *) out->key, out->key_len)) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    *gm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportRecvMessage **rm, const std::string &buf) {
    TransportRecvMessage *out = Memory::FBP_MEDIUM::Instance().acquire<TransportRecvMessage>();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportMessage *>(out), s) != MDHIM_SUCCESS) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!s.read((char *) &out->error, sizeof(out->error))) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    *rm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportGetRecvMessage **grm, const std::string &buf) {
    TransportGetRecvMessage *out = Memory::FBP_MEDIUM::Instance().acquire<TransportGetRecvMessage>();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportMessage *>(out), s) != MDHIM_SUCCESS) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->error, sizeof(out->error))
        .read((char *) &out->key_len, sizeof(out->key_len))
        .read((char *) &out->value_len, sizeof(out->value_len))) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    if (!(out->key = Memory::FBP_MEDIUM::Instance().acquire(out->key_len))         ||
        !(out->value = Memory::FBP_MEDIUM::Instance().acquire(out->value_len))     ||
        !s
        .read((char *) out->key, out->key_len)
        .read((char *) out->value, out->value_len)) {
        Memory::FBP_MEDIUM::Instance().release(out);
        return MDHIM_ERROR;
    }

    *grm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportMessage **msg, const std::string &buf) {
    TransportMessage *out = Memory::FBP_MEDIUM::Instance().acquire<TransportMessage>();
    std::stringstream s(buf);
    if (unpack(out, s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    *msg = out;
    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportMessage *msg, std::stringstream &s) {
    if (!msg) {
        return MDHIM_ERROR;
    }

    return s
        .read((char *) &msg->mtype, sizeof(msg->mtype))
        .read((char *) &msg->src, sizeof(msg->src))
        .read((char *) &msg->dst, sizeof(msg->dst))
        .read((char *) &msg->index, sizeof(msg->index))
        .read((char *) &msg->index_type, sizeof(msg->index_type))
        .read((char *) &msg->index_name, sizeof(msg->index_name))?MDHIM_SUCCESS:MDHIM_ERROR;
}
