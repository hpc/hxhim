#include "transport/ThalliumUnpacker.hpp"

int ThalliumUnpacker::any(TransportMessage **msg, const std::string &buf) {
    TransportMessage *basemsg = nullptr;
    if (unpack(&basemsg, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // try unpacking into a TransportRequestMessage
    TransportRequestMessage *req = nullptr;
    if (unpack(&req, buf, basemsg->mtype) == MDHIM_SUCCESS) {
        *msg = req;
        delete basemsg;
        return MDHIM_SUCCESS;
    }

    // try unpacking into a Transport Response
    TransportResponseMessage *res = nullptr;
    if (unpack(&res, buf, basemsg->mtype) == MDHIM_SUCCESS) {
        *msg = res;
        delete basemsg;
        return MDHIM_SUCCESS;
    }

    *msg = nullptr;
    delete basemsg;
    return MDHIM_ERROR;
}

int ThalliumUnpacker::unpack(TransportRequestMessage **req, const std::string &buf) {
    TransportMessage *basemsg = nullptr;
    if (unpack(&basemsg, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    const int ret = unpack(req, buf, basemsg->mtype);

    delete basemsg;

    return ret;
}

int ThalliumUnpacker::unpack(TransportPutMessage **pm, const std::string &buf) {
    TransportPutMessage *out = new TransportPutMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportRequestMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->rs_idx, sizeof(out->rs_idx))
        .read((char *) &out->key_len, sizeof(out->key_len))
        .read((char *) &out->value_len, sizeof(out->value_len))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!(out->key = ::operator new(out->key_len))     ||
        !(out->value = ::operator new(out->value_len)) ||
        !s
        .read((char *) out->key, out->key_len)
        .read((char *) out->value, out->value_len)) {
        delete out;
        return MDHIM_ERROR;
    }

    *pm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportBPutMessage **bpm, const std::string &buf) {
    TransportBPutMessage *out = new TransportBPutMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportRequestMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->num_keys, sizeof(out->num_keys))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())             ||
            !(out->keys = new void *[out->num_keys]())            ||
            !(out->key_lens = new std::size_t[out->num_keys]())   ||
            !(out->values = new void *[out->num_keys]())          ||
            !(out->value_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!s
                .read((char *) &out->rs_idx[i], sizeof(out->rs_idx[i]))         ||
                // read the key
                !s
                .read((char *) &out->key_lens[i], sizeof(out->key_lens[i]))     ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))              ||
                !s
                .read((char *) out->keys[i], out->key_lens[i])                  ||
                // read the value
                !s
                .read((char *) &out->value_lens[i], sizeof(out->value_lens[i])) ||
                !(out->values[i] = ::operator new(out->value_lens[i]))          ||
                !s
                .read((char *) out->values[i], out->value_lens[i]))              {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bpm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportGetMessage **gm, const std::string &buf) {
    TransportGetMessage *out = new TransportGetMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportRequestMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->op, sizeof(out->op))
        .read((char *) &out->num_keys, sizeof(out->num_keys))
        .read((char *) &out->rs_idx, sizeof(out->rs_idx))
        .read((char *) &out->key_len, sizeof(out->key_len))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!(out->key = ::operator new(out->key_len)) ||
        !s
        .read((char *) out->key, out->key_len)) {
        delete out;
        return MDHIM_ERROR;
    }

    *gm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportBGetMessage **bgm, const std::string &buf) {
    TransportBGetMessage *out = new TransportBGetMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportRequestMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->op, sizeof(out->op))
        .read((char *) &out->num_keys, sizeof(out->num_keys))
        .read((char *) &out->num_recs, sizeof(out->num_recs))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())           ||
            !(out->keys = new void *[out->num_keys]())          ||
            !(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!s
                .read((char *) &out->rs_idx[i], sizeof(out->rs_idx[i]))) {
                delete out;
                return MDHIM_ERROR;
            }

            if (!s
                .read((char *) &out->key_lens[i], sizeof(out->key_lens[i])) ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))          ||
                !s
                .read((char *) out->keys[i], out->key_lens[i]))              {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bgm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportDeleteMessage **dm, const std::string &buf) {
    TransportDeleteMessage *out = new TransportDeleteMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportRequestMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->rs_idx, sizeof(out->rs_idx))
        .read((char *) &out->key_len, sizeof(out->key_len))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!(out->key = ::operator new(out->key_len)) ||
        !s
        .read((char *) out->key, out->key_len)) {
        delete out;
        return MDHIM_ERROR;
    }

    *dm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportBDeleteMessage **bdm, const std::string &buf) {
    TransportBDeleteMessage *out = new TransportBDeleteMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportRequestMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->num_keys, sizeof(out->num_keys))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())           ||
            !(out->keys = new void *[out->num_keys]())          ||
            !(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!s
                .read((char *) &out->rs_idx[i], sizeof(out->rs_idx[i]))) {
                delete out;
                return MDHIM_ERROR;
            }

            if (!s
                .read((char *) &out->key_lens[i], sizeof(out->key_lens[i])) ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))          ||
                !s
                .read((char *) out->keys[i], out->key_lens[i]))              {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bdm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportResponseMessage **res, const std::string &buf) {
    TransportMessage *basemsg = nullptr;
    if (unpack(&basemsg, buf) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    const int ret = unpack(res, buf, basemsg->mtype);

    delete basemsg;

    return ret;
}

int ThalliumUnpacker::unpack(TransportRecvMessage **rm, const std::string &buf) {
    TransportRecvMessage *out = new TransportRecvMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportResponseMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->rs_idx, sizeof(out->rs_idx))
        .read((char *) &out->error, sizeof(out->error))) {
        delete out;
        return MDHIM_ERROR;
    }

    *rm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportGetRecvMessage **grm, const std::string &buf) {
    TransportGetRecvMessage *out = new TransportGetRecvMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportResponseMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->rs_idx, sizeof(out->rs_idx))
        .read((char *) &out->error, sizeof(out->error))
        .read((char *) &out->key_len, sizeof(out->key_len))
        .read((char *) &out->value_len, sizeof(out->value_len))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!(out->key = ::operator new(out->key_len))     ||
        !(out->value = ::operator new(out->value_len)) ||
        !s
        .read((char *) out->key, out->key_len)
        .read((char *) out->value, out->value_len)) {
        delete out;
        return MDHIM_ERROR;
    }

    *grm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportBGetRecvMessage **bgrm, const std::string &buf) {
    TransportBGetRecvMessage *out = new TransportBGetRecvMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportResponseMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->error, sizeof(out->error))
        .read((char *) &out->num_keys, sizeof(out->num_keys))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())             ||
            !(out->keys = new void *[out->num_keys]())            ||
            !(out->key_lens = new std::size_t[out->num_keys]())   ||
            !(out->values = new void *[out->num_keys]())          ||
            !(out->value_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!s
                .read((char *) &out->rs_idx[i], sizeof(out->rs_idx[i]))         ||
                // read the key
                !s
                .read((char *) &out->key_lens[i], sizeof(out->key_lens[i]))     ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))              ||
                !s
                .read((char *) out->keys[i], out->key_lens[i])                  ||
                // read the value
                !s
                .read((char *) &out->value_lens[i], sizeof(out->value_lens[i])) ||
                !(out->values[i] = malloc(out->value_lens[i]))                  ||
                !s
                .read((char *) out->values[i], out->value_lens[i]))              {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bgrm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportBRecvMessage **brm, const std::string &buf) {
    TransportBRecvMessage *out = new TransportBRecvMessage();
    std::stringstream s(buf);
    if (unpack(static_cast<TransportResponseMessage *>(out), s) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (!s
        .read((char *) &out->error, sizeof(out->error))
        .read((char *) &out->num_keys, sizeof(out->num_keys))) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!s
                .read((char *) &out->rs_idx[i], sizeof(out->rs_idx[i]))) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *brm = out;

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportMessage **msg, const std::string &buf) {
    TransportMessage *out = new TransportMessage();
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

    msg->clean = true;
    return s
        .read((char *) &msg->mtype, sizeof(msg->mtype))
        .read((char *) &msg->src, sizeof(msg->src))
        .read((char *) &msg->dst, sizeof(msg->dst))
        .read((char *) &msg->index, sizeof(msg->index))
        .read((char *) &msg->index_type, sizeof(msg->index_type))
        .read((char *) &msg->index_name, sizeof(msg->index_name))?MDHIM_SUCCESS:MDHIM_ERROR;
}

int ThalliumUnpacker::unpack(TransportRequestMessage *req, std::stringstream &s) {
    if (unpack(static_cast<TransportMessage *>(req), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportResponseMessage *res, std::stringstream &s) {
    if (unpack(static_cast<TransportMessage *>(res), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int ThalliumUnpacker::unpack(TransportRequestMessage  **req, const std::string &buf, const TransportMessageType mtype) {
    int ret = MDHIM_ERROR;
    switch (mtype) {
        case TransportMessageType::PUT:
            {
                TransportPutMessage *put = nullptr;
                ret = unpack(&put, buf);
                *req = put;
            }
            break;
        case TransportMessageType::BPUT:
            {
                TransportBPutMessage *bput = nullptr;
                ret = unpack(&bput, buf);
                *req = bput;
            }
            break;
        case TransportMessageType::GET:
            {
                TransportGetMessage *get = nullptr;
                ret = unpack(&get, buf);
                *req = get;
            }
            break;
        case TransportMessageType::BGET:
            {
                TransportBGetMessage *bget = nullptr;
                ret = unpack(&bget, buf);
                *req = bget;
            }
            break;
        case TransportMessageType::DELETE:
            {
                TransportDeleteMessage *dm = nullptr;
                ret = unpack(&dm, buf);
                *req = dm;
            }
            break;
        case TransportMessageType::BDELETE:
            {
                TransportBDeleteMessage *bdm = nullptr;
                ret = unpack(&bdm, buf);
                *req = bdm;
            }
            break;
        default:
            break;
    }

    return ret;
}

int ThalliumUnpacker::unpack(TransportResponseMessage **res, const std::string &buf, const TransportMessageType mtype) {
    int ret = MDHIM_ERROR;
    switch (mtype) {
        case TransportMessageType::RECV:
            {
                TransportRecvMessage *rm = nullptr;
                ret = unpack(&rm, buf);
                *res = rm;
            }
            break;
        case TransportMessageType::RECV_GET:
            {
                TransportGetRecvMessage *grm = nullptr;
                ret = unpack(&grm, buf);
                *res = grm;
            }
            break;
        case TransportMessageType::RECV_BGET:
            {
                TransportBGetRecvMessage *bgrm = nullptr;
                ret = unpack(&bgrm, buf);
                *res = bgrm;
            }
            break;
        case TransportMessageType::RECV_BULK:
            {
                TransportBRecvMessage *brm = nullptr;
                ret = unpack(&brm, buf);
                *res = brm;
            }
            break;
        default:
            break;
    }

    return ret;

}
