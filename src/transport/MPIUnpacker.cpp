#include "MPIUnpacker.hpp"

int MPIUnpacker::any(const MPI_Comm comm, TransportMessage **msg, const void *buf, const std::size_t bufsize) {
    TransportMessage *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // try unpacking into a TransportRequestMessage
    TransportRequestMessage *req = nullptr;
    if (unpack(comm, &req, buf, bufsize, basemsg->mtype) == MDHIM_SUCCESS) {
        *msg = req;
        delete basemsg;
        return MDHIM_SUCCESS;
    }

    // try unpacking into a Transport Response
    TransportResponseMessage *res = nullptr;
    if (unpack(comm, &res, buf, bufsize, basemsg->mtype) == MDHIM_SUCCESS) {
        *msg = res;
        delete basemsg;
        return MDHIM_SUCCESS;
    }

    *msg = nullptr;
    delete basemsg;
    return MDHIM_ERROR;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportRequestMessage **req, const void *buf, const std::size_t bufsize) {
    TransportMessage *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    const int ret = unpack(comm, req, buf, bufsize, basemsg->mtype);

    delete basemsg;

    return ret;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportPutMessage **pm, const void *buf, const std::size_t bufsize) {
    TransportPutMessage *out = new TransportPutMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                   != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx, sizeof(out->rs_idx), MPI_CHAR, comm)       != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->value_len, sizeof(out->value_len), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->key_len) {
        if (!(out->key = ::operator new (out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm)               != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    if (out->value_len) {
        if (!(out->value = ::operator new(out->value_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->value, out->value_len, MPI_CHAR, comm)           != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *pm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBPutMessage **bpm, const void *buf, const std::size_t bufsize) {
    TransportBPutMessage *out = new TransportBPutMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                               != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm)                != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
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
            if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx[i], sizeof(out->rs_idx[i]), MPI_CHAR, comm)         != MPI_SUCCESS) ||
                // unpack the key
                (MPI_Unpack(buf, bufsize, &position, &out->key_lens[i], sizeof(out->key_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))                                                                    ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                // unpack the value
                (MPI_Unpack(buf, bufsize, &position, &out->value_lens[i], sizeof(out->value_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                !(out->values[i] = ::operator new(out->value_lens[i]))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->values[i], out->value_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bpm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportGetMessage **gm, const void *buf, const std::size_t bufsize) {
    TransportGetMessage *out = new TransportGetMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                 != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->op, sizeof(out->op), MPI_CHAR, comm)             != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->rs_idx, sizeof(out->rs_idx), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm)   != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there is a key, allocate space for it and unpack
    if (out->key_len) {
        if (!(out->key = ::operator new(out->key_len))                                                              ||
            (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm)            != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *gm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBGetMessage **bgm, const void *buf, const std::size_t bufsize) {
    if (!bgm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBGetMessage *out = new TransportBGetMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                               != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->op, sizeof(out->op), MPI_CHAR, comm)                           != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm)               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_recs, sizeof(out->num_recs), MPI_CHAR, comm)               != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())           ||
            !(out->keys = new void *[out->num_keys]())          ||
            !(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx[i], sizeof(out->rs_idx[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                // unpack the key
                (MPI_Unpack(buf, bufsize, &position, &out->key_lens[i], sizeof(out->key_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm) != MPI_SUCCESS))              {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bgm = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportDeleteMessage **dm, const void *buf, const std::size_t bufsize) {
    TransportDeleteMessage *out = new TransportDeleteMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)               != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx, sizeof(out->rs_idx), MPI_CHAR, comm)   != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there is a key, allocate space for it and unpack
    if (out->key_len) {
        if (!(out->key = ::operator new(out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm)           != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *dm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBDeleteMessage **bdm, const void *buf, const std::size_t bufsize) {
    TransportBDeleteMessage *out = new TransportBDeleteMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                               != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm)                != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())           ||
            !(out->keys = new void *[out->num_keys]())          ||
            !(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx[i], sizeof(out->rs_idx[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                // unpack the key
                (MPI_Unpack(buf, bufsize, &position, &out->key_lens[i], sizeof(out->key_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bdm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportResponseMessage **res, const void *buf, const std::size_t bufsize) {
    TransportMessage *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    const int ret = unpack(comm, res, buf, bufsize, basemsg->mtype);

    delete basemsg;

    return ret;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportRecvMessage **rm, const void *buf, const std::size_t bufsize) {
    TransportRecvMessage *out = new TransportRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)             != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx, sizeof(out->rs_idx), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm)   != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    *rm = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportGetRecvMessage **grm, const void *buf, const std::size_t bufsize) {
    TransportGetRecvMessage *out = new TransportGetRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                   != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx, sizeof(out->rs_idx), MPI_CHAR, comm)       != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm)         != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->value_len, sizeof(out->value_len), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->key_len) {
        if (!(out->key = ::operator new(out->key_len))                                                                ||
            (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm)              != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    if (out->value_len) {
        if (!(out->value = malloc(out->value_len))                                                                    ||
            (MPI_Unpack(buf, bufsize, &position, out->value, out->value_len, MPI_CHAR, comm)          != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }

    }

    *grm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBGetRecvMessage **bgrm, const void *buf, const std::size_t bufsize) {
    TransportBGetRecvMessage *out = new TransportBGetRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                                   != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm)                         != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm)                   != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
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
            if ((MPI_Unpack(buf, bufsize, &position, &out->rs_idx[i], sizeof(out->rs_idx[i]), MPI_CHAR, comm)         != MPI_SUCCESS) ||

                // unpack the key
                (MPI_Unpack(buf, bufsize, &position, &out->key_lens[i], sizeof(out->key_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                !(out->keys[i] = ::operator new(out->key_lens[i]))                                                                    ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||

                // unpack the value
                (MPI_Unpack(buf, bufsize, &position, &out->value_lens[i], sizeof(out->value_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                !(out->values[i] = malloc(out->value_lens[i]))                                                                        ||
                (MPI_Unpack(buf, bufsize, &position, out->values[i], out->value_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bgrm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBRecvMessage **brm, const void *buf, const std::size_t bufsize) {
    TransportBRecvMessage *out = new TransportBRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position)                      != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm)            != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm)      != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->num_keys) {
        if (!(out->rs_idx = new int[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }
        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (MPI_Unpack(buf, bufsize, &position, &out->rs_idx[i], sizeof(out->rs_idx[i]), MPI_CHAR, comm) != MPI_SUCCESS) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *brm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportMessage **msg, const void *buf, const std::size_t bufsize) {
    TransportMessage *out = new TransportMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, out, buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    *msg = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportMessage *msg, const void *buf, const std::size_t bufsize, int *position) {
    if (!msg || !buf || !position) {
        return MDHIM_ERROR;
    }

    msg->clean = true;

    std::size_t givensize = 0;
    if ((MPI_Unpack(buf, bufsize, position, &msg->mtype,      sizeof(msg->mtype),      MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &givensize,       sizeof(givensize),       MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->src,        sizeof(msg->src),        MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->dst,        sizeof(msg->dst),        MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->index,      sizeof(msg->index),      MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->index_type, sizeof(msg->index_type), MPI_CHAR, comm) != MPI_SUCCESS) ||
        // intentional error
        (MPI_Unpack(buf, bufsize, position, &msg->index_name, sizeof(msg->index_name), MPI_CHAR, comm) != MPI_SUCCESS)) {
        return MDHIM_ERROR;
    }

    return (givensize == bufsize)?MDHIM_SUCCESS:MDHIM_ERROR;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportRequestMessage *req, const void *buf, const std::size_t bufsize, int *position) {
    if (unpack(comm, static_cast<TransportMessage *>(req), buf, bufsize, position) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportResponseMessage *res, const void *buf, const std::size_t bufsize, int *position) {
    if (unpack(comm, static_cast<TransportMessage *>(res), buf, bufsize, position) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportRequestMessage **req, const void *buf, const std::size_t bufsize, const TransportMessageType mtype) {
    int ret = MDHIM_ERROR;
    switch (mtype) {
        case TransportMessageType::PUT:
            {
                TransportPutMessage *put = nullptr;
                ret = unpack(comm, &put, buf, bufsize);
                *req = put;
            }
            break;
        case TransportMessageType::BPUT:
            {
                TransportBPutMessage *bput = nullptr;
                ret = unpack(comm, &bput, buf, bufsize);
                *req = bput;
            }
            break;
        case TransportMessageType::GET:
            {
                TransportGetMessage *get = nullptr;
                ret = unpack(comm, &get, buf, bufsize);
                *req = get;
            }
            break;
        case TransportMessageType::BGET:
            {
                TransportBGetMessage *bget = nullptr;
                ret = unpack(comm, &bget, buf, bufsize);
                *req = bget;
            }
            break;
        case TransportMessageType::DELETE:
            {
                TransportDeleteMessage *dm = nullptr;
                ret = unpack(comm, &dm, buf, bufsize);
                *req = dm;
            }
            break;
        case TransportMessageType::BDELETE:
            {
                TransportBDeleteMessage *bdm = nullptr;
                ret = unpack(comm, &bdm, buf, bufsize);
                *req = bdm;
            }
            break;
        default:
            break;
    }

    return ret;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportResponseMessage **res, const void *buf, const std::size_t bufsize, const TransportMessageType mtype) {
    int ret = MDHIM_ERROR;
    switch (mtype) {
        case TransportMessageType::RECV:
            {
                TransportRecvMessage *rm = nullptr;
                ret = unpack(comm, &rm, buf, bufsize);
                *res = rm;
            }
            break;
        case TransportMessageType::RECV_GET:
            {
                TransportGetRecvMessage *grm = nullptr;
                ret = unpack(comm, &grm, buf, bufsize);
                *res = grm;
            }
            break;
        case TransportMessageType::RECV_BGET:
            {
                TransportBGetRecvMessage *bgrm = nullptr;
                ret = unpack(comm, &bgrm, buf, bufsize);
                *res = bgrm;
            }
            break;
        case TransportMessageType::RECV_BULK:
            {
                TransportBRecvMessage *brm = nullptr;
                ret = unpack(comm, &brm, buf, bufsize);
                *res = brm;
            }
            break;
        default:
            break;
    }

    return ret;
}
