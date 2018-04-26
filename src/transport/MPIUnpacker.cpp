#include "MPIUnpacker.hpp"

int MPIUnpacker::any(const MPI_Comm comm, TransportMessage **msg, const void *buf, const std::size_t bufsize) {
    if (!msg) {
        return MDHIM_ERROR;
    }

    TransportMessage *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    switch (basemsg->mtype) {
        case TransportMessageType::PUT:
            {
                TransportPutMessage *put = nullptr;
                ret = unpack(comm, &put, buf, bufsize);
                *msg = put;
            }
            break;
        case TransportMessageType::BPUT:
            {
                TransportBPutMessage *bput = nullptr;
                ret = unpack(comm, &bput, buf, bufsize);
                *msg = bput;
            }
            break;
        case TransportMessageType::GET:
            {
                TransportGetMessage *get = nullptr;
                ret = unpack(comm, &get, buf, bufsize);
                *msg = get;
            }
            break;
        case TransportMessageType::BGET:
            {
                TransportBGetMessage *bget = nullptr;
                ret = unpack(comm, &bget, buf, bufsize);
                *msg = bget;
            }
            break;
        case TransportMessageType::DELETE:
            {
                TransportDeleteMessage *dm = nullptr;
                ret = unpack(comm, &dm, buf, bufsize);
                *msg = dm;
            }
            break;
        case TransportMessageType::BDELETE:
            {
                TransportBDeleteMessage *bdm = nullptr;
                ret = unpack(comm, &bdm, buf, bufsize);
                *msg = bdm;
            }
            break;
        // close meesages are not sent across the network
        // case TransportMessageType::CLOSE:
        //     break;
        case TransportMessageType::RECV:
            {
                TransportRecvMessage *rm = nullptr;
                ret = unpack(comm, &rm, buf, bufsize);
                *msg = rm;
            }
            break;
        case TransportMessageType::RECV_GET:
            {
                TransportGetRecvMessage *grm = nullptr;
                ret = unpack(comm, &grm, buf, bufsize);
                *msg = grm;
            }
            break;
        case TransportMessageType::RECV_BGET:
            {
                TransportBGetRecvMessage *bgrm = nullptr;
                ret = unpack(comm, &bgrm, buf, bufsize);
                *msg = bgrm;
            }
            break;
        // commit messages are not sent across the network
        // case TransportMessageType::COMMIT:
        //     break;
        default:
            break;
    }

    delete basemsg;

    return ret;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportPutMessage **pm, const void *buf, const std::size_t bufsize) {
    if (!pm || !buf) {
        return MDHIM_ERROR;
    }

    TransportPutMessage *out = new TransportPutMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->value_len, sizeof(out->value_len), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->key_len) {
        if (!(out->key = ::operator new (out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    if (out->value_len) {
        if (!(out->value = ::operator new(out->value_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->value, out->value_len, MPI_CHAR, comm) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *pm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBPutMessage **bpm, const void *buf, const std::size_t bufsize) {
    if (!bpm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBPutMessage *out = new TransportBPutMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->value_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if ((MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys * sizeof(*out->key_lens), MPI_CHAR, comm)   != MPI_SUCCESS) ||
            (MPI_Unpack(buf, bufsize, &position, out->value_lens, out->num_keys * sizeof(*out->value_lens), MPI_CHAR, comm) != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = new void *[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->values = new void *[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = ::operator new(out->key_lens[i]))     ||
                !(out->values[i] = ::operator new(out->value_lens[i])) ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, out->values[i], out->value_lens[i], MPI_CHAR, comm) != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bpm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportGetMessage **gm, const void *buf, const std::size_t bufsize) {
    if (!gm || !buf) {
        return MDHIM_ERROR;
    }

    TransportGetMessage *out = new TransportGetMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->op, sizeof(out->op), MPI_CHAR, comm)             != MPI_SUCCESS) ||
        // not sure if out->num_keys is used/set
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm)   != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there is a key, allocate space for it and unpack
    if (out->key_len) {
        if (!(out->key = ::operator new(out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm) != MPI_SUCCESS) {
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
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->op, sizeof(out->op), MPI_CHAR, comm)             != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_recs, sizeof(out->num_recs), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = new void *[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys * sizeof(*out->key_lens), MPI_CHAR, comm) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = ::operator new(out->key_lens[i])) ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm) != MPI_SUCCESS)) {
                return MDHIM_ERROR;
            }
        }
    }

    *bgm = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportDeleteMessage **dm, const void *buf, const std::size_t bufsize) {
    if (!dm || !buf) {
        return MDHIM_ERROR;
    }

    TransportDeleteMessage *out = new TransportDeleteMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there is a key, allocate space for it and unpack
    if (out->key_len) {
        if (!(out->key = ::operator new(out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *dm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBDeleteMessage **bdm, const void *buf, const std::size_t bufsize) {
    if (!bdm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBDeleteMessage *out = new TransportBDeleteMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys * sizeof(*out->key_lens), MPI_CHAR, comm) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = new void *[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = ::operator new(out->key_lens[i]))     ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm) != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bdm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportRecvMessage **rm, const void *buf, const std::size_t bufsize) {
    if (!rm || !buf) {
        return MDHIM_ERROR;
    }

    TransportRecvMessage *out = new TransportRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    *rm = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportGetRecvMessage **grm, const void *buf, const std::size_t bufsize) {
    if (!grm || !buf) {
        return MDHIM_ERROR;
    }

    TransportGetRecvMessage *out = new TransportGetRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm)         != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->value_len, sizeof(out->value_len), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->key_len) {
        if (!(out->key = ::operator new(out->key_len)) ||
            (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, comm) != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    if (out->value_len) {
        if (!(out->value = ::operator new(out->value_len)) ||
            (MPI_Unpack(buf, bufsize, &position, out->value, out->value_len, MPI_CHAR, comm) != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }

    }

    *grm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportBGetRecvMessage **bgrm, const void *buf, const std::size_t bufsize) {
    if (!bgrm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBGetRecvMessage *out = new TransportBGetRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, comm)       != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->value_lens = new std::size_t[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if ((MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys * sizeof(*out->key_lens), MPI_CHAR, comm)   != MPI_SUCCESS) ||
            (MPI_Unpack(buf, bufsize, &position, out->value_lens, out->num_keys * sizeof(*out->value_lens), MPI_CHAR, comm) != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = new void *[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->values = new void *[out->num_keys]())) {
            delete out;
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = ::operator new(out->key_lens[i])) ||
                !(out->values[i] = ::operator new(out->value_lens[i])) ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, out->values[i], out->value_lens[i], MPI_CHAR, comm) != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bgrm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPI_Comm comm, TransportMessage **msg, const void *buf, const std::size_t bufsize) {
    if (!msg || !buf) {
        return MDHIM_ERROR;
    }

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

    std::size_t givensize = 0;
    if ((MPI_Unpack(buf, bufsize, position, &msg->mtype, sizeof(msg->mtype), MPI_CHAR, comm)           != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &givensize, sizeof(givensize), MPI_CHAR, comm)             != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->src, sizeof(msg->src), MPI_CHAR, comm)               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->dst, sizeof(msg->dst), MPI_CHAR, comm)               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->index, sizeof(msg->index), MPI_CHAR, comm)           != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->index_type, sizeof(msg->index_type), MPI_CHAR, comm) != MPI_SUCCESS) ||
        // intentional error
        (MPI_Unpack(buf, bufsize, position, &msg->index_name, sizeof(msg->index_name), MPI_CHAR, comm) != MPI_SUCCESS)) {
        return MDHIM_ERROR;
    }

    return (givensize == bufsize)?MDHIM_SUCCESS:MDHIM_ERROR;
}
