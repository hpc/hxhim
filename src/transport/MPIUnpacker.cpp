#include "MPIUnpacker.hpp"

int MPIUnpacker::any(const MPITransportBase *transportbase, TransportMessage **msg, const void *buf, const int bufsize) {
    TransportMessage *basemsg = nullptr;
    if (unpack(transportbase, &basemsg, buf, bufsize) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    switch (basemsg->mtype) {
        case TransportMessageType::PUT:
            {
                TransportPutMessage *put = nullptr;
                ret = unpack(transportbase, &put, buf, bufsize);
                *msg = put;
            }
            break;
        case TransportMessageType::BPUT:
            {
                TransportBPutMessage *bput = nullptr;
                ret = unpack(transportbase, &bput, buf, bufsize);
                *msg = bput;
            }
            break;
        case TransportMessageType::BGET:
            {
                TransportBGetMessage *bget = nullptr;
                ret = unpack(transportbase, &bget, buf, bufsize);
                *msg = bget;
            }
            break;
        case TransportMessageType::DELETE:
            {
                TransportDeleteMessage *dm = nullptr;
                ret = unpack(transportbase, &dm, buf, bufsize);
                *msg = dm;
            }
            break;
        case TransportMessageType::BDELETE:
            {
                TransportBDeleteMessage *bdm = nullptr;
                ret = unpack(transportbase, &bdm, buf, bufsize);
                *msg = bdm;
            }
            break;
        // close meesages are not sent across the network
        // case TransportMessageType::CLOSE:
        //     break;
        case TransportMessageType::RECV:
            {
                TransportRecvMessage *rm = nullptr;
                ret = unpack(transportbase, &rm, buf, bufsize);
                *msg = rm;
            }
            break;
        // case TransportMessageType::RECV_GET:
        //     break;
        case TransportMessageType::RECV_BGET:
            {
                TransportBGetRecvMessage *bgrm = nullptr;
                ret = unpack(transportbase, &bgrm, buf, bufsize);
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

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportPutMessage **pm, const void *buf, const int bufsize) {
    if (!pm || !buf) {
        return MDHIM_ERROR;
    }

    TransportPutMessage *out = new TransportPutMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, transportbase->Comm())     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->value_len, sizeof(out->value_len), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    if (out->key_len) {
        if (!(out->key = malloc(out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    if (out->value_len) {
        if (!(out->value = malloc(out->value_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->value, out->value_len, MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *pm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportBPutMessage **bpm, const void *buf, const int bufsize) {
    if (!bpm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBPutMessage *out = new TransportBPutMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = (int *)malloc(out->num_keys * sizeof(int)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->value_lens = (int *)malloc(out->num_keys * sizeof(int)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if ((MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys, MPI_INT, transportbase->Comm())   != MPI_SUCCESS) ||
            (MPI_Unpack(buf, bufsize, &position, out->value_lens, out->num_keys, MPI_INT, transportbase->Comm()) != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = (void **)malloc(out->num_keys * sizeof(void *)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->values = (void **)malloc(out->num_keys * sizeof(void *)))) {
            delete out;
            return MDHIM_ERROR;
        }

        for(int i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = malloc(out->key_lens[i]))     ||
                !(out->values[i] = malloc(out->value_lens[i])) ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, transportbase->Comm())     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, out->values[i], out->value_lens[i], MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bpm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportGetMessage **gm, const void *buf, const int bufsize) {
    if (!gm || !buf) {
        return MDHIM_ERROR;
    }

    TransportGetMessage *out = new TransportGetMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->op, sizeof(out->op), MPI_CHAR, transportbase->Comm())             != MPI_SUCCESS) ||
        // not sure if out->num_keys is used/set
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, transportbase->Comm())   != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there is a key, allocate space for it and unpack
    if (out->key_len) {
        if (!(out->key = malloc(out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *gm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportBGetMessage **bgm, const void *buf, const int bufsize) {
    if (!bgm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBGetMessage *out = new TransportBGetMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->op, sizeof(out->op), MPI_CHAR, transportbase->Comm())             != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_recs, sizeof(out->num_recs), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = (int *)malloc(out->num_keys * sizeof(int)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = (void **)malloc(out->num_keys * sizeof(void *)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys, MPI_INT, transportbase->Comm()) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }

        for(int i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = malloc(out->key_lens[i])) ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
                return MDHIM_ERROR;
            }
        }
    }

    *bgm = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportDeleteMessage **dm, const void *buf, const int bufsize) {
    if (!dm || !buf) {
        return MDHIM_ERROR;
    }

    TransportDeleteMessage *out = new TransportDeleteMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->key_len, sizeof(out->key_len), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there is a key, allocate space for it and unpack
    if (out->key_len) {
        if (!(out->key = malloc(out->key_len))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key, out->key_len, MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }
    }

    *dm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportBDeleteMessage **bdm, const void *buf, const int bufsize) {
    if (!bdm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBDeleteMessage *out = new TransportBDeleteMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = (int *)malloc(out->num_keys * sizeof(int)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys, MPI_INT, transportbase->Comm()) != MPI_SUCCESS) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = (void **)malloc(out->num_keys * sizeof(void *)))) {
            delete out;
            return MDHIM_ERROR;
        }

        for(int i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = malloc(out->key_lens[i]))     ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bdm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportRecvMessage **rm, const void *buf, const int bufsize) {
    if (!rm || !buf) {
        return MDHIM_ERROR;
    }

    TransportRecvMessage *out = new TransportRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    *rm = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportBGetRecvMessage **bgrm, const void *buf, const int bufsize) {
    if (!bgrm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBGetRecvMessage *out = new TransportBGetRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, transportbase->Comm())       != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->num_keys, sizeof(out->num_keys), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
        delete out;
        return MDHIM_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (out->num_keys) {
        if (!(out->key_lens = (int *)malloc(out->num_keys * sizeof(int)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->value_lens = (int *)malloc(out->num_keys * sizeof(int)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if ((MPI_Unpack(buf, bufsize, &position, out->key_lens, out->num_keys, MPI_INT, transportbase->Comm())   != MPI_SUCCESS) ||
            (MPI_Unpack(buf, bufsize, &position, out->value_lens, out->num_keys, MPI_INT, transportbase->Comm()) != MPI_SUCCESS)) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->keys = (void **)malloc(out->num_keys * sizeof(void *)))) {
            delete out;
            return MDHIM_ERROR;
        }

        if (!(out->values = (void **)malloc(out->num_keys * sizeof(void *)))) {
            delete out;
            return MDHIM_ERROR;
        }

        for(int i = 0; i < out->num_keys; i++) {
            if (!(out->keys[i] = malloc(out->key_lens[i])) ||
                !(out->values[i] = malloc(out->value_lens[i])) ||
                (MPI_Unpack(buf, bufsize, &position, out->keys[i], out->key_lens[i], MPI_CHAR, transportbase->Comm())     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, out->values[i], out->value_lens[i], MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS)) {
                delete out;
                return MDHIM_ERROR;
            }
        }
    }

    *bgrm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportBRecvMessage **brm, const void *buf, const int bufsize) {
    if (!brm || !buf) {
        return MDHIM_ERROR;
    }

    TransportBRecvMessage *out = new TransportBRecvMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, static_cast<TransportMessage *>(out), buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->error, sizeof(out->error), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    *brm = out;

    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportMessage **msg, const void *buf, const int bufsize) {
    if (!msg || !buf) {
        return MDHIM_ERROR;
    }

    TransportMessage *out = new TransportMessage();
    if (!out) {
        return MDHIM_ERROR;
    }

    int position = 0;
    if (unpack(transportbase, out, buf, bufsize, &position) != MDHIM_SUCCESS) {
        delete out;
        return MDHIM_ERROR;
    }

    *msg = out;
    return MDHIM_SUCCESS;
}

int MPIUnpacker::unpack(const MPITransportBase *transportbase, TransportMessage *msg, const void *buf, const int bufsize, int *position) {
    if (!msg || !buf || !position) {
        return MDHIM_ERROR;
    }

    int givensize = 0;
    if ((MPI_Unpack(buf, bufsize, position, &msg->mtype, sizeof(msg->mtype), MPI_CHAR, transportbase->Comm())             != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &givensize, sizeof(givensize), MPI_CHAR, transportbase->Comm())               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->server_rank, sizeof(msg->server_rank), MPI_CHAR, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->index, sizeof(msg->index), MPI_CHAR, transportbase->Comm())             != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->index_type, sizeof(msg->index_type), MPI_CHAR, transportbase->Comm())   != MPI_SUCCESS) ||
        // intentional error
        (MPI_Unpack(buf, bufsize, position, &msg->index_name, sizeof(msg->index_name), MPI_CHAR, transportbase->Comm())   != MPI_SUCCESS)) {
        return MDHIM_ERROR;
    }

    return (givensize == bufsize)?MDHIM_SUCCESS:MDHIM_ERROR;
}
