#include "MPIPacker.hpp"

int MPIPacker::any (const MPITransportBase *transportbase, const TransportMessage *msg, void **buf, int *bufsize) {
    int ret = MDHIM_ERROR;
    switch (msg->mtype) {
        case TransportMessageType::PUT:
            ret = pack(transportbase, dynamic_cast<const TransportPutMessage *>(msg), buf, bufsize);
            break;
        case TransportMessageType::BPUT:
            ret = pack(transportbase, dynamic_cast<const TransportBPutMessage *>(msg), buf, bufsize);
            break;
        case TransportMessageType::BGET:
            ret = pack(transportbase, dynamic_cast<const TransportBGetMessage *>(msg), buf, bufsize);
            break;
        // case TransportMessageType::DELETE:
        //     break;
        // case TransportMessageType::BDELETE:
        //     break;
        // case TransportMessageType::CLOSE:
        //     break;
        case TransportMessageType::RECV:
            ret = pack(transportbase, dynamic_cast<const TransportRecvMessage *>(msg), buf, bufsize);
            break;
        // case TransportMessageType::RECV_GET:
        //     break;
        case TransportMessageType::RECV_BGET:
            ret = pack(transportbase, dynamic_cast<const TransportBGetRecvMessage *>(msg), buf, bufsize);
            break;
        // case TransportMessageType::COMMIT:
        //     break;
        default:
            break;
    }

    return ret;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportPutMessage *pm, void **buf, int *bufsize) {
    *bufsize = pm->size() + sizeof(*bufsize);
    int position = 0;
    if (pack(transportbase, static_cast<const TransportMessage*>(pm), buf, bufsize, &position) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&pm->key_len, sizeof(pm->key_len), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())     != MPI_SUCCESS) ||
        (MPI_Pack(&pm->value_len, sizeof(pm->value_len), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Pack(pm->key, pm->key_len, MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())                  != MPI_SUCCESS) ||
        (MPI_Pack(pm->value, pm->value_len, MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())              != MPI_SUCCESS)) {
        cleanup(buf, bufsize);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportBPutMessage *bpm, void **buf, int *bufsize) {
    throw;
    return MDHIM_ERROR;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportGetMessage *gm, void **buf, int *bufsize) {
    throw;
    return MDHIM_ERROR;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportBGetMessage *bgm, void **buf, int *bufsize) {
    *bufsize = bgm->size() + sizeof(*bufsize);
    int position = 0;
    if (pack(transportbase, static_cast<const TransportMessage*>(bgm), buf, bufsize, &position) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&bgm->op, sizeof(bgm->op), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())             != MPI_SUCCESS) ||
        (MPI_Pack(&bgm->num_keys, sizeof(bgm->num_keys), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Pack(&bgm->num_recs, sizeof(bgm->num_recs), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Pack(bgm->key_lens, bgm->num_keys, MPI_INT, *buf, *bufsize, &position, transportbase->Comm())           != MPI_SUCCESS)) {
        cleanup(buf, bufsize);
        return MDHIM_ERROR;
    }

    for(int i = 0; i < bgm->num_keys; i++) {
        if (MPI_Pack(bgm->keys[i], bgm->key_lens[i], MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm()) != MPI_SUCCESS) {
            cleanup(buf, bufsize);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportDeleteMessage *dm, void **buf, int *bufsize) {
    throw;
    return MDHIM_ERROR;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportBDeleteMessage *bdm, void **buf, int *bufsize) {
    throw;
    return MDHIM_ERROR;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportRecvMessage *rm, void **buf, int *bufsize) {
    *bufsize = rm->size() + sizeof(*bufsize);
    int position = 0;
    if (pack(transportbase, static_cast<const TransportMessage*>(rm), buf, bufsize, &position) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (MPI_Pack(&rm->error, sizeof(rm->error), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm()) != MPI_SUCCESS) {
        cleanup(buf, bufsize);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportBGetRecvMessage *bgrm, void **buf, int *bufsize) {
    *bufsize = bgrm->size() + sizeof(*bufsize);
    int position = 0;
    if (pack(transportbase, static_cast<const TransportMessage*>(bgrm), buf, bufsize, &position) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&bgrm->error, sizeof(bgrm->error), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())        != MPI_SUCCESS) ||
        (MPI_Pack(&bgrm->num_keys, sizeof(bgrm->num_keys), MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())  != MPI_SUCCESS) ||
        (MPI_Pack(bgrm->key_lens, bgrm->num_keys, MPI_INT, *buf, *bufsize, &position, transportbase->Comm())            != MPI_SUCCESS) ||
        (MPI_Pack(bgrm->value_lens, bgrm->num_keys, MPI_INT, *buf, *bufsize, &position, transportbase->Comm())          != MPI_SUCCESS)) {
        cleanup(buf, bufsize);
        return MDHIM_ERROR;
    }

    for(int i = 0; i < bgrm->num_keys; i++) {
        if ((MPI_Pack(bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm())     != MPI_SUCCESS) ||
            (MPI_Pack(bgrm->values[i], bgrm->value_lens[i], MPI_CHAR, *buf, *bufsize, &position, transportbase->Comm()) != MPI_SUCCESS)) {
            cleanup(buf, bufsize);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportBRecvMessage *brm, void **buf, int *bufsize) {
    throw;
    return MDHIM_ERROR;
}

int MPIPacker::pack(const MPITransportBase *transportbase, const TransportMessage *msg, void **buf, int *bufsize, int *position) {
    // *bufsize should have been set
    if (!msg || !buf || !bufsize || !position) {
        return MDHIM_ERROR;
    }

    // Is the computed message size of a safe value? (less than a max message size?)
    if (*bufsize > MDHIM_MAX_MSG_SIZE) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - Error: put message too large."
             " Put is over Maximum size allowed of %d.", transportbase->Rank(), MDHIM_MAX_MSG_SIZE);
        return MDHIM_ERROR;
    }

    // Allocate the buffer
    if (!(*buf = malloc(*bufsize))) {
        return MDHIM_ERROR;
    }

    // Pack the comment fields
    if ((MPI_Pack(&msg->mtype, sizeof(msg->mtype), MPI_CHAR, *buf, *bufsize, position, transportbase->Comm())             != MPI_SUCCESS) ||
        (MPI_Pack(bufsize, sizeof(*bufsize), MPI_CHAR, *buf, *bufsize, position, transportbase->Comm())                   != MPI_SUCCESS) ||
        (MPI_Pack(&msg->server_rank, sizeof(msg->server_rank), MPI_CHAR, *buf, *bufsize, position, transportbase->Comm()) != MPI_SUCCESS) ||
        (MPI_Pack(&msg->index, sizeof(msg->index), MPI_CHAR, *buf, *bufsize, position, transportbase->Comm())             != MPI_SUCCESS) ||
        (MPI_Pack(&msg->index_type, sizeof(msg->index_type), MPI_CHAR, *buf, *bufsize, position, transportbase->Comm())   != MPI_SUCCESS) ||
        // intentional error
        (MPI_Pack(&msg->index_name, sizeof(msg->index_name), MPI_CHAR, *buf, *bufsize, position, transportbase->Comm())   != MPI_SUCCESS)) {
        cleanup(buf, bufsize);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

void MPIPacker::cleanup(void **buf, int *bufsize) {
    if (buf) {
        free(*buf);
        *buf = nullptr;
    }

    if (bufsize) {
        *bufsize = 0;
    }
}
