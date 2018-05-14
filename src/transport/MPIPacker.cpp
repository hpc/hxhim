#include "MPIPacker.hpp"

int MPIPacker::any(const MPI_Comm comm, const TransportMessage *msg, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    if (!msg) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    switch (msg->mtype) {
        case TransportMessageType::PUT:
            ret = pack(comm, dynamic_cast<const TransportPutMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::BPUT:
            ret = pack(comm, dynamic_cast<const TransportBPutMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::GET:
            ret = pack(comm, dynamic_cast<const TransportGetMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::BGET:
            ret = pack(comm, dynamic_cast<const TransportBGetMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::DELETE:
            ret = pack(comm, dynamic_cast<const TransportDeleteMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::BDELETE:
            ret = pack(comm, dynamic_cast<const TransportBDeleteMessage *>(msg), buf, bufsize, fbp);
            break;
        // close meesages are not sent across the network
        // case TransportMessageType::CLOSE:
        //     break;
        case TransportMessageType::RECV:
            ret = pack(comm, dynamic_cast<const TransportRecvMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::RECV_GET:
            ret = pack(comm, dynamic_cast<const TransportGetRecvMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::RECV_BGET:
            ret = pack(comm, dynamic_cast<const TransportBGetRecvMessage *>(msg), buf, bufsize, fbp);
            break;
        case TransportMessageType::RECV_BULK:
            ret = pack(comm, dynamic_cast<const TransportBRecvMessage *>(msg), buf, bufsize, fbp);
            break;
        // commit messages are not sent across the network
        // case TransportMessageType::COMMIT:
        //     break;
        default:
            break;
    }

    return ret;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportPutMessage *pm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(pm), buf, bufsize, &position, fbp)         != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&pm->rs_idx, sizeof(pm->rs_idx), MPI_CHAR, *buf, *bufsize, &position, comm)       != MPI_SUCCESS) ||
        (MPI_Pack(&pm->key_len, sizeof(pm->key_len), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&pm->value_len, sizeof(pm->value_len), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(pm->key, pm->key_len, MPI_CHAR, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
        (MPI_Pack(pm->value, pm->value_len, MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportBPutMessage *bpm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(bpm), buf, bufsize, &position, fbp)                      != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (MPI_Pack(&bpm->num_keys, sizeof(bpm->num_keys), MPI_CHAR, *buf, *bufsize, &position, comm)                != MPI_SUCCESS) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bpm->num_keys; i++) {
        if ((MPI_Pack(&bpm->rs_idx[i], sizeof(bpm->rs_idx[i]), MPI_CHAR, *buf, *bufsize, &position, comm)         != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->key_lens[i], sizeof(bpm->key_lens[i]), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(bpm->keys[i], bpm->key_lens[i], MPI_CHAR, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->value_lens[i], sizeof(bpm->value_lens[i]), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(bpm->values[i], bpm->value_lens[i], MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, fbp);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportGetMessage *gm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(gm), buf, bufsize, &position, fbp)       != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&gm->op, sizeof(gm->op), MPI_CHAR, *buf, *bufsize, &position, comm)             != MPI_SUCCESS) ||
        (MPI_Pack(&gm->num_keys, sizeof(gm->num_keys), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&gm->rs_idx, sizeof(gm->rs_idx), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&gm->key_len, sizeof(gm->key_len), MPI_CHAR, *buf, *bufsize, &position, comm)   != MPI_SUCCESS) ||
        (MPI_Pack(gm->key, gm->key_len, MPI_CHAR, *buf, *bufsize, &position, comm)                != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportBGetMessage *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(bgm), buf, bufsize, &position, fbp)                  != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&bgm->op, sizeof(bgm->op), MPI_CHAR, *buf, *bufsize, &position, comm)                       != MPI_SUCCESS) ||
        (MPI_Pack(&bgm->num_keys, sizeof(bgm->num_keys), MPI_CHAR, *buf, *bufsize, &position, comm)           != MPI_SUCCESS) ||
        (MPI_Pack(&bgm->num_recs, sizeof(bgm->num_recs), MPI_CHAR, *buf, *bufsize, &position, comm)           != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bgm->num_keys; i++) {
        if ((MPI_Pack(&bgm->rs_idx[i], sizeof(bgm->rs_idx[i]), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->key_lens[i], sizeof(bgm->key_lens[i]), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(bgm->keys[i], bgm->key_lens[i], MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, fbp);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportDeleteMessage *dm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(dm), buf, bufsize, &position, fbp)     != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&dm->rs_idx, sizeof(dm->rs_idx), MPI_CHAR, *buf, *bufsize, &position, comm)   != MPI_SUCCESS) ||
        (MPI_Pack(&dm->key_len, sizeof(dm->key_len), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(dm->key, dm->key_len, MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportBDeleteMessage *bdm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(bdm), buf, bufsize, &position, fbp)                  != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (MPI_Pack(&bdm->num_keys, sizeof(bdm->num_keys), MPI_CHAR, *buf, *bufsize, &position, comm)           != MPI_SUCCESS) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bdm->num_keys; i++) {
        if ((MPI_Pack(&bdm->rs_idx[i], sizeof(bdm->rs_idx[i]), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bdm->key_lens[i], sizeof(bdm->key_lens[i]), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(bdm->keys[i], bdm->key_lens[i], MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, fbp);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportRecvMessage *rm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(rm), buf, bufsize, &position, fbp)   != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&rm->rs_idx, sizeof(rm->rs_idx), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&rm->error, sizeof(rm->error), MPI_CHAR, *buf, *bufsize, &position, comm)   != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportGetRecvMessage *grm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(grm), buf, bufsize, &position, fbp)          != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&grm->rs_idx, sizeof(grm->rs_idx), MPI_CHAR, *buf, *bufsize, &position, comm)       != MPI_SUCCESS) ||
        (MPI_Pack(&grm->error, sizeof(grm->error), MPI_CHAR, *buf, *bufsize, &position, comm)         != MPI_SUCCESS) ||
        (MPI_Pack(&grm->key_len, sizeof(grm->key_len), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&grm->value_len, sizeof(grm->value_len), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(grm->key, grm->key_len, MPI_CHAR, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
        (MPI_Pack(grm->value, grm->value_len, MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportBGetRecvMessage *bgrm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(bgrm), buf, bufsize, &position, fbp)                       != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&bgrm->error, sizeof(bgrm->error), MPI_CHAR, *buf, *bufsize, &position, comm)                     != MPI_SUCCESS) ||
        (MPI_Pack(&bgrm->num_keys, sizeof(bgrm->num_keys), MPI_CHAR, *buf, *bufsize, &position, comm)               != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bgrm->num_keys; i++) {
        if ((MPI_Pack(&bgrm->rs_idx[i], sizeof(bgrm->rs_idx[i]), MPI_CHAR, *buf, *bufsize, &position, comm)         != MPI_SUCCESS) ||
            (MPI_Pack(&bgrm->key_lens[i], sizeof(bgrm->key_lens[i]), MPI_CHAR, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(bgrm->keys[i], bgrm->key_lens[i], MPI_CHAR, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(&bgrm->value_lens[i], sizeof(bgrm->value_lens[i]), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(bgrm->values[i], bgrm->value_lens[i], MPI_CHAR, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, fbp);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportBRecvMessage *brm, void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    int position = 0;
    if (pack(comm, static_cast<const TransportMessage *>(brm), buf, bufsize, &position, fbp)             != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if ((MPI_Pack(&brm->error, sizeof(brm->error), MPI_CHAR, *buf, *bufsize, &position, comm)            != MPI_SUCCESS) ||
        (MPI_Pack(&brm->num_keys, sizeof(brm->num_keys), MPI_CHAR, *buf, *bufsize, &position, comm)      != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < brm->num_keys; i++) {
        if (MPI_Pack(&brm->rs_idx[i], sizeof(brm->rs_idx[i]), MPI_CHAR, *buf, *bufsize, &position, comm) != MPI_SUCCESS) {
            cleanup(buf, bufsize, fbp);
            return MDHIM_ERROR;
        }
    }

    return MDHIM_SUCCESS;
}

int MPIPacker::pack(const MPI_Comm comm, const TransportMessage *msg, void **buf, std::size_t *bufsize, int *position, FixedBufferPool *fbp) {
    // *bufsize should have been set
    if (!msg || !buf || !bufsize || !position) {
        return MDHIM_ERROR;
    }


    *bufsize = msg->size() + sizeof(*bufsize);

    // Is the computed message size greater than the max message size?
    if (*bufsize > MDHIM_MAX_MSG_SIZE) {
        return MDHIM_ERROR;
    }

    // Allocate the buffer
    if (!(*buf = fbp->acquire(*bufsize))) {
        return MDHIM_ERROR;
    }

    // Pack the comment fields
    if ((MPI_Pack(&msg->mtype, sizeof(msg->mtype), MPI_CHAR, *buf, *bufsize, position, comm)           != MPI_SUCCESS) ||
        (MPI_Pack(bufsize, sizeof(*bufsize), MPI_CHAR, *buf, *bufsize, position, comm)                 != MPI_SUCCESS) ||
        (MPI_Pack(&msg->src, sizeof(msg->src), MPI_CHAR, *buf, *bufsize, position, comm)               != MPI_SUCCESS) ||
        (MPI_Pack(&msg->dst, sizeof(msg->dst), MPI_CHAR, *buf, *bufsize, position, comm)               != MPI_SUCCESS) ||
        (MPI_Pack(&msg->index, sizeof(msg->index), MPI_CHAR, *buf, *bufsize, position, comm)           != MPI_SUCCESS) ||
        (MPI_Pack(&msg->index_type, sizeof(msg->index_type), MPI_CHAR, *buf, *bufsize, position, comm) != MPI_SUCCESS) ||
        // intentional error
        (MPI_Pack(&msg->index_name, sizeof(msg->index_name), MPI_CHAR, *buf, *bufsize, position, comm) != MPI_SUCCESS)) {
        cleanup(buf, bufsize, fbp);
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

void MPIPacker::cleanup(void **buf, std::size_t *bufsize, FixedBufferPool *fbp) {
    if (buf) {
        fbp->release(*buf);
        *buf = nullptr;
    }

    if (bufsize) {
        *bufsize = 0;
    }
}
