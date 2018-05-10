#include "ThalliumPacker.hpp"

int ThalliumPacker::any(const TransportMessage *msg, std::string &buf) {
    if (!msg) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    switch (msg->mtype) {
        case TransportMessageType::PUT:
            ret = pack(dynamic_cast<const TransportPutMessage *>(msg), buf);
            break;
        case TransportMessageType::BPUT:
            ret = pack(dynamic_cast<const TransportBPutMessage *>(msg), buf);
            break;
        case TransportMessageType::GET:
            ret = pack(dynamic_cast<const TransportGetMessage *>(msg), buf);
            break;
        case TransportMessageType::BGET:
            ret = pack(dynamic_cast<const TransportBGetMessage *>(msg), buf);
            break;
        case TransportMessageType::DELETE:
            ret = pack(dynamic_cast<const TransportDeleteMessage *>(msg), buf);
            break;
        case TransportMessageType::BDELETE:
            ret = pack(dynamic_cast<const TransportBDeleteMessage *>(msg), buf);
            break;
        // close meesages are not sent across the network
        // case TransportMessageType::CLOSE:
        //     break;
        case TransportMessageType::RECV:
            ret = pack(dynamic_cast<const TransportRecvMessage *>(msg), buf);
            break;
        case TransportMessageType::RECV_GET:
            ret = pack(dynamic_cast<const TransportGetRecvMessage *>(msg), buf);
            break;
        case TransportMessageType::RECV_BGET:
            ret = pack(dynamic_cast<const TransportBGetRecvMessage *>(msg), buf);
            break;
        case TransportMessageType::RECV_BULK:
            ret = pack(dynamic_cast<const TransportBRecvMessage *>(msg), buf);
            break;
        // commit messages are not sent across the network
        // case TransportMessageType::COMMIT:
        //     break;
        default:
            break;
    }

    return ret;
}

int ThalliumPacker::pack(const TransportPutMessage *pm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(pm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &pm->rs_idx, sizeof(pm->rs_idx))
        .write((char *) &pm->key_len, sizeof(pm->key_len))
        .write((char *) &pm->value_len, sizeof(pm->value_len))
        .write((char *) pm->key, pm->key_len)
        .write((char *) pm->value, pm->value_len)) {
        return MDHIM_ERROR;
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportBPutMessage *bpm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(bpm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &bpm->num_keys, sizeof(bpm->num_keys))) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bpm->num_keys; i++) {
        if (!s
            .write((char *) &bpm->rs_idx[i], sizeof(bpm->rs_idx[i]))
            .write((char *) &bpm->key_lens[i], sizeof(bpm->key_lens[i]))
            .write((char *) bpm->keys[i], bpm->key_lens[i])
            .write((char *) &bpm->value_lens[i], sizeof(bpm->value_lens[i]))
            .write((char *) bpm->values[i], bpm->value_lens[i])) {
            return MDHIM_ERROR;
        }
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportGetMessage *gm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(gm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &gm->rs_idx, sizeof(gm->rs_idx))
        .write((char *) &gm->op, sizeof(gm->op))
        .write((char *) &gm->num_keys, sizeof(gm->num_keys))
        .write((char *) &gm->key_len, sizeof(gm->key_len))
        .write((char *) gm->key, gm->key_len)) {
        return MDHIM_ERROR;
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportBGetMessage *bgm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(bgm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &bgm->op, sizeof(bgm->op))
        .write((char *) &bgm->num_keys, sizeof(bgm->num_keys))
        .write((char *) &bgm->num_recs, sizeof(bgm->num_recs))) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bgm->num_keys; i++) {
        if (!s
            .write((char *) &bgm->rs_idx[i], sizeof(bgm->rs_idx[i]))
            .write((char *) &bgm->key_lens[i], sizeof(bgm->key_lens[i]))
            .write((char *) bgm->keys[i], bgm->key_lens[i])) {
            return MDHIM_ERROR;
        }
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportDeleteMessage *dm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(dm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &dm->rs_idx, sizeof(dm->rs_idx))
        .write((char *) &dm->key_len, sizeof(dm->key_len))
        .write((char *) dm->key, dm->key_len)) {
        return MDHIM_ERROR;
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportBDeleteMessage *bdm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(bdm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &bdm->num_keys, sizeof(bdm->num_keys))) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bdm->num_keys; i++) {
        if (!s
            .write((char *) &bdm->rs_idx[i], sizeof(bdm->rs_idx[i]))
            .write((char *) &bdm->key_lens[i], sizeof(bdm->key_lens[i]))
            .write((char *) bdm->keys[i], bdm->key_lens[i])) {
            return MDHIM_ERROR;
        }
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportRecvMessage *rm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(rm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &rm->rs_idx, sizeof(rm->rs_idx))
        .write((char *) &rm->error, sizeof(rm->error))) {
        return MDHIM_ERROR;
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportGetRecvMessage *grm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(grm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &grm->rs_idx, sizeof(grm->rs_idx))
        .write((char *) &grm->error, sizeof(grm->error))
        .write((char *) &grm->key_len, sizeof(grm->key_len))
        .write((char *) &grm->value_len, sizeof(grm->value_len))
        .write((char *) grm->key, grm->key_len)
        .write((char *) grm->value, grm->value_len)) {
        return MDHIM_ERROR;
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportBGetRecvMessage *bgrm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(bgrm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &bgrm->error, sizeof(bgrm->error))
        .write((char *) &bgrm->num_keys, sizeof(bgrm->num_keys))) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bgrm->num_keys; i++) {
        if (!s
            .write((char *) &bgrm->rs_idx[i], sizeof(bgrm->rs_idx[i]))
            .write((char *) &bgrm->key_lens[i], sizeof(bgrm->key_lens[i]))
            .write((char *) bgrm->keys[i], bgrm->key_lens[i])
            .write((char *) &bgrm->value_lens[i], sizeof(bgrm->value_lens[i]))
            .write((char *) bgrm->values[i], bgrm->value_lens[i])) {
            return MDHIM_ERROR;
        }
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportBRecvMessage *brm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const TransportMessage *>(brm), s) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    if (!s
        .write((char *) &brm->error, sizeof(brm->error))
        .write((char *) &brm->num_keys, sizeof(brm->num_keys))) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < brm->num_keys; i++) {
        if (!s
            .write((char *) &brm->rs_idx[i], sizeof(brm->rs_idx[i]))) {
            return MDHIM_ERROR;
        }
    }

    buf = s.str();

    return MDHIM_SUCCESS;
}

int ThalliumPacker::pack(const TransportMessage *msg, std::stringstream &s) {
    return s
        .write((char *) &msg->mtype, sizeof(msg->mtype))
        .write((char *) &msg->src, sizeof(msg->src))
        .write((char *) &msg->dst, sizeof(msg->dst))
        .write((char *) &msg->index, sizeof(msg->index))
        .write((char *) &msg->index_type, sizeof(msg->index_type))
        .write((char *) &msg->index_name, sizeof(msg->index_name))?MDHIM_SUCCESS:MDHIM_ERROR;
}
