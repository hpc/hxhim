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
        // case TransportMessageType::DELETE:
        //     ret = pack(dynamic_cast<const TransportDeleteMessage *>(msg), buf);
        //     break;
        // case TransportMessageType::BDELETE:
        //     ret = pack(dynamic_cast<const TransportBDeleteMessage *>(msg), buf);
        //     break;
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
        .write((char *) &bpm->num_keys, sizeof(bpm->num_keys))
        .write((char *) bpm->key_lens, sizeof(*bpm->key_lens) * bpm->num_keys)
        .write((char *) bpm->value_lens, sizeof(*bpm->value_lens) * bpm->num_keys)) {
        return MDHIM_ERROR;
    }

    for(int i = 0; i < bpm->num_keys; i++) {
        if (!s
            .write((char *) bpm->keys[i], bpm->key_lens[i])
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
        .write((char *) &bgm->num_recs, sizeof(bgm->num_recs))
        .write((char *) bgm->key_lens, sizeof(*bgm->key_lens) * bgm->num_keys)) {
        return MDHIM_ERROR;
    }

    for(int i = 0; i < bgm->num_keys; i++) {
        if (!s
            .write((char *) bgm->keys[i], bgm->key_lens[i])) {
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

    if (!s.write((char *) &rm->error, sizeof(rm->error))) {
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
        .write((char *) &bgrm->num_keys, sizeof(bgrm->num_keys))
        .write((char *) bgrm->key_lens, sizeof(*bgrm->key_lens) * bgrm->num_keys)
        .write((char *) bgrm->value_lens, sizeof(*bgrm->value_lens) * bgrm->num_keys)) {
        return MDHIM_ERROR;
    }

    for(int i = 0; i < bgrm->num_keys; i++) {
        if (!s
            .write((char *) bgrm->keys[i], bgrm->key_lens[i])
            .write((char *) bgrm->values[i], bgrm->value_lens[i])) {
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
