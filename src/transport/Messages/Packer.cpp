#include <cstring>

#include "transport/Messages/Packer.hpp"

namespace Transport {

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Packing Request type %d", req->type);

    switch (req->type) {
        case Message::BPUT:
            ret = pack(static_cast<const Request::BPut *>(req), buf, bufsize);
            break;
        case Message::BGET:
            ret = pack(static_cast<const Request::BGet *>(req), buf, bufsize);
            break;
        case Message::BGETOP:
            ret = pack(static_cast<const Request::BGetOp *>(req), buf, bufsize);
            break;
        case Message::BDELETE:
            ret = pack(static_cast<const Request::BDelete *>(req), buf, bufsize);
            break;
        default:
            break;
    }

    // mlog(THALLIUM_DBG, "Done Packing Request type %d", req->type);

    return ret;
}

int Packer::pack(const Request::BPut *bpm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bpm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bpm->count, sizeof(bpm->count));
    curr += sizeof(bpm->count);

    for(std::size_t i = 0; i < bpm->count; i++) {
        memcpy(curr, &bpm->ds_offsets[i], sizeof(bpm->ds_offsets[i]));
        curr += sizeof(bpm->ds_offsets[i]);

        bpm->subjects[i]->pack(curr);
        bpm->predicates[i]->pack(curr);
        memcpy(curr, &bpm->object_types[i], sizeof(bpm->object_types[i]));
        curr += sizeof(bpm->object_types[i]);
        bpm->objects[i]->pack(curr);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGet *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        // subject
        bgm->subjects[i]->pack(curr);

        // subject addr
        memcpy(curr, &bgm->subjects[i]->ptr, sizeof(bgm->subjects[i]->ptr));
        curr += sizeof(bgm->subjects[i]->ptr);

        // predicate
        bgm->predicates[i]->pack(curr);

        // predicate addr
        memcpy(curr, &bgm->predicates[i]->ptr, sizeof(bgm->predicates[i]->ptr));
        curr += sizeof(bgm->predicates[i]->ptr);

        // object type
        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // object addr
        memcpy(curr, &(bgm->orig.objects[i]), sizeof(bgm->orig.objects[i]));
        curr += sizeof(bgm->orig.objects[i]);

        // object len addr
        memcpy(curr, &(bgm->orig.object_lens[i]), sizeof(bgm->orig.object_lens[i]));
        curr += sizeof(bgm->orig.object_lens[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGetOp *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        bgm->subjects[i]->pack(curr);
        bgm->predicates[i]->pack(curr);

        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        memcpy(curr, &bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);

        memcpy(curr, &bgm->ops[i], sizeof(bgm->ops[i]));
        curr += sizeof(bgm->ops[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BDelete *bdm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bdm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bdm->count, sizeof(bdm->count));
    curr += sizeof(bdm->count);

    for(std::size_t i = 0; i < bdm->count; i++) {
        memcpy(curr, &bdm->ds_offsets[i], sizeof(bdm->ds_offsets[i]));
        curr += sizeof(bdm->ds_offsets[i]);

        bdm->subjects[i]->pack(curr);
        bdm->predicates[i]->pack(curr);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BHistogram *bhist, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bhist), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bhist->count, sizeof(bhist->count));
    curr += sizeof(bhist->count);

    for(std::size_t i = 0; i < bhist->count; i++) {
        memcpy(curr, &bhist->ds_offsets[i], sizeof(bhist->ds_offsets[i]));
        curr += sizeof(bhist->ds_offsets[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Packing Response type %d", res->type);

    switch (res->type) {
        case Message::BPUT:
            ret = pack(static_cast<const Response::BPut *>(res), buf, bufsize);
            break;
        case Message::BGET:
            ret = pack(static_cast<const Response::BGet *>(res), buf, bufsize);
            break;
        case Message::BGETOP:
            ret = pack(static_cast<const Response::BGetOp *>(res), buf, bufsize);
            break;
        case Message::BDELETE:
            ret = pack(static_cast<const Response::BDelete *>(res), buf, bufsize);
            break;
        default:
            break;
    }

    // mlog(THALLIUM_DBG, "Done Packing Response type %d", res->type);

    return ret;
}

int Packer::pack(const Response::BPut *bpm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bpm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bpm->count, sizeof(bpm->count));
    curr += sizeof(bpm->count);

    memcpy(curr, bpm->ds_offsets, sizeof(*bpm->ds_offsets) * bpm->count);
    curr += sizeof(*bpm->ds_offsets) * bpm->count;

    memcpy(curr, bpm->statuses, sizeof(*bpm->statuses) * bpm->count);
    // curr += sizeof(*bpm->statuses) * bpm->count;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGet *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;

    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        memcpy(curr, &bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        // subject
        bgm->subjects[i]->pack(curr);

        // original subject addr
        memcpy(curr, &bgm->orig.subjects[i], sizeof(bgm->orig.subjects[i]));
        curr += sizeof(bgm->orig.subjects[i]);

        // predicate
        bgm->predicates[i]->pack(curr);

        // original predicate addr
        memcpy(curr, &bgm->orig.predicates[i], sizeof(bgm->orig.predicates[i]));
        curr += sizeof(bgm->orig.predicates[i]);

        // object type
        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // original object len addr
        memcpy(curr, &bgm->orig.object_lens[i], sizeof(bgm->orig.object_lens[i]));
        curr += sizeof(bgm->orig.object_lens[i]);

        // original object addr
        memcpy(curr, &bgm->orig.objects[i], sizeof(bgm->orig.objects[i]));
        curr += sizeof(bgm->orig.objects[i]);

        // object
        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            bgm->objects[i]->pack(curr);
        }
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGetOp *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        memcpy(curr, &bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        bgm->subjects[i]->pack(curr);
        bgm->predicates[i]->pack(curr);

        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            bgm->objects[i]->pack(curr);
        }
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BDelete *bdm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bdm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bdm->count, sizeof(bdm->count));
    curr += sizeof(bdm->count);

    memcpy(curr, bdm->ds_offsets, sizeof(*bdm->ds_offsets) * bdm->count);
    curr += sizeof(*bdm->ds_offsets) * bdm->count;

    memcpy(curr, bdm->statuses, sizeof(*bdm->statuses) * bdm->count);
    // curr += sizeof(*bdm->statuses) * bdm->count;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BHistogram *bhist, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;

    if (pack(static_cast<const Response::Response *>(bhist), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bhist->count, sizeof(bhist->count));
    curr += sizeof(bhist->count);

    memcpy(curr, bhist->ds_offsets, sizeof(*bhist->ds_offsets) * bhist->count);
    curr += sizeof(*bhist->ds_offsets) * bhist->count;

    memcpy(curr, bhist->statuses, sizeof(*bhist->statuses) * bhist->count);
    curr += sizeof(*bhist->statuses) * bhist->count;

    for(std::size_t i = 0; i < bhist->count; i++) {
        memcpy(curr, &bhist->hists[i].size, sizeof(bhist->hists[i].size));
        curr += sizeof(bhist->hists[i].size);

        for(std::size_t j = 0; j < bhist->hists[i].size; j++) {
            memcpy(curr, &bhist->hists[i].buckets[j], sizeof(bhist->hists[i].buckets[j]));
            curr += sizeof(bhist->hists[i].buckets[j]);

            memcpy(curr, &bhist->hists[i].counts[j], sizeof(bhist->hists[i].counts[j]));
            curr += sizeof(bhist->hists[i].counts[j]);
        }
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Message *msg, void **buf, std::size_t *bufsize, char **curr) {
    if (!msg || !buf || !bufsize || !curr) {
        return TRANSPORT_ERROR;
    }

    *bufsize = msg->size();

    // only allocate space if a nullptr is provided; otherwise, assume *buf has enough space
    if (!*buf) {
        if (!(*buf = alloc(*bufsize))) {
            *bufsize = 0;
            return TRANSPORT_ERROR;
        }
    }

    *curr = (char *) *buf;

    // copy header into *buf
    memcpy(*curr, (char *)&msg->direction, sizeof(msg->direction));
    *curr += sizeof(msg->direction);

    memcpy(*curr, (char *)&msg->type, sizeof(msg->type));
    *curr += sizeof(msg->type);

    memcpy(*curr, &msg->src, sizeof(msg->src));
    *curr += sizeof(msg->src);

    memcpy(*curr, &msg->dst, sizeof(msg->dst));
    *curr += sizeof(msg->dst);

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize, char **curr) {
    return pack(static_cast<const Message *>(req), buf, bufsize, curr);
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize, char **curr) {
    return pack(static_cast<const Message *>(res), buf, bufsize, curr);
}

}
