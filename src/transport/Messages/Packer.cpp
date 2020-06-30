#include <cstring>

#include "transport/Messages/Packer.hpp"

namespace Transport {

static char *pack_addr(char *&dst, void *ptr) {
    // // skip check
    // if (!dst) {
    //     return nullptr;
    // }

    memcpy(dst, &ptr, sizeof(ptr));
    dst += sizeof(ptr);
    return dst;
}

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

        // subject + len
        bpm->subjects[i]->pack(curr);

        // subject addr
        pack_addr(curr, bpm->subjects[i]->data());

        // predicate + len
        bpm->predicates[i]->pack(curr);

        // predicate addr
        pack_addr(curr, bpm->predicates[i]->data());

        // object type
        memcpy(curr, &bpm->object_types[i], sizeof(bpm->object_types[i]));
        curr += sizeof(bpm->object_types[i]);

        // object + len
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
        pack_addr(curr, bgm->subjects[i]->data());

        // predicate
        bgm->predicates[i]->pack(curr);

        // predicate addr
        pack_addr(curr, bgm->predicates[i]->data());

        // object type
        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);
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

        // operation to run
        memcpy(curr, &bgm->ops[i], sizeof(bgm->ops[i]));
        curr += sizeof(bgm->ops[i]);

        if ((bgm->ops[i] != hxhim_get_op_t::HXHIM_GET_FIRST) &&
            (bgm->ops[i] != hxhim_get_op_t::HXHIM_GET_LAST))  {
            // subject
            bgm->subjects[i]->pack(curr);

            // predicate
            bgm->predicates[i]->pack(curr);
        }

        // object type
        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // number of records to get back
        memcpy(curr, &bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);
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

        // subject
        bdm->subjects[i]->pack(curr);

        // subject addr
        pack_addr(curr, bdm->subjects[i]->data());

        // predicate
        bdm->predicates[i]->pack(curr);

        // predicate addr
        pack_addr(curr, bdm->predicates[i]->data());
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

    for(std::size_t i = 0; i < bpm->count; i++) {
        memcpy(curr, &bpm->ds_offsets[i], sizeof(bpm->ds_offsets[i]));
        curr += sizeof(bpm->ds_offsets[i]);

        memcpy(curr, &bpm->statuses[i], sizeof(bpm->statuses[i]));
        curr += sizeof(bpm->statuses[i]);

        // original subject addr + len
        bpm->orig.subjects[i]->pack_ref(curr);

        // original predicate addr + len
        bpm->orig.predicates[i]->pack_ref(curr);
    }

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

        // original subject addr + len
        bgm->orig.subjects[i]->pack_ref(curr);

        // original predicate addr + len
        bgm->orig.predicates[i]->pack_ref(curr);

        // object type
        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

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

        // object type
        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // num_recs
        memcpy(curr, &bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);

        for(std::size_t j = 0; j < bgm->num_recs[i]; j++) {
            // subject
            bgm->subjects[i][j]->pack(curr);

            // predicate
            bgm->predicates[i][j]->pack(curr);

            // object
            if (bgm->statuses[i] == HXHIM_SUCCESS) {
                bgm->objects[i][j]->pack(curr);
            }
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

    for(std::size_t i = 0; i < bdm->count; i++) {
        memcpy(curr, &bdm->ds_offsets[i], sizeof(bdm->ds_offsets[i]));
        curr += sizeof(bdm->ds_offsets[i]);

        memcpy(curr, &bdm->statuses[i], sizeof(bdm->statuses[i]));
        curr += sizeof(bdm->statuses[i]);

        // original subject addr + len
        bdm->orig.subjects[i]->pack_ref(curr);

        // original predicate addr + len
        bdm->orig.predicates[i]->pack_ref(curr);
    }

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
