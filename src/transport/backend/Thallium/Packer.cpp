#if HXHIM_HAVE_THALLIUM

#include <cstring>

#include "transport/backend/Thallium/Packer.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

namespace Transport {
namespace Thallium {

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Packing Request type %d", req->type);

    switch (req->type) {
        case Message::PUT:
            ret = pack(static_cast<const Request::Put *>(req), buf, bufsize, packed);
            break;
        case Message::GET:
            ret = pack(static_cast<const Request::Get *>(req), buf, bufsize, packed);
            break;
        case Message::DELETE:
            ret = pack(static_cast<const Request::Delete *>(req), buf, bufsize, packed);
            break;
        case Message::HISTOGRAM:
            ret = pack(static_cast<const Request::Histogram *>(req), buf, bufsize, packed);
            break;
        case Message::BPUT:
            ret = pack(static_cast<const Request::BPut *>(req), buf, bufsize, packed);
            break;
        case Message::BGET:
            ret = pack(static_cast<const Request::BGet *>(req), buf, bufsize, packed);
            break;
        case Message::BGETOP:
            ret = pack(static_cast<const Request::BGetOp *>(req), buf, bufsize, packed);
            break;
        case Message::BDELETE:
            ret = pack(static_cast<const Request::BDelete *>(req), buf, bufsize, packed);
            break;
        default:
            break;
    }

    // mlog(THALLIUM_DBG, "Done Packing Request type %d", req->type);

    return ret;
}

int Packer::pack(const Request::Put *pm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(pm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &pm->ds_offset, sizeof(pm->ds_offset));
    curr += sizeof(pm->ds_offset);

    memcpy(curr, &pm->subject_len, sizeof(pm->subject_len));
    curr += sizeof(pm->subject_len);

    memcpy(curr, &pm->predicate_len, sizeof(pm->predicate_len));
    curr += sizeof(pm->predicate_len);

    memcpy(curr, &pm->object_len, sizeof(pm->object_len));
    curr += sizeof(pm->object_len);

    memcpy(curr, &pm->object_type, sizeof(pm->object_type));
    curr += sizeof(pm->object_type);

    memcpy(curr, pm->subject, pm->subject_len);
    curr += pm->subject_len;

    memcpy(curr, pm->predicate, pm->predicate_len);
    curr += pm->predicate_len;

    memcpy(curr, pm->object, pm->object_len);
    // curr += pm->object_len;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Get *gm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(gm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &gm->ds_offset, sizeof(gm->ds_offset));
    curr += sizeof(gm->ds_offset);

    memcpy(curr, &gm->subject_len, sizeof(gm->subject_len));
    curr += sizeof(gm->subject_len);

    memcpy(curr, &gm->predicate_len, sizeof(gm->predicate_len));
    curr += sizeof(gm->predicate_len);

    memcpy(curr, &gm->object_type, sizeof(gm->object_type));
    curr += sizeof(gm->object_type);

    memcpy(curr, gm->subject, gm->subject_len);
    curr += gm->subject_len;

    memcpy(curr, gm->predicate, gm->predicate_len);
    // curr += gm->predicate_len;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Delete *dm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(dm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &dm->ds_offset, sizeof(dm->ds_offset));
    curr += sizeof(dm->ds_offset);

    memcpy(curr, &dm->subject_len, sizeof(dm->subject_len));
    curr += sizeof(dm->subject_len);

    memcpy(curr, &dm->predicate_len, sizeof(dm->predicate_len));
    curr += sizeof(dm->predicate_len);

    memcpy(curr, dm->subject, dm->subject_len);
    curr += dm->subject_len;

    memcpy(curr, dm->predicate, dm->predicate_len);
    // curr += db->predicate_len;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Histogram *hist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(hist), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &hist->ds_offset, sizeof(hist->ds_offset));
    // curr += sizeof(hist->ds_offset);

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BPut *bpm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bpm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bpm->count, sizeof(bpm->count));
    curr += sizeof(bpm->count);

    for(std::size_t i = 0; i < bpm->count; i++) {
        memcpy(curr, &bpm->ds_offsets[i], sizeof(bpm->ds_offsets[i]));
        curr += sizeof(bpm->ds_offsets[i]);

        memcpy(curr, &bpm->subject_lens[i], sizeof(bpm->subject_lens[i]));
        curr += sizeof(bpm->subject_lens[i]);

        memcpy(curr, &bpm->predicate_lens[i], sizeof(bpm->predicate_lens[i]));
        curr += sizeof(bpm->predicate_lens[i]);

        memcpy(curr, &bpm->object_lens[i], sizeof(bpm->object_lens[i]));
        curr += sizeof(bpm->object_lens[i]);

        memcpy(curr, &bpm->object_types[i], sizeof(bpm->object_types[i]));
        curr += sizeof(bpm->object_types[i]);

        memcpy(curr, bpm->subjects[i], bpm->subject_lens[i]);
        curr += bpm->subject_lens[i];

        memcpy(curr, bpm->predicates[i], bpm->predicate_lens[i]);
        curr += bpm->predicate_lens[i];

        memcpy(curr, bpm->objects[i], bpm->object_lens[i]);
        curr += bpm->object_lens[i];
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGet *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        memcpy(curr, &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]));
        curr += sizeof(bgm->subject_lens[i]);

        memcpy(curr, &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]));
        curr += sizeof(bgm->predicate_lens[i]);

        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        memcpy(curr, bgm->subjects[i], bgm->subject_lens[i]);
        curr += bgm->subject_lens[i];

        memcpy(curr, bgm->predicates[i], bgm->predicate_lens[i]);
        curr += bgm->predicate_lens[i];
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGetOp *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        memcpy(curr, &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]));
        curr += sizeof(bgm->subject_lens[i]);

        memcpy(curr, &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]));
        curr += sizeof(bgm->predicate_lens[i]);

        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        memcpy(curr, &bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);

        memcpy(curr, &bgm->ops[i], sizeof(bgm->ops[i]));
        curr += sizeof(bgm->ops[i]);

        memcpy(curr, bgm->subjects[i], bgm->subject_lens[i]);
        curr += bgm->subject_lens[i];

        memcpy(curr, bgm->predicates[i], bgm->predicate_lens[i]);
        curr += bgm->predicate_lens[i];
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BDelete *bdm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bdm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bdm->count, sizeof(bdm->count));
    curr += sizeof(bdm->count);

    for(std::size_t i = 0; i < bdm->count; i++) {
        memcpy(curr, &bdm->ds_offsets[i], sizeof(bdm->ds_offsets[i]));
        curr += sizeof(bdm->ds_offsets[i]);

        memcpy(curr, &bdm->subject_lens[i], sizeof(bdm->subject_lens[i]));
        curr += sizeof(bdm->subject_lens[i]);

        memcpy(curr, &bdm->predicate_lens[i], sizeof(bdm->predicate_lens[i]));
        curr += sizeof(bdm->predicate_lens[i]);

        memcpy(curr, bdm->subjects[i], bdm->subject_lens[i]);
        curr += bdm->subject_lens[i];

        memcpy(curr, bdm->predicates[i], bdm->predicate_lens[i]);
        curr += bdm->predicate_lens[i];
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BHistogram *bhist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bhist), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
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

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Packing Response type %d", res->type);

    switch (res->type) {
        case Message::PUT:
            ret = pack(static_cast<const Response::Put *>(res), buf, bufsize, packed);
            break;
        case Message::GET:
            ret = pack(static_cast<const Response::Get *>(res), buf, bufsize, packed);
            break;
        case Message::DELETE:
            ret = pack(static_cast<const Response::Delete *>(res), buf, bufsize, packed);
            break;
        case Message::HISTOGRAM:
            ret = pack(static_cast<const Response::Histogram *>(res), buf, bufsize, packed);
            break;
        case Message::BPUT:
            ret = pack(static_cast<const Response::BPut *>(res), buf, bufsize, packed);
            break;
        case Message::BGET:
            ret = pack(static_cast<const Response::BGet *>(res), buf, bufsize, packed);
            break;
        case Message::BGETOP:
            ret = pack(static_cast<const Response::BGetOp *>(res), buf, bufsize, packed);
            break;
        case Message::BDELETE:
            ret = pack(static_cast<const Response::BDelete *>(res), buf, bufsize, packed);
            break;
        default:
            break;
    }

    // mlog(THALLIUM_DBG, "Done Packing Response type %d", res->type);

    return ret;
}

int Packer::pack(const Response::Put *pm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(pm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &pm->status, sizeof(pm->status));
    curr += sizeof(pm->status);

    memcpy(curr, &pm->ds_offset, sizeof(pm->ds_offset));
    // curr += sizeof(pm->ds_offset);

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Get *gm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(gm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &gm->status, sizeof(gm->status));
    curr += sizeof(gm->status);

    memcpy(curr, &gm->ds_offset, sizeof(gm->ds_offset));
    curr += sizeof(gm->ds_offset);

    memcpy(curr, &gm->subject_len, sizeof(gm->subject_len));
    curr += sizeof(gm->subject_len);

    memcpy(curr, &gm->predicate_len, sizeof(gm->predicate_len));
    curr += sizeof(gm->predicate_len);

    memcpy(curr, &gm->object_type, sizeof(gm->object_type));
    curr += sizeof(gm->object_type);

    memcpy(curr, gm->subject, gm->subject_len);
    curr += gm->subject_len;

    memcpy(curr, gm->predicate, gm->predicate_len);

    // only write the rest of the data if it exists
    if (gm->status == HXHIM_SUCCESS) {
        curr += gm->predicate_len;

        memcpy(curr, &gm->object_len, sizeof(gm->object_len));
        curr += sizeof(gm->object_len);

        memcpy(curr, gm->object, gm->object_len);
        // curr += gm->object_len;
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Delete *dm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(dm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &dm->status, sizeof(dm->status));
    curr += sizeof(dm->status);

    memcpy(curr, &dm->ds_offset, sizeof(dm->ds_offset));
    // curr += sizeof(dm->ds_offset);

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Histogram *hist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(hist), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &hist->status, sizeof(hist->status));
    curr += sizeof(hist->status);

    memcpy(curr, &hist->ds_offset, sizeof(hist->ds_offset));
    curr += sizeof(hist->ds_offset);

    const std::size_t size = hist->hist.size;
    memcpy(curr, &size, sizeof(size));
    curr += sizeof(size);

    for(std::size_t i = 0; i < size; i++) {
        memcpy(curr, &hist->hist.buckets[i], sizeof(hist->hist.buckets[i]));
        curr += sizeof(hist->hist.buckets[i]);

        memcpy(curr, &hist->hist.counts[i], sizeof(hist->hist.counts[i]));
        curr += sizeof(hist->hist.counts[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BPut *bpm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bpm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
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

int Packer::pack(const Response::BGet *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        memcpy(curr, &bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        memcpy(curr, &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]));
        curr += sizeof(bgm->subject_lens[i]);

        memcpy(curr, &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]));
        curr += sizeof(bgm->predicate_lens[i]);

        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        memcpy(curr, bgm->subjects[i], bgm->subject_lens[i]);
        curr += bgm->subject_lens[i];

        memcpy(curr, bgm->predicates[i], bgm->predicate_lens[i]);
        curr += bgm->predicate_lens[i];

        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            memcpy(curr, &bgm->object_lens[i], sizeof(bgm->object_lens[i]));
            curr += sizeof(bgm->object_lens[i]);

            memcpy(curr, bgm->objects[i], bgm->object_lens[i]);
            curr += bgm->object_lens[i];
        }
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGetOp *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    memcpy(curr, &bgm->count, sizeof(bgm->count));
    curr += sizeof(bgm->count);

    for(std::size_t i = 0; i < bgm->count; i++) {
        memcpy(curr, &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]));
        curr += sizeof(bgm->ds_offsets[i]);

        memcpy(curr, &bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        memcpy(curr, &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]));
        curr += sizeof(bgm->subject_lens[i]);

        memcpy(curr, &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]));
        curr += sizeof(bgm->predicate_lens[i]);

        memcpy(curr, &bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        memcpy(curr, bgm->subjects[i], bgm->subject_lens[i]);
        curr += bgm->subject_lens[i];

        memcpy(curr, bgm->predicates[i], bgm->predicate_lens[i]);
        curr += bgm->predicate_lens[i];

        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            memcpy(curr, &bgm->object_lens[i], sizeof(bgm->object_lens[i]));
            curr += sizeof(bgm->object_lens[i]);

            memcpy(curr, bgm->objects[i], bgm->object_lens[i]);
            curr += bgm->object_lens[i];
        }
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BDelete *bdm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bdm), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
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

int Packer::pack(const Response::BHistogram *bhist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bhist), buf, bufsize, packed, &curr) != TRANSPORT_SUCCESS) {
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

int Packer::pack(const Message *msg, void **buf, std::size_t *bufsize, FixedBufferPool *packed, char **curr) {
    if (!msg || !buf || !bufsize || !curr) {
        return TRANSPORT_ERROR;
    }

    const std::size_t minsize = msg->size();

    // only allocate space if a nullptr is provided; otherwise, assume *buf has enough space
    if (!*buf) {
        if (!packed || !(*buf = packed->acquire(minsize))) {
            *bufsize = 0;
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = minsize;

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

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize, FixedBufferPool *packed, char **curr) {
    return pack(static_cast<const Message *>(req), buf, bufsize, packed, curr);
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize, FixedBufferPool *packed, char **curr) {
    return pack(static_cast<const Message *>(res), buf, bufsize, packed, curr);
}

}
}

#endif
