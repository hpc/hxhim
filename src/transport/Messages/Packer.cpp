#include <ctime>
#include <cstring>

#include "transport/Messages/Packer.hpp"
#include "utils/big_endian.hpp"

namespace Transport {

static char *pack_addr(char *&dst, void *ptr) {
    // // skip check
    // if (!dst) {
    //     return nullptr;
    // }

    big_endian::encode(dst, ptr);
    dst += sizeof(ptr);
    return dst;
}

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Packing Request type %d", req->op);

    switch (req->op) {
        case hxhim_op_t::HXHIM_PUT:
            ret = pack(static_cast<const Request::BPut *>(req), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_GET:
            ret = pack(static_cast<const Request::BGet *>(req), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_GETOP:
            ret = pack(static_cast<const Request::BGetOp *>(req), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_DELETE:
            ret = pack(static_cast<const Request::BDelete *>(req), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            ret = pack(static_cast<const Request::BHistogram *>(req), buf, bufsize);
            break;
        default:
            break;
    }

    // mlog(THALLIUM_DBG, "Done Packing Request type %d", req->op);

    return ret;
}

int Packer::pack(const Request::BPut *bpm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bpm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        // subject + len
        bpm->subjects[i].pack(curr);

        // subject addr
        pack_addr(curr, bpm->subjects[i].data());

        // predicate + len
        bpm->predicates[i].pack(curr);

        // predicate addr
        pack_addr(curr, bpm->predicates[i].data());

        // object type
        big_endian::encode(curr, bpm->object_types[i], sizeof(bpm->object_types[i]));
        curr += sizeof(bpm->object_types[i]);

        // object + len
        bpm->objects[i].pack(curr);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGet *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        // subject
        bgm->subjects[i].pack(curr);

        // subject addr
        pack_addr(curr, bgm->subjects[i].data());

        // predicate
        bgm->predicates[i].pack(curr);

        // predicate addr
        pack_addr(curr, bgm->predicates[i].data());

        // object type
        big_endian::encode(curr, bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGetOp *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        // operation to run
        big_endian::encode(curr, bgm->ops[i], sizeof(bgm->ops[i]));
        curr += sizeof(bgm->ops[i]);

        if ((bgm->ops[i] != hxhim_getop_t::HXHIM_GETOP_FIRST) &&
            (bgm->ops[i] != hxhim_getop_t::HXHIM_GETOP_LAST))  {
            // subject
            bgm->subjects[i].pack(curr);

            // predicate
            bgm->predicates[i].pack(curr);
        }

        // object type
        big_endian::encode(curr, bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // number of records to get back
        big_endian::encode(curr, bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BDelete *bdm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bdm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bdm->count; i++) {
        // subject
        bdm->subjects[i].pack(curr);

        // subject addr
        pack_addr(curr, bdm->subjects[i].data());

        // predicate
        bdm->predicates[i].pack(curr);

        // predicate addr
        pack_addr(curr, bdm->predicates[i].data());
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BHistogram *bhm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bhm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Packing Response type %d", res->op);

    switch (res->op) {
        case hxhim_op_t::HXHIM_PUT:
            ret = pack(static_cast<const Response::BPut *>(res), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_GET:
            ret = pack(static_cast<const Response::BGet *>(res), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_GETOP:
            ret = pack(static_cast<const Response::BGetOp *>(res), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_DELETE:
            ret = pack(static_cast<const Response::BDelete *>(res), buf, bufsize);
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            ret = pack(static_cast<const Response::BHistogram *>(res), buf, bufsize);
            break;
        default:
            break;
    }

    // mlog(THALLIUM_DBG, "Done Packing Response type %d", res->op);

    return ret;
}

int Packer::pack(const Response::BPut *bpm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bpm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        big_endian::encode(curr, bpm->statuses[i], sizeof(bpm->statuses[i]));
        curr += sizeof(bpm->statuses[i]);

        // original subject addr + len
        bpm->orig.subjects[i].pack_ref(curr);

        // original predicate addr + len
        bpm->orig.predicates[i].pack_ref(curr);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGet *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;

    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        big_endian::encode(curr, bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        // original subject addr + len
        bgm->orig.subjects[i].pack_ref(curr);

        // original predicate addr + len
        bgm->orig.predicates[i].pack_ref(curr);

        // object type
        big_endian::encode(curr, bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // object
        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            bgm->objects[i].pack(curr);
        }
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGetOp *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        big_endian::encode(curr, bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        // object type
        big_endian::encode(curr, bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // num_recs
        big_endian::encode(curr, bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);

        for(std::size_t j = 0; j < bgm->num_recs[i]; j++) {
            // subject
            bgm->subjects[i][j].pack(curr);

            // predicate
            bgm->predicates[i][j].pack(curr);

            // object
            if (bgm->statuses[i] == HXHIM_SUCCESS) {
                bgm->objects[i][j].pack(curr);
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

    for(std::size_t i = 0; i < bdm->count; i++) {
        big_endian::encode(curr, bdm->statuses[i], sizeof(bdm->statuses[i]));
        curr += sizeof(bdm->statuses[i]);

        // original subject addr + len
        bdm->orig.subjects[i].pack_ref(curr);

        // original predicate addr + len
        bdm->orig.predicates[i].pack_ref(curr);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BHistogram *bhm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bhm), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    std::size_t avail = *bufsize - (curr - (char *) *buf);

    for(std::size_t i = 0; i < bhm->count; i++) {
        big_endian::encode(curr, bhm->statuses[i], sizeof(bhm->statuses[i]));
        curr += sizeof(bhm->statuses[i]);

        // histogram
        bhm->histograms[i]->pack(curr, avail, nullptr);
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
    big_endian::encode(*curr, msg->direction);
    *curr += sizeof(msg->direction);

    big_endian::encode(*curr, msg->op);
    *curr += sizeof(msg->op);

    big_endian::encode(*curr, msg->src);
    *curr += sizeof(msg->src);

    big_endian::encode(*curr, msg->dst);
    *curr += sizeof(msg->dst);

    big_endian::encode(*curr, msg->count);
    *curr += sizeof(msg->count);

    for(std::size_t i = 0; i < msg->count; i++)  {
        big_endian::encode(*curr, msg->ds_offsets[i]);
        *curr += sizeof(msg->ds_offsets[i]);
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize, char **curr) {
    return pack(static_cast<const Message *>(req), buf, bufsize, curr);
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize, char **curr) {
    return pack(static_cast<const Message *>(res), buf, bufsize, curr);
}

}
