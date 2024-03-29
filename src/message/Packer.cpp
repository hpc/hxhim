#include "datastore/constants.hpp"
#include "message/Packer.hpp"
#include "utils/little_endian.hpp"
#include "utils/memory.hpp"

namespace Message {

static char *pack_addr(char *&dst, void *ptr) {
    // // skip check
    // if (!dst) {
    //     return nullptr;
    // }

    little_endian::encode(dst, ptr);
    dst += sizeof(ptr);
    return dst;
}

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize) {
    int ret = MESSAGE_ERROR;
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
    if (pack(static_cast<const Request::Request *>(bpm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        // subject + len
        bpm->subjects[i].pack(curr, true);

        // subject addr
        pack_addr(curr, bpm->subjects[i].data());

        // predicate + len
        bpm->predicates[i].pack(curr, true);

        // predicate addr
        pack_addr(curr, bpm->predicates[i].data());

        // object + len
        bpm->objects[i].pack(curr, true);
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Request::BGet *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        // subject
        bgm->subjects[i].pack(curr, true);

        // subject addr
        pack_addr(curr, bgm->subjects[i].data());

        // predicate
        bgm->predicates[i].pack(curr, true);

        // predicate addr
        pack_addr(curr, bgm->predicates[i].data());

        // object type
        little_endian::encode(curr, bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Request::BGetOp *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bgm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        // operation to run
        little_endian::encode(curr, bgm->ops[i], sizeof(bgm->ops[i]));
        curr += sizeof(bgm->ops[i]);

        if ((bgm->ops[i] != hxhim_getop_t::HXHIM_GETOP_FIRST) &&
            (bgm->ops[i] != hxhim_getop_t::HXHIM_GETOP_LAST))  {
            // subject
            bgm->subjects[i].pack(curr, true);

            // predicate
            bgm->predicates[i].pack(curr, true);
        }

        // object type
        little_endian::encode(curr, bgm->object_types[i], sizeof(bgm->object_types[i]));
        curr += sizeof(bgm->object_types[i]);

        // number of records to get back
        little_endian::encode(curr, bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Request::BDelete *bdm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bdm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bdm->count; i++) {
        // subject
        bdm->subjects[i].pack(curr, true);

        // subject addr
        pack_addr(curr, bdm->subjects[i].data());

        // predicate
        bdm->predicates[i].pack(curr, true);

        // predicate addr
        pack_addr(curr, bdm->predicates[i].data());
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Request::BHistogram *bhm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Request::Request *>(bhm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bhm->count; i++) {
        // histogram names
        bhm->names[i].pack(curr, false);
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize) {
    int ret = MESSAGE_ERROR;
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
    if (pack(static_cast<const Response::Response *>(bpm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        little_endian::encode(curr, bpm->statuses[i], sizeof(bpm->statuses[i]));
        curr += sizeof(bpm->statuses[i]);

        // original subject addr + len
        bpm->orig.subjects[i].pack_ref(curr, true);

        // original predicate addr + len
        bpm->orig.predicates[i].pack_ref(curr, true);
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Response::BGet *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;

    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        little_endian::encode(curr, bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        // original subject addr + len
        bgm->orig.subjects[i].pack_ref(curr, true);

        // original predicate addr + len
        bgm->orig.predicates[i].pack_ref(curr, true);

        // object
        if (bgm->statuses[i] == DATASTORE_SUCCESS) {
            bgm->objects[i].pack(curr, true);
        }
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Response::BGetOp *bgm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bgm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        little_endian::encode(curr, bgm->statuses[i], sizeof(bgm->statuses[i]));
        curr += sizeof(bgm->statuses[i]);

        // num_recs
        little_endian::encode(curr, bgm->num_recs[i], sizeof(bgm->num_recs[i]));
        curr += sizeof(bgm->num_recs[i]);

        for(std::size_t j = 0; j < bgm->num_recs[i]; j++) {
            // subject
            bgm->subjects[i][j].pack(curr, true);

            // predicate
            bgm->predicates[i][j].pack(curr, true);

            // object
            if (bgm->statuses[i] == DATASTORE_SUCCESS) {
                bgm->objects[i][j].pack(curr, true);
            }
        }
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Response::BDelete *bdm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bdm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < bdm->count; i++) {
        little_endian::encode(curr, bdm->statuses[i], sizeof(bdm->statuses[i]));
        curr += sizeof(bdm->statuses[i]);

        // original subject addr + len
        bdm->orig.subjects[i].pack_ref(curr, true);

        // original predicate addr + len
        bdm->orig.predicates[i].pack_ref(curr, true);
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Response::BHistogram *bhm, void **buf, std::size_t *bufsize) {
    char *curr = nullptr;
    if (pack(static_cast<const Response::Response *>(bhm), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        return MESSAGE_ERROR;
    }

    std::size_t avail = *bufsize - (curr - (char *) *buf);
    for(std::size_t i = 0; i < bhm->count; i++) {
        little_endian::encode(curr, bhm->statuses[i], sizeof(bhm->statuses[i]));
        curr += sizeof(bhm->statuses[i]);

        if (bhm->statuses[i] == DATASTORE_SUCCESS) {
            // histogram
            bhm->histograms[i]->pack(curr, avail, nullptr);
        }
    }

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Message *msg, void **buf, std::size_t *bufsize, char **curr) {
    if (!msg || !buf || !bufsize || !curr) {
        return MESSAGE_ERROR;
    }

    *bufsize = msg->size();

    // only allocate space if a nullptr is provided; otherwise, assume *buf has enough space
    if (!*buf) {
        if (!(*buf = alloc(*bufsize))) {
            *bufsize = 0;
            return MESSAGE_ERROR;
        }
    }

    *curr = (char *) *buf;

    // copy header into *buf
    little_endian::encode(*curr, msg->direction);
    *curr += sizeof(msg->direction);

    little_endian::encode(*curr, msg->op);
    *curr += sizeof(msg->op);

    little_endian::encode(*curr, msg->src);
    *curr += sizeof(msg->src);

    little_endian::encode(*curr, msg->dst);
    *curr += sizeof(msg->dst);

    little_endian::encode(*curr, msg->count);
    *curr += sizeof(msg->count);

    return MESSAGE_SUCCESS;
}

int Packer::pack(const Request::Request *req, void **buf, std::size_t *bufsize, char **curr) {
    return pack(static_cast<const Message *>(req), buf, bufsize, curr);
}

int Packer::pack(const Response::Response *res, void **buf, std::size_t *bufsize, char **curr) {
    return pack(static_cast<const Message *>(res), buf, bufsize, curr);
}

}
