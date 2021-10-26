#include <memory>

#include "datastore/constants.hpp"
#include "message/Unpacker.hpp"
#include "utils/little_endian.hpp"
#include "utils/memory.hpp"

namespace Message {

static char *unpack_addr(void **dst, char *&src) {
    // // skip check
    // if (!dst || !src) {
    //     return nullptr;
    // }

    little_endian::decode(dst, src, 1);
    src += sizeof(*dst);
    return src;
}

int Unpacker::unpack(Request::Request **req, void *buf, const std::size_t bufsize) {
    int ret = MESSAGE_ERROR;
    if (!req) {
        // mlog(THALLIUM_WARN, "Bad address to pointer to unpack into");
        return ret;
    }

    // mlog(THALLIUM_DBG, "%s", "Unpacking Request");

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, bufsize) != MESSAGE_SUCCESS) {
        return ret;
    }

    // make sure the data is for a request
    if (base->direction != Direction::REQUEST) {
        destruct(base);
        return ret;
    }

    *req = nullptr;

    // mlog(THALLIUM_DBG, "Unpacking Request type %d", base->op);
    switch (base->op) {
        case hxhim_op_t::HXHIM_PUT:
            {
                Request::BPut *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case hxhim_op_t::HXHIM_GET:
            {
                Request::BGet *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case hxhim_op_t::HXHIM_GETOP:
            {
                Request::BGetOp *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case hxhim_op_t::HXHIM_DELETE:
            {
                Request::BDelete *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            {
                Request::BHistogram *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        default:
            break;
    }

    destruct(base);
    // mlog(THALLIUM_DBG, "Done Unpacking Request type %d", base->op);

    return ret;
}

int Unpacker::unpack(Request::BPut **bpm, void *buf, const std::size_t bufsize) {
    Request::BPut *out = construct<Request::BPut>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // subject + len
        out->subjects[i].unpack(curr, true);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate + len
        out->predicates[i].unpack(curr, true);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        // object + len
        out->objects[i].unpack(curr, true);

        out->count++;
    }

    *bpm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Request::BGet **bgm, void *buf, const std::size_t bufsize) {
    Request::BGet *out = construct<Request::BGet>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // subject
        out->subjects[i].unpack(curr, true);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate
        out->predicates[i].unpack(curr, true);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        // object type
        little_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);

        out->count++;
    }

    *bgm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Request::BGetOp **bgm, void *buf, const std::size_t bufsize) {
    Request::BGetOp *out = construct<Request::BGetOp>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // operation to run
        little_endian::decode(out->ops[i], curr);
        curr += sizeof(out->ops[i]);

        if ((out->ops[i] != hxhim_getop_t::HXHIM_GETOP_FIRST) &&
            (out->ops[i] != hxhim_getop_t::HXHIM_GETOP_LAST))  {
            // subject
            out->subjects[i].unpack(curr, true);

            // predicate
            out->predicates[i].unpack(curr, true);
        }

        // object type
        little_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);

        // number of records to get back
        little_endian::decode(out->num_recs[i], curr);
        curr += sizeof(out->num_recs[i]);

        out->count++;
    }

    *bgm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Request::BDelete **bdm, void *buf, const std::size_t bufsize) {
    Request::BDelete *out = construct<Request::BDelete>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // subject
        out->subjects[i].unpack(curr, true);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate
        out->predicates[i].unpack(curr, true);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        out->count++;
    }

    *bdm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Request::BHistogram **bhm, void *buf, const std::size_t bufsize) {
    Request::BHistogram *out = construct<Request::BHistogram>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // histogram names
        out->names[i].unpack(curr, false);

        out->count++;
    }

    *bhm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Response::Response **res, void *buf, const std::size_t bufsize) {
    int ret = MESSAGE_ERROR;
    if (!res) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Done Unpacking Response");

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, bufsize) != MESSAGE_SUCCESS) {
        destruct(base);
        return ret;
    }

    // make sure the data is for a response
    if (base->direction != Direction::RESPONSE) {
        destruct(base);
        return ret;
    }

    *res = nullptr;

    // mlog(THALLIUM_DBG, "Unpacking Response type %d", base->op);
    switch (base->op) {
        case hxhim_op_t::HXHIM_PUT:
            {
                Response::BPut *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case hxhim_op_t::HXHIM_GET:
            {
                Response::BGet *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
       case hxhim_op_t::HXHIM_GETOP:
            {
                Response::BGetOp *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case hxhim_op_t::HXHIM_DELETE:
            {
                Response::BDelete *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case hxhim_op_t::HXHIM_HISTOGRAM:
            {
                Response::BHistogram *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        default:
            break;
    }

    destruct(base);
    // mlog(THALLIUM_DBG, "Done Unpacking Response type %d", base->op);

    return ret;
}

int Unpacker::unpack(Response::BPut **bpm, void *buf, const std::size_t bufsize) {
    Response::BPut *out = construct<Response::BPut>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        little_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i].unpack_ref(curr, true);

        // original predicate addr + len
        out->orig.predicates[i].unpack_ref(curr, true);

        out->count++;
    }

    *bpm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Response::BGet **bgm, void *buf, const std::size_t bufsize) {
    Response::BGet *out = construct<Response::BGet>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        little_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i].unpack_ref(curr, true);

        // original predicate addr + len
        out->orig.predicates[i].unpack_ref(curr, true);

        // object
        // unpack into user pointers
        if (out->statuses[i] == DATASTORE_SUCCESS) {
            out->objects[i].unpack(curr, true);
        }

        out->count++;
    }

    *bgm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Response::BGetOp **bgm, void *buf, const std::size_t bufsize) {
    Response::BGetOp *out = construct<Response::BGetOp>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        little_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // num_recs
        little_endian::decode(out->num_recs[i], curr);
        curr += sizeof(out->num_recs[i]);

        out->subjects[i]   = alloc_array<Blob>(out->num_recs[i]);
        out->predicates[i] = alloc_array<Blob>(out->num_recs[i]);
        out->objects[i]    = alloc_array<Blob>(out->num_recs[i]);

        for(std::size_t j = 0; j < out->num_recs[i]; j++) {
            // subject
            out->subjects[i][j].unpack(curr, true);

            // predicate
            out->predicates[i][j].unpack(curr, true);

            // object
            if (out->statuses[i] == DATASTORE_SUCCESS) {
                out->objects[i][j].unpack(curr, true);
            }
        }

        out->count++;
    }

    *bgm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Response::BDelete **bdm, void *buf, const std::size_t bufsize) {
    Response::BDelete *out = construct<Response::BDelete>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        little_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i].unpack_ref(curr, true);

        // original predicate addr + len
        out->orig.predicates[i].unpack_ref(curr, true);

        out->count++;
    }

    *bdm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Response::BHistogram **bhm, void *buf, const std::size_t bufsize) {
    Response::BHistogram *out = construct<Response::BHistogram>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    std::size_t remaining = bufsize - (curr - (char *) buf);
    for(std::size_t i = 0; i < out->max_count; i++) {
        little_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        if (out->statuses[i] == DATASTORE_SUCCESS) {
            out->histograms[i] = std::shared_ptr<Histogram::Histogram>(construct<Histogram::Histogram>(Histogram::Config{0, nullptr, nullptr}, ""), Histogram::deleter);
            out->histograms[i]->unpack(curr, remaining, nullptr);
        }

        out->count++;
    }

    *bhm = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Message **msg, void *buf, const std::size_t bufsize) {
    if (!msg) {
        return MESSAGE_ERROR;
    }

    *msg = nullptr;

    Message *out = construct<Message>(Direction::NONE, hxhim_op_t::HXHIM_INVALID);
    char *curr = nullptr;
    if (unpack(out, buf, bufsize, &curr) != MESSAGE_SUCCESS) {
        destruct(out);
        return MESSAGE_ERROR;
    }

    *msg = out;
    return MESSAGE_SUCCESS;
}

int Unpacker::unpack(Message *msg, void *buf, const std::size_t, char **curr) {
    if (!msg || !buf || !curr) {
        return MESSAGE_ERROR;
    }

    *curr = (char *) buf;

    little_endian::decode(msg->direction, *curr);
    *curr += sizeof(msg->direction);

    little_endian::decode(msg->op, *curr);
    *curr += sizeof(msg->op);

    little_endian::decode(msg->src, *curr);
    *curr += sizeof(msg->src);

    little_endian::decode(msg->dst, *curr);
    *curr += sizeof(msg->dst);

    std::size_t count = 0;
    little_endian::decode(count, *curr);
    *curr += sizeof(msg->count);

    msg->alloc(count);

    return MESSAGE_SUCCESS;
}

}
