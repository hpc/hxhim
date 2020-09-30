#include <cstring>
#include <memory>

#include "transport/Messages/Unpacker.hpp"
#include "utils/big_endian.hpp"

namespace Transport {

static char *unpack_addr(void **dst, char *&src) {
    // // skip check
    // if (!dst || !src) {
    //     return nullptr;
    // }

    big_endian::decode(dst, src, 1);
    src += sizeof(*dst);
    return src;
}

int Unpacker::unpack(Request::Request **req, void *buf, const std::size_t bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        // mlog(THALLIUM_WARN, "Bad address to pointer to unpack into");
        return ret;
    }

    // mlog(THALLIUM_DBG, "%s", "Unpacking Request");

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, bufsize) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // make sure the data is for a request
    if (base->direction != Message::REQUEST) {
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
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // subject + len
        out->subjects[i].unpack(curr);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate + len
        out->predicates[i].unpack(curr);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        // object type + object + len
        big_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);
        out->objects[i].unpack(curr);

        out->count++;
    }

    *bpm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::BGet **bgm, void *buf, const std::size_t bufsize) {
    Request::BGet *out = construct<Request::BGet>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // subject
        out->subjects[i].unpack(curr);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate
        out->predicates[i].unpack(curr);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        // object type
        big_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::BGetOp **bgm, void *buf, const std::size_t bufsize) {
    Request::BGetOp *out = construct<Request::BGetOp>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // operation to run
        big_endian::decode(out->ops[i], curr);
        curr += sizeof(out->ops[i]);

        if ((out->ops[i] != hxhim_getop_t::HXHIM_GETOP_FIRST) &&
            (out->ops[i] != hxhim_getop_t::HXHIM_GETOP_LAST))  {
            // subject
            out->subjects[i].unpack(curr);

            // predicate
            out->predicates[i].unpack(curr);
        }

        // object type
        big_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);

        // number of records to get back
        big_endian::decode(out->num_recs[i], curr);
        curr += sizeof(out->num_recs[i]);

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::BDelete **bdm, void *buf, const std::size_t bufsize) {
    Request::BDelete *out = construct<Request::BDelete>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        // subject
        out->subjects[i].unpack(curr);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate
        out->predicates[i].unpack(curr);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        out->count++;
    }

    *bdm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::BHistogram **bhm, void *buf, const std::size_t bufsize) {
    Request::BHistogram *out = construct<Request::BHistogram>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    out->count = out->max_count;

    *bhm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::Response **res, void *buf, const std::size_t bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Done Unpacking Response");

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, bufsize) != TRANSPORT_SUCCESS) {
        destruct(base);
        return ret;
    }

    // make sure the data is for a response
    if (base->direction != Message::RESPONSE) {
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
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        big_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i].unpack_ref(curr);

        // original predicate addr + len
        out->orig.predicates[i].unpack_ref(curr);

        out->count++;
    }

    *bpm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::BGet **bgm, void *buf, const std::size_t bufsize) {
    Response::BGet *out = construct<Response::BGet>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        big_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i].unpack_ref(curr);

        // original predicate addr + len
        out->orig.predicates[i].unpack_ref(curr);

        // object type
        big_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);

        // object
        // unpack into user pointers
        if (out->statuses[i] == DATASTORE_SUCCESS) {
            out->objects[i].unpack(curr);
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::BGetOp **bgm, void *buf, const std::size_t bufsize) {
    Response::BGetOp *out = construct<Response::BGetOp>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        big_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // object type
        big_endian::decode(out->object_types[i], curr);
        curr += sizeof(out->object_types[i]);

        // num_recs
        big_endian::decode(out->num_recs[i], curr);
        curr += sizeof(out->num_recs[i]);

        out->subjects[i]   = alloc_array<Blob>(out->num_recs[i]);
        out->predicates[i] = alloc_array<Blob>(out->num_recs[i]);
        out->objects[i]    = alloc_array<Blob>(out->num_recs[i]);

        for(std::size_t j = 0; j < out->num_recs[i]; j++) {
            // subject
            out->subjects[i][j].unpack(curr);

            // predicate
            out->predicates[i][j].unpack(curr);

            // object
            if (out->statuses[i] == DATASTORE_SUCCESS) {
                out->objects[i][j].unpack(curr);
            }
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::BDelete **bdm, void *buf, const std::size_t bufsize) {
    Response::BDelete *out = construct<Response::BDelete>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->max_count; i++) {
        big_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i].unpack_ref(curr);

        // original predicate addr + len
        out->orig.predicates[i].unpack_ref(curr);

        out->count++;
    }

    *bdm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::BHistogram **bhm, void *buf, const std::size_t bufsize) {
    Response::BHistogram *out = construct<Response::BHistogram>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    std::size_t remaining = bufsize - (curr - (char *) buf);

    for(std::size_t i = 0; i < out->max_count; i++) {
        big_endian::decode(out->statuses[i], curr);
        curr += sizeof(out->statuses[i]);

        out->histograms[i] = std::shared_ptr<Histogram::Histogram>(construct<Histogram::Histogram>(Histogram::Config{0, nullptr, nullptr}), Histogram::deleter);
        out->histograms[i]->unpack(curr, remaining, nullptr);

        out->count++;
    }

    *bhm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Message **msg, void *buf, const std::size_t bufsize) {
    if (!msg) {
        return TRANSPORT_ERROR;
    }

    *msg = nullptr;

    Message *out = construct<Message>(Message::NONE, hxhim_op_t::HXHIM_INVALID);
    char *curr = nullptr;
    if (unpack(out, buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    *msg = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Message *msg, void *buf, const std::size_t, char **curr) {
    if (!msg || !buf || !curr) {
        return TRANSPORT_ERROR;
    }

    *curr = (char *) buf;

    big_endian::decode(msg->direction, *curr);
    *curr += sizeof(msg->direction);

    big_endian::decode(msg->op, *curr);
    *curr += sizeof(msg->op);

    big_endian::decode(msg->src, *curr);
    *curr += sizeof(msg->src);

    big_endian::decode(msg->dst, *curr);
    *curr += sizeof(msg->dst);

    std::size_t count = 0;
    big_endian::decode(count, *curr);
    *curr += sizeof(msg->count);

    if (msg->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(msg);
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

}
