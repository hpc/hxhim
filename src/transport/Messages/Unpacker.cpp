#include <cstring>

#include "transport/Messages/Unpacker.hpp"

namespace Transport {

static char *unpack_addr(void **dst, char *&src) {
    // // skip check
    // if (!dst || !src) {
    //     return nullptr;
    // }

    const std::size_t len = sizeof(*dst);
    memcpy(dst, src, len);
    src += len;
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

    // mlog(THALLIUM_DBG, "Unpacking Request type %d", base->type);
    switch (base->type) {
        case Message::BPUT:
            {
                Request::BPut *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::BGET:
            {
                Request::BGet *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::BGETOP:
            {
                Request::BGetOp *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::BDELETE:
            {
                Request::BDelete *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        default:
            break;
    }

    destruct(base);
    // mlog(THALLIUM_DBG, "Done Unpacking Request type %d", base->type);

    return ret;
}

int Unpacker::unpack(Request::BPut **bpm, void *buf, const std::size_t bufsize) {
    Request::BPut *out = construct<Request::BPut>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        // subject + len
        out->subjects[i] = construct<RealBlob>(curr);

        // subject addr
        memcpy(&out->orig.subjects[i], curr, sizeof(out->orig.subjects[i]));
        curr += sizeof(out->orig.subjects[i]);

        // predicate + len
        out->predicates[i] = construct<RealBlob>(curr);

        // predicate addr
        memcpy(&out->orig.predicates[i], curr, sizeof(out->orig.predicates[i]));
        curr += sizeof(out->orig.predicates[i]);

        // object type + object + len
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);
        out->objects[i] = construct<RealBlob>(curr);

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

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        // subject
        out->subjects[i] = construct<RealBlob>(curr);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate
        out->predicates[i] = construct<RealBlob>(curr);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
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

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        // operation to run
        memcpy(&out->ops[i], curr, sizeof(out->ops[i]));
        curr += sizeof(out->ops[i]);

        if ((out->ops[i] != hxhim_get_op_t::HXHIM_GET_FIRST) &&
            (out->ops[i] != hxhim_get_op_t::HXHIM_GET_LAST))  {
            // subject
            out->subjects[i] = construct<RealBlob>(curr);

            // predicate
            out->predicates[i] = construct<RealBlob>(curr);
        }

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // number of records to get back
        memcpy(&out->num_recs[i], curr, sizeof(out->num_recs[i]));
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

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);


    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        // subject
        out->subjects[i] = construct<RealBlob>(curr);

        // subject addr
        unpack_addr(&out->orig.subjects[i], curr);

        // predicate
        out->predicates[i] = construct<RealBlob>(curr);

        // predicate addr
        unpack_addr(&out->orig.predicates[i], curr);

        out->count++;
    }

    *bdm = out;
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

    // mlog(THALLIUM_DBG, "Unpacking Response type %d", base->type);
    switch (base->type) {
        case Message::BPUT:
            {
                Response::BPut *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case Message::BGET:
            {
                Response::BGet *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
       case Message::BGETOP:
            {
                Response::BGetOp *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case Message::BDELETE:
            {
                Response::BDelete *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        default:
            break;
    }

    destruct(base);
    // mlog(THALLIUM_DBG, "Done Unpacking Response type %d", base->type);

    return ret;
}

int Unpacker::unpack(Response::BPut **bpm, void *buf, const std::size_t bufsize) {
    Response::BPut *out = construct<Response::BPut>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        memcpy(&out->statuses[i], curr, sizeof(out->statuses[i]));
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i] = construct<ReferenceBlob>();
        out->orig.subjects[i]->unpack_ref(curr);

        // original predicate addr + len
        out->orig.predicates[i] = construct<ReferenceBlob>();
        out->orig.predicates[i]->unpack_ref(curr);

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

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        memcpy(&out->statuses[i], curr, sizeof(out->statuses[i]));
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i] = construct<ReferenceBlob>();
        out->orig.subjects[i]->unpack_ref(curr);

        // original predicate addr + len
        out->orig.predicates[i] = construct<ReferenceBlob>();
        out->orig.predicates[i]->unpack_ref(curr);

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // object
        // unpack into user pointers
        if (out->statuses[i] == HXHIM_SUCCESS) {
            out->objects[i] = construct<RealBlob>(curr);
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

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        memcpy(&out->statuses[i], curr, sizeof(out->statuses[i]));
        curr += sizeof(out->statuses[i]);

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // num_recs
        memcpy(&out->num_recs[i], curr, sizeof(out->num_recs[i]));
        curr += sizeof(out->num_recs[i]);

        out->subjects[i]    = alloc_array<Blob *>(out->num_recs[i]);
        out->predicates[i]  = alloc_array<Blob *>(out->num_recs[i]);
        if (out->statuses[i] == HXHIM_SUCCESS) {
            out->objects[i] = alloc_array<Blob *>(out->num_recs[i]);
        }

        for(std::size_t j = 0; j < out->num_recs[i]; j++) {
            // subject
            out->subjects[i][j] = construct<RealBlob>(curr);

            // predicate
            out->predicates[i][j] = construct<RealBlob>(curr);

            if (out->statuses[i] == HXHIM_SUCCESS) {
                // object
                out->objects[i][j] = construct<RealBlob>(curr);
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

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        memcpy(&out->statuses[i], curr, sizeof(out->statuses[i]));
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i] = construct<ReferenceBlob>();
        out->orig.subjects[i]->unpack_ref(curr);

        // original predicate addr + len
        out->orig.predicates[i] = construct<ReferenceBlob>();
        out->orig.predicates[i]->unpack_ref(curr);

        out->count++;
    }

    *bdm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Message **msg, void *buf, const std::size_t bufsize) {
    if (!msg) {
        return TRANSPORT_ERROR;
    }

    *msg = nullptr;

    Message *out = construct<Message>(Message::NONE, Message::INVALID);
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

    memcpy(&msg->direction, *curr, sizeof(msg->direction));
    *curr += sizeof(msg->direction);

    memcpy(&msg->type, *curr, sizeof(msg->type));
    *curr += sizeof(msg->type);

    memcpy(&msg->src, *curr, sizeof(msg->src));
    *curr += sizeof(msg->src);

    memcpy(&msg->dst, *curr, sizeof(msg->dst));
    *curr += sizeof(msg->dst);

    return TRANSPORT_SUCCESS;
}

}
