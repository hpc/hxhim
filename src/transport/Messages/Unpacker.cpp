#include <cstring>

#include "transport/Messages/Unpacker.hpp"

namespace Transport {

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
        case Message::BGET2:
            {
                Request::BGet2 *out = nullptr;
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

        out->subjects[i] = construct<RealBlob>(curr);
        out->predicates[i] = construct<RealBlob>(curr);

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

        out->subjects[i] = construct<RealBlob>(curr);
        out->predicates[i] = construct<RealBlob>(curr);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::BGet2 **bgm, void *buf, const std::size_t bufsize) {
    Request::BGet2 *out = construct<Request::BGet2>();
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

        // subject + addr
        out->subjects[i] = construct<RealBlob>(curr);

        memcpy(&out->orig.subjects[i], curr, sizeof(out->orig.subjects[i]));
        curr += sizeof(out->orig.subjects[i]);

        // predicate + addr
        out->predicates[i] = construct<RealBlob>(curr);

        memcpy(&out->orig.predicates[i], curr, sizeof(out->orig.predicates[i]));
        curr += sizeof(out->orig.predicates[i]);

        // object type, addr, len addr
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        out->objects[i] = construct<RealBlob>();
        memcpy(&out->orig.objects[i], curr, sizeof(out->orig.objects[i]));
        curr += sizeof(out->orig.objects[i]);

        // get the address of the source object_len pointer, but don't deallocate it
        // since the pointer doesn't belong to the server
        memcpy(&out->orig.object_lens[i], curr, sizeof(out->orig.object_lens[i]));
        curr += sizeof(out->orig.object_lens[i]);

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

        out->subjects[i] = construct<RealBlob>(curr);
        out->predicates[i] = construct<RealBlob>(curr);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        memcpy(&out->num_recs[i], curr, sizeof(out->num_recs[i]));
        curr += sizeof(out->num_recs[i]);

        memcpy(&out->ops[i], curr, sizeof(out->ops[i]));
        curr += sizeof(out->ops[i]);

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

        out->subjects[i] = construct<RealBlob>(curr);
        out->predicates[i] = construct<RealBlob>(curr);

        out->count++;
    }

    *bdm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::BHistogram **bhist, void *buf, const std::size_t bufsize) {
    Request::BHistogram *out = construct<Request::BHistogram>();
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

        out->count++;
    }

    *bhist = out;
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
        case Message::BGET2:
            {
                Response::BGet2 *out = nullptr;
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

    out->count = count;

    // read arrays
    memcpy(out->ds_offsets, curr, sizeof(*out->ds_offsets) * out->count);
    curr += sizeof(*out->ds_offsets) * out->count;

    memcpy(out->statuses, curr, sizeof(*out->statuses) * out->count);
    // curr += sizeof(*out->statuses) * out->count);

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

        out->subjects[i] = construct<RealBlob>(curr);
        out->predicates[i] = construct<RealBlob>(curr);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if (out->statuses[i] == HXHIM_SUCCESS) {
            out->objects[i] = construct<RealBlob>(curr);
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::BGet2 **bgm, void *buf, const std::size_t bufsize) {
    Response::BGet2 *out = construct<Response::BGet2>();
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

        // subject
        out->subjects[i] = construct<RealBlob>(curr);

        // predicate
        out->predicates[i] = construct<RealBlob>(curr);

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // object len addr
        memcpy(&out->orig.object_lens[i], curr, sizeof(out->orig.object_lens[i]));
        curr += sizeof(out->orig.object_lens[i]);

        // object addr
        memcpy(&out->orig.objects[i], curr, sizeof(out->orig.objects[i]));
        curr += sizeof(out->orig.objects[i]);

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

        out->subjects[i] = construct<RealBlob>(curr);
        out->predicates[i] = construct<RealBlob>(curr);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if (out->statuses[i] == HXHIM_SUCCESS) {
            out->objects[i] = construct<RealBlob>(curr);
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

    out->count = count;

    // read arrays
    memcpy(out->ds_offsets, curr, sizeof(*out->ds_offsets) * out->count);
    curr += sizeof(*out->ds_offsets) * out->count;

    memcpy(out->statuses, curr, sizeof(*out->statuses) * out->count);
    curr += sizeof(*out->statuses) * out->count;

    *bdm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::BHistogram **bhist, void *buf, const std::size_t bufsize) {
    Response::BHistogram *out = construct<Response::BHistogram>();
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

    // read arrays
    memcpy(out->ds_offsets, curr, sizeof(*out->ds_offsets) * count);
    curr += sizeof(*out->ds_offsets) * count;

    memcpy(out->statuses, curr, sizeof(*out->statuses) * count);
    curr += sizeof(*out->statuses) * count;

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->hists[i].size, curr, sizeof(out->hists[i].size));
        curr += sizeof(out->hists[i].size);

        if (out->hists[i].size &&
            (!(out->hists[i].buckets = alloc_array<double>(out->hists[i].size))      ||
             !(out->hists[i].counts = alloc_array<std::size_t>(out->hists[i].size)))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t j = 0; j < out->hists[i].size; j++) {
            memcpy(&out->hists[i].buckets[j], curr, sizeof(out->hists[i].buckets[j]));
            curr += sizeof(out->hists[i].buckets[j]);

            memcpy(&out->hists[i].counts[j], curr, sizeof(out->hists[i].counts[j]));
            curr += sizeof(out->hists[i].counts[j]);
        }

        out->count++;
    }

    *bhist = out;
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