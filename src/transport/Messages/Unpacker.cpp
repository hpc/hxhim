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
        memcpy(&out->orig.subjects[i], curr, sizeof(out->orig.subjects[i]));
        curr += sizeof(out->orig.subjects[i]);

        // predicate
        out->predicates[i] = construct<RealBlob>(curr);

        // predicate addr
        memcpy(&out->orig.predicates[i], curr, sizeof(out->orig.predicates[i]));
        curr += sizeof(out->orig.predicates[i]);

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // object addr
        memcpy(&out->orig.objects[i], curr, sizeof(out->orig.objects[i]));
        curr += sizeof(out->orig.objects[i]);

        // object len addr
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

        // subject
        out->subjects[i] = construct<RealBlob>(curr);

        // subject addr
        memcpy(&out->orig.subjects[i], curr, sizeof(out->orig.subjects[i]));
        curr += sizeof(out->orig.subjects[i]);

        // predicate
        out->predicates[i] = construct<RealBlob>(curr);

        // predicate addr
        memcpy(&out->orig.predicates[i], curr, sizeof(out->orig.predicates[i]));
        curr += sizeof(out->orig.predicates[i]);

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
        memcpy(&out->orig.subjects[i]->ptr, curr, sizeof(out->orig.subjects[i]->ptr));
        curr += sizeof(out->orig.subjects[i]->ptr);

        memcpy(&out->orig.subjects[i]->len, curr, sizeof(out->orig.subjects[i]->len));
        curr += sizeof(out->orig.subjects[i]->len);

        // original predicate addr + len
        out->orig.predicates[i] = construct<ReferenceBlob>();
        memcpy(&out->orig.predicates[i]->ptr, curr, sizeof(out->orig.predicates[i]->ptr));
        curr += sizeof(out->orig.predicates[i]->ptr);

        memcpy(&out->orig.predicates[i]->len, curr, sizeof(out->orig.predicates[i]->len));
        curr += sizeof(out->orig.predicates[i]->len);

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
        memcpy(&out->orig.subjects[i]->ptr, curr, sizeof(out->orig.subjects[i]->ptr));
        curr += sizeof(out->orig.subjects[i]->ptr);

        memcpy(&out->orig.subjects[i]->len, curr, sizeof(out->orig.subjects[i]->len));
        curr += sizeof(out->orig.subjects[i]->len);

        // original predicate addr + len
        out->orig.predicates[i] = construct<ReferenceBlob>();
        memcpy(&out->orig.predicates[i]->ptr, curr, sizeof(out->orig.predicates[i]->ptr));
        curr += sizeof(out->orig.predicates[i]->ptr);

        memcpy(&out->orig.predicates[i]->len, curr, sizeof(out->orig.predicates[i]->len));
        curr += sizeof(out->orig.predicates[i]->len);

        // object type
        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // object len addr
        memcpy(&out->orig.object_lens[i], curr, sizeof(out->orig.object_lens[i]));
        curr += sizeof(out->orig.object_lens[i]);

        // object addr
        memcpy(&out->orig.objects[i], curr, sizeof(out->orig.objects[i]));
        curr += sizeof(out->orig.objects[i]);

        // object
        // unpack into user pointers
        if (out->statuses[i] == HXHIM_SUCCESS) {
            // get the length found in the packet
            std::size_t get_len = 0;
            memcpy(&get_len, curr, sizeof(get_len));
            curr += sizeof(get_len);

            // the available space for storing the object is set in object_lens by the caller
            const std::size_t buffer_len = *(out->orig.object_lens[i]);

            // get the maximum amount of data to copy
            const std::size_t copy_len = (get_len < buffer_len)?get_len:buffer_len;

            if (out->orig.object_lens[i]) {
                *(out->orig.object_lens[i]) = copy_len;
            }

            if (out->orig.objects[i]) {
                memcpy(out->orig.objects[i], curr, copy_len);
            }

            out->objects[i] = construct<ReferenceBlob>(out->orig.objects[i], *(out->orig.object_lens[i]));

            // move past all data, not just data that was placed into the buffer
            curr += get_len;
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

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        memcpy(&out->statuses[i], curr, sizeof(out->statuses[i]));
        curr += sizeof(out->statuses[i]);

        // original subject addr + len
        out->orig.subjects[i] = construct<ReferenceBlob>();
        memcpy(&out->orig.subjects[i]->ptr, curr, sizeof(out->orig.subjects[i]->ptr));
        curr += sizeof(out->orig.subjects[i]->ptr);

        memcpy(&out->orig.subjects[i]->len, curr, sizeof(out->orig.subjects[i]->len));
        curr += sizeof(out->orig.subjects[i]->len);

        // original predicate addr + len
        out->orig.predicates[i] = construct<ReferenceBlob>();
        memcpy(&out->orig.predicates[i]->ptr, curr, sizeof(out->orig.predicates[i]->ptr));
        curr += sizeof(out->orig.predicates[i]->ptr);

        memcpy(&out->orig.predicates[i]->len, curr, sizeof(out->orig.predicates[i]->len));
        curr += sizeof(out->orig.predicates[i]->len);

        out->count++;
    }

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
