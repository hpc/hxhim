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
        case Message::PUT:
            {
                Request::Put *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::GET:
            {
                Request::Get *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::GET2:
            {
                Request::Get2 *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::DELETE:
            {
                Request::Delete *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
        case Message::HISTOGRAM:
            {
                Request::Histogram *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *req = out;
            }
            break;
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

int Unpacker::unpack(Request::Put **pm, void *buf, const std::size_t bufsize) {
    Request::Put *out = construct<Request::Put>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    memcpy(&out->object_len, curr, sizeof(out->object_len));
    curr += sizeof(out->object_len);

    memcpy(&out->object_type, curr, sizeof(out->object_type));
    curr += sizeof(out->object_type);

    // allocate arrays
    if ((out->subject_len && !(out->subject = alloc(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = alloc(out->predicate_len))) ||
        (out->object_len && !(out->object = alloc(out->object_len))))          {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    memcpy(out->subject, curr, out->subject_len);
    curr += out->subject_len;

    memcpy(out->predicate, curr, out->predicate_len);
    curr += out->predicate_len;

    memcpy(out->object, curr, out->object_len);
    // curr += out->object_len;

    *pm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::Get **gm, void *buf, const std::size_t bufsize) {
    Request::Get *out = construct<Request::Get>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    memcpy(&out->object_type, curr, sizeof(out->object_type));
    curr += sizeof(out->object_type);

    // allocate arrays
    if ((out->subject_len && !(out->subject = alloc(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = alloc(out->predicate_len)))) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    memcpy(out->subject, curr, out->subject_len);
    curr += out->subject_len;

    memcpy(out->predicate, curr, out->predicate_len);
    // curr += out->predicate_len;

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::Get2 **gm, void *buf, const std::size_t bufsize) {
    Request::Get2 *out = construct<Request::Get2>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    memcpy(&out->object_type, curr, sizeof(out->object_type));
    curr += sizeof(out->object_type);

    // read the address
    memcpy(&out->src_object, curr, sizeof(out->src_object));
    curr += sizeof(out->src_object);

    // read the address
    memcpy(&out->src_object_len, curr, sizeof(out->src_object_len));
    curr += sizeof(out->src_object_len);

    // get the actual length
    out->object_len = &out->dst_len;
    memcpy(out->object_len, curr, sizeof(*(out->object_len)));
    curr += sizeof(*(out->object_len));

    // allocate arrays
    if ((out->subject_len   && !(out->subject   = alloc(out->subject_len)))   ||
        (out->predicate_len && !(out->predicate = alloc(out->predicate_len))) ||
        (out->object_len    && !(out->object    = alloc(*(out->object_len)))) ||
        !out->src_object                                                      ||
        !out->src_object_len) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    memcpy(out->subject, curr, out->subject_len);
    curr += out->subject_len;

    memcpy(out->predicate, curr, out->predicate_len);
    // curr += out->predicate_len;

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::Delete **dm, void *buf, const std::size_t bufsize) {
    Request::Delete *out = construct<Request::Delete>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    // allocate arrays
    if ((out->subject_len && !(out->subject = alloc(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = alloc(out->predicate_len)))) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    memcpy(out->subject, curr, out->subject_len);
    curr += out->subject_len;

    memcpy(out->predicate, curr, out->predicate_len);
    // curr += out->predicate_len;

    *dm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Request::Histogram **hist, void *buf, const std::size_t bufsize) {
    Request::Histogram *out = construct<Request::Histogram>();
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    // curr += sizeof(out->ds_offset);

    *hist = out;
    return TRANSPORT_SUCCESS;
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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_lens[i], curr, sizeof(out->object_lens[i]));
        curr += sizeof(out->object_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if ((out->subject_lens[i]   && !(out->subjects[i]   = alloc(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i]))) ||
            (out->object_lens[i]    && !(out->objects[i]    = alloc(out->object_lens[i]))))    {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

        memcpy(out->objects[i], curr, out->object_lens[i]);
        curr += out->object_lens[i];

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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if ((out->subject_lens[i]   && !(out->subjects[i]   = alloc(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        // read the value of objects[i] (address)
        memcpy(&out->src_objects[i], curr, sizeof(out->src_objects[i]));
        curr += sizeof(out->src_objects[i]);

        // read the value of object_lens[i] (address)
        memcpy(&out->src_object_lens[i], curr, sizeof(out->src_object_lens[i]));
        curr += sizeof(out->src_object_lens[i]);

        if ((out->subject_lens[i]   && !(out->subjects[i]   = alloc(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        memcpy(&out->num_recs[i], curr, sizeof(out->num_recs[i]));
        curr += sizeof(out->num_recs[i]);

        memcpy(&out->ops[i], curr, sizeof(out->ops[i]));
        curr += sizeof(out->ops[i]);

        if ((out->subject_lens[i]   && !(out->subjects[i]   = alloc(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        if ((out->subject_lens[i]   && !(out->subjects[i]   = alloc(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

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
        case Message::PUT:
            {
                Response::Put *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case Message::GET:
            {
                Response::Get *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case Message::GET2:
            {
                Response::Get2 *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case Message::DELETE:
            {
                Response::Delete *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
        case Message::HISTOGRAM:
            {
                Response::Histogram *out = nullptr;
                ret = unpack(&out, buf, bufsize);
                *res = out;
            }
            break;
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

int Unpacker::unpack(Response::Put **pm, void *buf, const std::size_t bufsize) {
    Response::Put *out = construct<Response::Put>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->status, curr, sizeof(out->status));
    // curr += sizeof(out->status);

    *pm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::Get **gm, void *buf, const std::size_t bufsize) {
    Response::Get *out = construct<Response::Get>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    memcpy(&out->object_type, curr, sizeof(out->object_type));
    curr += sizeof(out->object_type);

    // allocate arrays
    if ((out->subject_len && !(out->subject = alloc(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = alloc(out->predicate_len)))) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    memcpy(out->subject, curr, out->subject_len);
    curr += out->subject_len;

    memcpy(out->predicate, curr, out->predicate_len);
    curr += out->predicate_len;

    // only read the rest of the data if the status is TRANSPORT_SUCCESS
    if (out->status == HXHIM_SUCCESS) {
        memcpy(&out->object_len, curr, sizeof(out->object_len));
        curr += sizeof(out->object_len);

        // allocate arrays
        if ((out->object_len && !(out->object = alloc(out->object_len)))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        // read arrays
        memcpy(out->object, curr, out->object_len);
        curr += out->object_len;
    }

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::Get2 **gm, void *buf, const std::size_t bufsize) {
    Response::Get2 *out = construct<Response::Get2>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    memcpy(&out->object_type, curr, sizeof(out->object_type));
    curr += sizeof(out->object_type);

    // allocate arrays
    if ((out->subject_len && !(out->subject = alloc(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = alloc(out->predicate_len)))) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    memcpy(out->subject, curr, out->subject_len);
    curr += out->subject_len;

    memcpy(out->predicate, curr, out->predicate_len);
    curr += out->predicate_len;

    // only read the rest of the data if the status is TRANSPORT_SUCCESS
    if (out->status == HXHIM_SUCCESS) {
        // read the address of the object (should still be valid)
        memcpy(&out->object, curr, sizeof(out->object));
        curr += sizeof(out->object);

        // read the address of the object_len (should still be valid)
        memcpy(&out->object_len, curr, sizeof(out->object_len));
        curr += sizeof(out->object_len);

        // read the received length
        memcpy(out->object_len, curr, sizeof(*(out->object_len)));
        curr += sizeof(*(out->object_len));

        // read the received data
        memcpy(out->object, curr, *(out->object_len));
        curr += *(out->object_len);
    }

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::Delete **dm, void *buf, const std::size_t bufsize) {
    Response::Delete *out = construct<Response::Delete>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->status, curr, sizeof(out->status));
    // curr += sizeof(out->status);

    *dm = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(Response::Histogram **hist, void *buf, const std::size_t bufsize) {
    Response::Histogram *out = construct<Response::Histogram>();
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->hist.size, curr, sizeof(out->hist.size));
    curr += sizeof(out->hist.size);

    if (out->hist.size &&
        (!(out->hist.buckets = alloc_array<double>(out->hist.size))      ||
         !(out->hist.counts = alloc_array<std::size_t>(out->hist.size)))) {
        destruct(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->hist.size; i++) {
        memcpy(&out->hist.buckets[i], curr, sizeof(out->hist.buckets[i]));
        curr += sizeof(out->hist.buckets[i]);

        memcpy(&out->hist.counts[i], curr, sizeof(out->hist.counts[i]));
        curr += sizeof(out->hist.counts[i]);
    }

    *hist = out;
    return TRANSPORT_SUCCESS;
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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if ((out->subject_lens[i] && !(out->subjects[i] = alloc(out->subject_lens[i])))       ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

        if (out->statuses[i] == HXHIM_SUCCESS) {
            memcpy(&out->object_lens[i], curr, sizeof(out->object_lens[i]));
            curr += sizeof(out->subject_lens[i]);

            if (!(out->objects[i] = alloc(out->object_lens[i]))) {
                destruct(out);
                return TRANSPORT_ERROR;
            }

            memcpy(out->objects[i], curr, out->object_lens[i]);
            curr += out->object_lens[i];
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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if ((out->subject_lens[i] && !(out->subjects[i] = alloc(out->subject_lens[i])))       ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

        if (out->statuses[i] == HXHIM_SUCCESS) {
            // read the address of the object (should still be valid)
            memcpy(&out->objects[i], curr, sizeof(out->objects[i]));
            curr += sizeof(out->objects[i]);

            // read the address of the object_len (should still be valid)
            memcpy(&out->object_lens[i], curr, sizeof(out->object_lens[i]));
            curr += sizeof(out->object_lens[i]);

            memcpy(out->object_lens[i], curr, sizeof(*(out->object_lens[i])));
            curr += sizeof(*(out->object_lens[i]));

            memcpy(out->objects[i], curr, *(out->object_lens[i]));
            curr += *(out->object_lens[i]);
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

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        memcpy(&out->object_types[i], curr, sizeof(out->object_types[i]));
        curr += sizeof(out->object_types[i]);

        if ((out->subject_lens[i] && !(out->subjects[i] = alloc(out->subject_lens[i])))       ||
            (out->predicate_lens[i] && !(out->predicates[i] = alloc(out->predicate_lens[i])))) {
            destruct(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

        if (out->statuses[i] == HXHIM_SUCCESS) {
            memcpy(&out->object_lens[i], curr, sizeof(out->object_lens[i]));
            curr += sizeof(out->object_lens[i]);

            if (!(out->objects[i] = alloc(out->object_lens[i]))) {
                destruct(out);
                return TRANSPORT_ERROR;
            }

            memcpy(out->objects[i], curr, out->object_lens[i]);
            curr += out->object_lens[i];
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

    msg->clean = true;

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
