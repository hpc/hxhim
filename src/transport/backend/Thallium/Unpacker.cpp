#if HXHIM_HAVE_THALLIUM

#include <cstring>

#include "transport/backend/Thallium/Unpacker.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

int Transport::Thallium::Unpacker::unpack(Request::Request **req, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        // mlog(THALLIUM_WARN, "Bad address to pointer to unpack into");
        return ret;
    }

    // mlog(THALLIUM_DBG, "%s", "Unpacking Request");

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, bufsize, requests, arrays, buffers) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // make sure the data is for a request
    if (base->direction != Message::REQUEST) {
        requests->release(base);
        return ret;
    }

    *req = nullptr;

    // mlog(THALLIUM_DBG, "Unpacking Request type %d", base->type);
    switch (base->type) {
        case Message::PUT:
            {
                Request::Put *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::GET:
            {
                Request::Get *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::DELETE:
            {
                Request::Delete *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::HISTOGRAM:
            {
                Request::Histogram *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BPUT:
            {
                Request::BPut *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BGET:
            {
                Request::BGet *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BGETOP:
            {
                Request::BGetOp *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BDELETE:
            {
                Request::BDelete *out = nullptr;
                ret = unpack(&out, buf, bufsize, requests, arrays, buffers);
                *req = out;
            }
            break;
        default:
            break;
    }

    requests->release(base);
    // mlog(THALLIUM_DBG, "Done Unpacking Request type %d", base->type);

    return ret;
}

int Transport::Thallium::Unpacker::unpack(Request::Put **pm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Put *out = requests->acquire<Request::Put>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
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
    if ((out->subject_len && !(out->subject = buffers->acquire(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = buffers->acquire(out->predicate_len))) ||
        (out->object_len && !(out->object = buffers->acquire(out->object_len))))          {
        requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::Get **gm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Get *out = requests->acquire<Request::Get>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
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
    if ((out->subject_len && !(out->subject = buffers->acquire(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = buffers->acquire(out->predicate_len)))) {
        requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::Delete **dm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Delete *out = requests->acquire<Request::Delete>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
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
    if ((out->subject_len && !(out->subject = buffers->acquire(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = buffers->acquire(out->predicate_len)))) {
        requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::Histogram **hist, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Histogram *out = requests->acquire<Request::Histogram>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    // curr += sizeof(out->ds_offset);

    *hist = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::BPut **bpm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BPut *out = requests->acquire<Request::BPut>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
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

        if ((out->subject_lens[i]   && !(out->subjects[i]   = buffers->acquire(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = buffers->acquire(out->predicate_lens[i]))) ||
            (out->object_lens[i]    && !(out->objects[i]    = buffers->acquire(out->object_lens[i]))))    {
            requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::BGet **bgm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BGet *out = requests->acquire<Request::BGet>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
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

        if ((out->subject_lens[i]   && !(out->subjects[i]   = buffers->acquire(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))) {
            requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::BGetOp **bgm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BGetOp *out = requests->acquire<Request::BGetOp>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
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

        if ((out->subject_lens[i]   && !(out->subjects[i]   = buffers->acquire(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))) {
            requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::BDelete **bdm, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BDelete *out = requests->acquire<Request::BDelete>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);


    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        memcpy(&out->ds_offsets[i], curr, sizeof(out->ds_offsets[i]));
        curr += sizeof(out->ds_offsets[i]);

        memcpy(&out->subject_lens[i], curr, sizeof(out->subject_lens[i]));
        curr += sizeof(out->subject_lens[i]);

        memcpy(&out->predicate_lens[i], curr, sizeof(out->predicate_lens[i]));
        curr += sizeof(out->predicate_lens[i]);

        if ((out->subject_lens[i]   && !(out->subjects[i]   = buffers->acquire(out->subject_lens[i])))   ||
            (out->predicate_lens[i] && !(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))) {
            requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Request::BHistogram **bhist, void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BHistogram *out = requests->acquire<Request::BHistogram>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Request::Request *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
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

int Transport::Thallium::Unpacker::unpack(Response::Response **res, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    // mlog(THALLIUM_DBG, "Done Unpacking Response");

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, bufsize, responses, arrays, buffers) != TRANSPORT_SUCCESS) {
        responses->release(base);
        return ret;
    }

    // make sure the data is for a response
    if (base->direction != Message::RESPONSE) {
        responses->release(base);
        return ret;
    }

    *res = nullptr;

    // mlog(THALLIUM_DBG, "Unpacking Response type %d", base->type);
    switch (base->type) {
        case Message::PUT:
            {
                Response::Put *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::GET:
            {
                Response::Get *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::DELETE:
            {
                Response::Delete *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::HISTOGRAM:
            {
                Response::Histogram *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BPUT:
            {
                Response::BPut *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BGET:
            {
                Response::BGet *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BGETOP:
            {
                Response::BGetOp *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BDELETE:
            {
                Response::BDelete *out = nullptr;
                ret = unpack(&out, buf, bufsize, responses, arrays, buffers);
                *res = out;
            }
            break;
        default:
            break;
    }

    responses->release(base);
    // mlog(THALLIUM_DBG, "Done Unpacking Response type %d", base->type);

    return ret;
}

int Transport::Thallium::Unpacker::unpack(Response::Put **pm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Put *out = responses->acquire<Response::Put>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    *pm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Get **gm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Get *out = responses->acquire<Response::Get>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->subject_len, curr, sizeof(out->subject_len));
    curr += sizeof(out->subject_len);

    memcpy(&out->predicate_len, curr, sizeof(out->predicate_len));
    curr += sizeof(out->predicate_len);

    memcpy(&out->object_type, curr, sizeof(out->object_type));
    curr += sizeof(out->object_type);

    // allocate arrays
    if ((out->subject_len && !(out->subject = buffers->acquire(out->subject_len)))       ||
        (out->predicate_len && !(out->predicate = buffers->acquire(out->predicate_len)))) {
        responses->release(out);
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
        if ((out->object_len && !(out->object = buffers->acquire(out->object_len)))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        // read arrays
        memcpy(out->object, curr, out->object_len);
        curr += out->object_len;
    }

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Delete **dm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Delete *out = responses->acquire<Response::Delete>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    *dm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Histogram **hist, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Histogram *out = responses->acquire<Response::Histogram>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    memcpy(&out->status, curr, sizeof(out->status));
    curr += sizeof(out->status);

    memcpy(&out->ds_offset, curr, sizeof(out->ds_offset));
    curr += sizeof(out->ds_offset);

    memcpy(&out->hist.size, curr, sizeof(out->hist.size));
    curr += sizeof(out->hist.size);

    if (out->hist.size &&
        (!(out->hist.buckets = arrays->acquire_array<double>(out->hist.size))      ||
         !(out->hist.counts = arrays->acquire_array<std::size_t>(out->hist.size)))) {
        responses->release(out);
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

int Transport::Thallium::Unpacker::unpack(Response::BPut **bpm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BPut *out = responses->acquire<Response::BPut>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
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

int Transport::Thallium::Unpacker::unpack(Response::BGet **bgm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BGet *out = responses->acquire<Response::BGet>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
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

        if ((out->subject_lens[i] && !(out->subjects[i] = buffers->acquire(out->subject_lens[i])))       ||
            (out->predicate_lens[i] && !(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

        if (out->statuses[i] == HXHIM_SUCCESS) {
            memcpy(&out->object_lens[i], curr, sizeof(out->object_lens[i]));
            curr += sizeof(out->subject_lens[i]);

            if (!(out->objects[i] = buffers->acquire(out->object_lens[i]))) {
                responses->release(out);
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

int Transport::Thallium::Unpacker::unpack(Response::BGetOp **bgm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BGetOp *out = responses->acquire<Response::BGetOp>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
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

        if ((out->subject_lens[i] && !(out->subjects[i] = buffers->acquire(out->subject_lens[i])))       ||
            (out->predicate_lens[i] && !(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        memcpy(out->subjects[i], curr, out->subject_lens[i]);
        curr += out->subject_lens[i];

        memcpy(out->predicates[i], curr, out->predicate_lens[i]);
        curr += out->predicate_lens[i];

        if (out->statuses[i] == HXHIM_SUCCESS) {
            memcpy(&out->object_lens[i], curr, sizeof(out->object_lens[i]));
            curr += sizeof(out->object_lens[i]);

            if (!(out->objects[i] = buffers->acquire(out->object_lens[i]))) {
                responses->release(out);
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

int Transport::Thallium::Unpacker::unpack(Response::BDelete **bdm, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BDelete *out = responses->acquire<Response::BDelete>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
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

int Transport::Thallium::Unpacker::unpack(Response::BHistogram **bhist, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BHistogram *out = responses->acquire<Response::BHistogram>(arrays, buffers);
    char *curr = nullptr;
    if (unpack(static_cast<Response::Response *>(out), buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    memcpy(&count, curr, sizeof(out->count));
    curr += sizeof(out->count);

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
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
            (!(out->hists[i].buckets = arrays->acquire_array<double>(out->hists[i].size))      ||
             !(out->hists[i].counts = arrays->acquire_array<std::size_t>(out->hists[i].size)))) {
            responses->release(out);
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

int Transport::Thallium::Unpacker::unpack(Message **msg, void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!msg) {
        return TRANSPORT_ERROR;
    }

    *msg = nullptr;

    Message *out = responses->acquire<Message>(Message::NONE, Message::INVALID, arrays, buffers);
    char *curr = nullptr;
    if (unpack(out, buf, bufsize, &curr) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    *msg = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Message *msg, void *buf, const std::size_t, char **curr) {
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

#endif
