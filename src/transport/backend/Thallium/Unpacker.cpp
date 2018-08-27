#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/Unpacker.hpp"

int Transport::Thallium::Unpacker::unpack(Request::Request **req, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, requests, arrays, buffers) != TRANSPORT_SUCCESS) {
        delete base;
        return ret;
    }

    // make sure the data is for a request
    if (base->direction != Message::REQUEST) {
        delete base;
        return ret;
    }

    ret = unpack(req, buf, base->type, requests, arrays, buffers);

    delete base;
    return ret;
}

int Transport::Thallium::Unpacker::unpack(Request::Put **pm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Put *out = requests->acquire<Request::Put>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))
        .read((char *) &out->subject_len, sizeof(out->subject_len))
        .read((char *) &out->predicate_len, sizeof(out->predicate_len))
        .read((char *) &out->object_len, sizeof(out->object_len))
        .read((char *) &out->object_type, sizeof(out->object_type))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate arrays
    if (!(out->subject = buffers->acquire(out->subject_len))     ||
        !(out->predicate = buffers->acquire(out->predicate_len)) ||
        !(out->object = buffers->acquire(out->object_len)))       {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    if (!s
        .read((char *) out->subject, out->subject_len)
        .read((char *) out->predicate, out->predicate_len)
        .read((char *) out->object, out->object_len)) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    *pm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::Get **gm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Get *out = requests->acquire<Request::Get>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))
        .read((char *) &out->subject_len, sizeof(out->subject_len))
        .read((char *) &out->predicate_len, sizeof(out->predicate_len))
        .read((char *) &out->object_type, sizeof(out->object_type))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate arrays
    if (!(out->subject = buffers->acquire(out->subject_len))     ||
        !(out->predicate = buffers->acquire(out->predicate_len))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    if (!s
        .read((char *) out->subject, out->subject_len)
        .read((char *) out->predicate, out->predicate_len)) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::Delete **dm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Delete *out = requests->acquire<Request::Delete>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))
        .read((char *) &out->subject_len, sizeof(out->subject_len))
        .read((char *) &out->predicate_len, sizeof(out->predicate_len))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate arrays
    if (!(out->subject = buffers->acquire(out->subject_len))     ||
        !(out->predicate = buffers->acquire(out->predicate_len))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    if (!s
        .read((char *) out->subject, out->subject_len)
        .read((char *) out->predicate, out->predicate_len)) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    *dm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::Histogram **hist, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::Histogram *out = requests->acquire<Request::Histogram>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    *hist = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::BPut **bpm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BPut *out = requests->acquire<Request::BPut>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))
            .read((char *) &out->subject_lens[i], sizeof(out->subject_lens[i]))
            .read((char *) &out->predicate_lens[i], sizeof(out->predicate_lens[i]))
            .read((char *) &out->object_lens[i], sizeof(out->object_lens[i]))
            .read((char *) &out->object_types[i], sizeof(out->object_types[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (!(out->subjects[i]   = buffers->acquire(out->subject_lens[i]))   ||
            !(out->predicates[i] = buffers->acquire(out->predicate_lens[i])) ||
            !(out->objects[i]    = buffers->acquire(out->object_lens[i])))    {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (!s
            .read((char *) out->subjects[i], out->subject_lens[i])
            .read((char *) out->predicates[i], out->predicate_lens[i])
            .read((char *) out->objects[i], out->object_lens[i])) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        out->count++;
    }

    *bpm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::BGet **bgm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BGet *out = requests->acquire<Request::BGet>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))
            .read((char *) &out->subject_lens[i], sizeof(out->subject_lens[i]))
            .read((char *) &out->predicate_lens[i], sizeof(out->predicate_lens[i]))
            .read((char *) &out->object_types[i], sizeof(out->object_types[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (!(out->subjects[i]   = buffers->acquire(out->subject_lens[i]))   ||
            !(out->predicates[i] = buffers->acquire(out->predicate_lens[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (!s
            .read((char *) out->subjects[i], out->subject_lens[i])
            .read((char *) out->predicates[i], out->predicate_lens[i])) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::BGetOp **bgm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BGetOp *out = requests->acquire<Request::BGetOp>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))
            .read((char *) &out->subject_lens[i], sizeof(out->subject_lens[i]))
            .read((char *) &out->predicate_lens[i], sizeof(out->predicate_lens[i]))
            .read((char *) &out->object_types[i], sizeof(out->object_types[i]))
            .read((char *) &out->num_recs[i], sizeof(out->num_recs[i]))
            .read((char *) &out->ops[i], sizeof(out->ops[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (!(out->subjects[i]   = buffers->acquire(out->subject_lens[i]))   ||
            !(out->predicates[i] = buffers->acquire(out->predicate_lens[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (!s
            .read((char *) out->subjects[i], out->subject_lens[i])
            .read((char *) out->predicates[i], out->predicate_lens[i])) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Request::BDelete **bdm, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BDelete *out = requests->acquire<Request::BDelete>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))
            .read((char *) &out->subject_lens[i], sizeof(out->subject_lens[i]))
            .read((char *) &out->predicate_lens[i], sizeof(out->predicate_lens[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }


        if (!(out->subjects[i]   = buffers->acquire(out->subject_lens[i]))   ||
            !(out->predicates[i] = buffers->acquire(out->predicate_lens[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
        if (!s
            .read((char *) out->subjects[i], out->subject_lens[i])
            .read((char *) out->predicates[i], out->predicate_lens[i])) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        out->count++;
    }

    *bdm = out;
    return TRANSPORT_SUCCESS;
}


int Transport::Thallium::Unpacker::unpack(Request::BHistogram **bhist, const std::string &buf, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BHistogram *out = requests->acquire<Request::BHistogram>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Request::Request *>(out), s) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        out->count++;
    }

    *bhist = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Response **res, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    // partial unpacking
    Message *base = nullptr;
    if (unpack(&base, buf, responses, arrays, buffers) != TRANSPORT_SUCCESS) {
        delete base;
        return ret;
    }

    // make sure the data is for a response
    if (base->direction != Message::RESPONSE) {
        delete base;
        return ret;
    }

    ret = unpack(res, buf, base->type, responses, arrays, buffers);
    delete base;
    return ret;
}

int Transport::Thallium::Unpacker::unpack(Response::Put **pm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Put *out = responses->acquire<Response::Put>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->status, sizeof(out->status))
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    *pm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Get **gm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Get *out = responses->acquire<Response::Get>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->status, sizeof(out->status))
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))
        .read((char *) &out->subject_len, sizeof(out->subject_len))
        .read((char *) &out->predicate_len, sizeof(out->predicate_len))
        .read((char *) &out->object_type, sizeof(out->object_type))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate arrays
    if (!(out->subject = buffers->acquire(out->subject_len))     ||
        !(out->predicate = buffers->acquire(out->predicate_len))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    if (!s
        .read((char *) out->subject, out->subject_len)
        .read((char *) out->predicate, out->predicate_len)) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // only read the rest of the data if the status is TRANSPORT_SUCCESS
    if (out->status == HXHIM_SUCCESS) {
        if (!s
            .read((char *) &out->object_len, sizeof(out->object_len))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        // allocate arrays
        if (!(out->object = buffers->acquire(out->object_len))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        // read arrays
        if (!s
            .read((char *) out->object, out->object_len)) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }
    }

    *gm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Delete **dm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Delete *out = responses->acquire<Response::Delete>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->status, sizeof(out->status))
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    *dm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::Histogram **hist, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Histogram *out = responses->acquire<Response::Histogram>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    if (!s
        .read((char *) &out->status, sizeof(out->status))
        .read((char *) &out->ds_offset, sizeof(out->ds_offset))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if (!s
        .read((char *) &out->hist.size, sizeof(out->hist.size))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if (!(out->hist.buckets = arrays->acquire_array<double>(out->hist.size))     ||
        !(out->hist.counts = arrays->acquire_array<std::size_t>(out->hist.size))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->hist.size; i++) {
        if (!s
            .read((char *) &out->hist.buckets[i], sizeof(out->hist.buckets[i]))
            .read((char *) &out->hist.counts[i], sizeof(out->hist.counts[i]))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }
    }

    *hist = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::BPut **bpm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BPut *out = responses->acquire<Response::BPut>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    out->count = count;

    // read arrays
    if (!s
        .read((char *) out->ds_offsets, sizeof(*out->ds_offsets) * out->count)
        .read((char *) out->statuses, sizeof(*out->statuses) * out->count)) {
        return TRANSPORT_ERROR;
    }

    *bpm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::BGet **bgm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BGet *out = responses->acquire<Response::BGet>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))
            .read((char *) &out->statuses[i], sizeof(out->statuses[i]))
            .read((char *) &out->subject_lens[i], sizeof(out->subject_lens[i]))
            .read((char *) &out->predicate_lens[i], sizeof(out->predicate_lens[i]))
            .read((char *) &out->object_types[i], sizeof(out->object_types[i]))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (!(out->subjects[i] = buffers->acquire(out->subject_lens[i]))     ||
            !(out->predicates[i] = buffers->acquire(out->predicate_lens[i]))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (!s
            .read((char *) out->subjects[i], out->subject_lens[i])
            .read((char *) out->predicates[i], out->predicate_lens[i])) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (out->statuses[i] == HXHIM_SUCCESS) {
            if (!s
                .read((char *) &out->object_lens[i], sizeof(out->object_lens[i]))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (!(out->objects[i] = buffers->acquire(out->object_lens[i]))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (!s
                .read((char *) out->objects[i], out->object_lens[i])) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::BGetOp **bgm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BGetOp *out = responses->acquire<Response::BGetOp>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->ds_offsets[i], sizeof(out->ds_offsets[i]))
            .read((char *) &out->statuses[i], sizeof(out->statuses[i]))
            .read((char *) &out->subject_lens[i], sizeof(out->subject_lens[i]))
            .read((char *) &out->predicate_lens[i], sizeof(out->predicate_lens[i]))
            .read((char *) &out->object_types[i], sizeof(out->object_types[i]))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (!(out->subjects[i] = buffers->acquire(out->subject_lens[i]))     ||
            !(out->predicates[i] = buffers->acquire(out->predicate_lens[i]))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (!s
            .read((char *) out->subjects[i], out->subject_lens[i])
            .read((char *) out->predicates[i], out->predicate_lens[i])) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (out->statuses[i] == HXHIM_SUCCESS) {
            if (!s
                .read((char *) &out->object_lens[i], sizeof(out->object_lens[i]))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (!(out->objects[i] = buffers->acquire(out->object_lens[i]))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (!s
                .read((char *) out->objects[i], out->object_lens[i])) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }
        }

        out->count++;
    }

    *bgm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::BDelete **bdm, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BDelete *out = responses->acquire<Response::BDelete>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    out->count = count;

    // read arrays
    if (!s
        .read((char *) out->ds_offsets, sizeof(*out->ds_offsets) * out->count)
        .read((char *) out->statuses, sizeof(*out->statuses) * out->count)) {
        return TRANSPORT_ERROR;
    }

    *bdm = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Response::BHistogram **bhist, const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BHistogram *out = responses->acquire<Response::BHistogram>(arrays, buffers);
    std::stringstream s(buf);
    if (unpack(static_cast<Response::Response *>(out), s) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read non array data
    std::size_t count = 0;
    if (!s.read((char *) &count, sizeof(out->count))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // allocate space
    if (out->alloc(count) != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // read arrays
    if (!s
        .read((char *) out->ds_offsets, sizeof(*out->ds_offsets) * count)
        .read((char *) out->statuses, sizeof(*out->statuses) * count)) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < count; i++) {
        if (!s
            .read((char *) &out->hists[i].size, sizeof(out->hists[i].size))) {
            return TRANSPORT_ERROR;
        }

        if (!(out->hists[i].buckets = arrays->acquire_array<double>(out->hists[i].size))     ||
            !(out->hists[i].counts = arrays->acquire_array<std::size_t>(out->hists[i].size))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t j = 0; j < out->hists[i].size; j++) {
            if (!s
                .read((char *) &out->hists[i].buckets[j], sizeof(out->hists[i].buckets[j]))
                .read((char *) &out->hists[i].counts[j], sizeof(out->hists[i].counts[j]))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }
        }

        out->count++;
    }

    *bhist = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Message **msg, const std::string &buf, FixedBufferPool *fbp, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!msg) {
        return TRANSPORT_ERROR;
    }

    *msg = nullptr;

    std::stringstream s(buf);
    Message *out = fbp->acquire<Message>(Message::NONE, Message::INVALID, arrays, buffers);
    if (unpack(out, s) != TRANSPORT_SUCCESS) {
        fbp->release(out);
        return TRANSPORT_ERROR;
    }

    *msg = out;
    return TRANSPORT_SUCCESS;
}

int Transport::Thallium::Unpacker::unpack(Message *msg, std::stringstream &s) {
    if (!msg) {
        return TRANSPORT_ERROR;
    }

    msg->clean = true;

    return s
        .read((char *) &msg->direction, sizeof(msg->direction))
        .read((char *) &msg->type, sizeof(msg->type))
        .read((char *) &msg->src, sizeof(msg->src))
        .read((char *) &msg->dst, sizeof(msg->dst))?TRANSPORT_SUCCESS:TRANSPORT_ERROR;
}

int Transport::Thallium::Unpacker::unpack(Request::Request **req, const std::string &buf, const Message::Type type, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    *req = nullptr;

    switch (type) {
        case Message::PUT:
            {
                Request::Put *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::GET:
            {
                Request::Get *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::DELETE:
            {
                Request::Delete *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::HISTOGRAM:
            {
                Request::Histogram *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BPUT:
            {
                Request::BPut *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BGET:
            {
                Request::BGet *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BGETOP:
            {
                Request::BGetOp *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        case Message::BDELETE:
            {
                Request::BDelete *out = nullptr;
                ret = unpack(&out, buf, requests, arrays, buffers);
                *req = out;
            }
            break;
        default:
            break;
    }

    return ret;
}

int Transport::Thallium::Unpacker::unpack(Response::Response **res, const std::string &buf, const Message::Type type, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    *res = nullptr;

    switch (type) {
        case Message::PUT:
            {
                Response::Put *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::GET:
            {
                Response::Get *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::DELETE:
            {
                Response::Delete *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::HISTOGRAM:
            {
                Response::Histogram *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BPUT:
            {
                Response::BPut *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BGET:
            {
                Response::BGet *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BGETOP:
            {
                Response::BGetOp *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        case Message::BDELETE:
            {
                Response::BDelete *out = nullptr;
                ret = unpack(&out, buf, responses, arrays, buffers);
                *res = out;
            }
            break;
        default:
            break;
    }

    return ret;
}

#endif
