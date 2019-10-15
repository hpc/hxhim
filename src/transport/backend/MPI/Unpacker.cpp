#include "transport/backend/MPI/Unpacker.hpp"

namespace Transport {
namespace MPI {

int Unpacker::unpack(const MPI_Comm comm, Request::Request **req, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!req || !buf) {
        return ret;
    }

    // unpack the header
    Message *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize, requests, arrays, buffers) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // make sure the data represents a request
    if (basemsg->direction != Message::REQUEST) {
        requests->release(basemsg);
        return TRANSPORT_ERROR;
    }

    // unpack the rest of the data
    Request::Request *request = nullptr;
    if ((ret = unpack(comm, &request, buf, bufsize, basemsg->type, requests, arrays, buffers)) == TRANSPORT_SUCCESS){
        *req = request;
    }
    else {
        requests->release(request);
    }

    requests->release(basemsg);

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Put **pm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!pm){
        return TRANSPORT_ERROR;
    }

    Request::Put *out = requests->acquire<Request::Put>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                            != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                                != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_BYTE, comm)      != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_BYTE, comm)  != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_type, sizeof(out->object_type), MPI_BYTE, comm)      != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_len, sizeof(out->object_len), MPI_BYTE, comm)        != MPI_SUCCESS)) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = buffers->acquire(out->subject_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_BYTE, comm)                != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = buffers->acquire(out->predicate_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_BYTE, comm)            != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    if (out->object_len) {
        if (!(out->object = buffers->acquire(out->object_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->object, out->object_len, MPI_BYTE, comm)                  != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    *pm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Get **gm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!gm){
        return TRANSPORT_ERROR;
    }

    Request::Get *out = requests->acquire<Request::Get>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                           != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_BYTE, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_BYTE, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_type, sizeof(out->object_type), MPI_BYTE, comm)     != MPI_SUCCESS)) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = buffers->acquire(out->subject_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_BYTE, comm)               != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = buffers->acquire(out->predicate_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_BYTE, comm)           != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    *gm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Delete **dm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!dm){
        return TRANSPORT_ERROR;
    }

    Request::Delete *out = requests->acquire<Request::Delete>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                           != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_BYTE, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_BYTE, comm) != MPI_SUCCESS)) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = buffers->acquire(out->subject_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_BYTE, comm)               != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = buffers->acquire(out->predicate_len))) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_BYTE, comm)           != MPI_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }
    }

    *dm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Histogram **hist, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!hist){
        return TRANSPORT_ERROR;
    }

    Request::Histogram *out = requests->acquire<Request::Histogram>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position) != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)      != MPI_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    *hist = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BPut **bpm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BPut *out = requests->acquire<Request::BPut>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                       != MPI_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_BYTE, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_lens[i], sizeof(out->object_lens[i]), MPI_BYTE, comm)       != MPI_SUCCESS) ||
                (!(out->subjects[i] = buffers->acquire(out->subject_lens[i])))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_BYTE, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))                                                            ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_BYTE, comm)              != MPI_SUCCESS) ||
                (!(out->objects[i] = buffers->acquire(out->object_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->objects[i], out->object_lens[i], MPI_BYTE, comm)                    != MPI_SUCCESS)) {
                requests->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bpm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BGet **bgm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BGet *out = requests->acquire<Request::BGet>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                       != MPI_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_BYTE, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (!(out->subjects[i] = buffers->acquire(out->subject_lens[i])))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_BYTE, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))                                                            ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_BYTE, comm)              != MPI_SUCCESS)) {
                requests->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BGetOp **bgm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BGetOp *out = requests->acquire<Request::BGetOp>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                       != MPI_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_BYTE, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->num_recs[i], sizeof(out->num_recs[i]), MPI_BYTE, comm)             != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->ops[i], sizeof(out->ops[i]), MPI_BYTE, comm)                       != MPI_SUCCESS) ||
                (!(out->subjects[i] = buffers->acquire(out->subject_lens[i])))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_BYTE, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))                                                            ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_BYTE, comm)              != MPI_SUCCESS)) {
                requests->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BDelete **bdm, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Request::BDelete *out = requests->acquire<Request::BDelete>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                       != MPI_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_BYTE, comm) != MPI_SUCCESS) ||
                (!(out->subjects[i] = buffers->acquire(out->subject_lens[i])))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_BYTE, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))                                                            ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_BYTE, comm)              != MPI_SUCCESS)) {
                requests->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bdm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BHistogram **bhist, const void *buf, const std::size_t bufsize, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!bhist){
        return TRANSPORT_ERROR;
    }

    Request::BHistogram *out = requests->acquire<Request::BHistogram>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)    != TRANSPORT_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)     != MPI_SUCCESS) {
        requests->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            requests->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if (MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm) != MPI_SUCCESS) {
                requests->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bhist = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Response **res, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!res || !buf) {
        return ret;
    }

    // unpack the header
    Message *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize, responses, arrays, buffers) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // make sure the data represents a response
    if (basemsg->direction != Message::RESPONSE) {
        responses->release(basemsg);
        return TRANSPORT_ERROR;
    }

    // unpack the rest of the data
    Response::Response *response = nullptr;
    if ((ret = unpack(comm, &response, buf, bufsize, basemsg->type, responses, arrays, buffers)) == TRANSPORT_SUCCESS){
        *res = response;
    }
    else {
        responses->release(response);
    }

    responses->release(basemsg);

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Put **pm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Put *out = responses->acquire<Response::Put>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)           != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                 != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_BYTE, comm) != MPI_SUCCESS)) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    *pm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Get **gm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!gm){
        return TRANSPORT_ERROR;
    }

    Response::Get *out = responses->acquire<Response::Get>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                         != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_BYTE, comm)               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_BYTE, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_BYTE, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_type, sizeof(out->object_type), MPI_BYTE, comm)     != MPI_SUCCESS)) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = buffers->acquire(out->subject_len))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_BYTE, comm)               != MPI_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = buffers->acquire(out->predicate_len))) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_BYTE, comm)           != MPI_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }
    }

    if (out->status == HXHIM_SUCCESS) {
        if (MPI_Unpack(buf, bufsize, &position, &out->object_len, sizeof(out->object_len), MPI_BYTE, comm)    != MPI_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        if (out->object_len) {
            if (!(out->object = buffers->acquire(out->object_len))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (MPI_Unpack(buf, bufsize, &position, out->object, out->object_len, MPI_BYTE, comm)             != MPI_SUCCESS) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }
        }
    }

    *gm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Delete **dm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Delete *out = responses->acquire<Response::Delete>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)           != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                 != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_BYTE, comm) != MPI_SUCCESS)) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    *dm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Histogram **hist, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::Histogram *out = responses->acquire<Response::Histogram>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                               != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                                     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_BYTE, comm)                     != MPI_SUCCESS)) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if (MPI_Unpack(buf, bufsize, &position, &out->hist.size, sizeof(out->hist.size), MPI_BYTE, comm)                != MPI_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    if (out->hist.size &&
        (!(out->hist.buckets = arrays->acquire_array<double>(out->hist.size))      ||
         !(out->hist.counts = arrays->acquire_array<std::size_t>(out->hist.size)))) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < out->hist.size; i++) {
        if ((MPI_Unpack(buf, bufsize, &position, &out->hist.buckets[i], 1, MPI_DOUBLE, comm)                        != MPI_SUCCESS) ||
            (MPI_Unpack(buf, bufsize, &position, &out->hist.counts[i], sizeof(out->hist.counts[i]), MPI_BYTE, comm) != MPI_SUCCESS)) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }
    }

    *hist = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BPut **bpm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BPut *out = responses->acquire<Response::BPut>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                             != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                           != MPI_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                       != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_BYTE, comm) != MPI_SUCCESS)) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bpm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BGet **bgm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BGet *out = responses->acquire<Response::BGet>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                                         != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                       != MPI_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_BYTE, comm)             != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_BYTE, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (!(out->subjects[i] = buffers->acquire(out->subject_lens[i])))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_BYTE, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))                                                            ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_BYTE, comm)              != MPI_SUCCESS)) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            // only read object if status is HXHIM_SUCCESS
            if (out->statuses[i] == HXHIM_SUCCESS) {
                if ((MPI_Unpack(buf, bufsize, &position, &out->object_lens[i], sizeof(out->object_lens[i]), MPI_BYTE, comm)   != MPI_SUCCESS) ||
                    (!(out->objects[i] = buffers->acquire(out->object_lens[i])))                                                              ||
                    (MPI_Unpack(buf, bufsize, &position, out->objects[i], out->object_lens[i], MPI_BYTE, comm)                != MPI_SUCCESS)) {
                    responses->release(out);
                    return TRANSPORT_ERROR;
                }
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BGetOp **bgm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BGetOp *out = responses->acquire<Response::BGetOp>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                                         != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                       != MPI_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_BYTE, comm)             != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_BYTE, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_BYTE, comm)     != MPI_SUCCESS) ||
                (!(out->subjects[i] = buffers->acquire(out->subject_lens[i])))                                                                ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_BYTE, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = buffers->acquire(out->predicate_lens[i])))                                                            ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_BYTE, comm)              != MPI_SUCCESS)) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            // only read object if status is HXHIM_SUCCESS
            if (out->statuses[i] == HXHIM_SUCCESS) {
                if ((MPI_Unpack(buf, bufsize, &position, &out->object_lens[i], sizeof(out->object_lens[i]), MPI_BYTE, comm)   != MPI_SUCCESS) ||
                    (!(out->objects[i] = buffers->acquire(out->object_lens[i])))                                                              ||
                    (MPI_Unpack(buf, bufsize, &position, out->objects[i], out->object_lens[i], MPI_BYTE, comm)                != MPI_SUCCESS)) {
                    responses->release(out);
                    return TRANSPORT_ERROR;
                }
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BDelete **bdm, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BDelete *out = responses->acquire<Response::BDelete>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                             != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                           != MPI_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                       != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_BYTE, comm) != MPI_SUCCESS)) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bdm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BHistogram **bhist, const void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    Response::BHistogram *out = responses->acquire<Response::BHistogram>(arrays, buffers);
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                                               != TRANSPORT_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_BYTE, comm)                                             != MPI_SUCCESS) {
        responses->release(out);
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            responses->release(out);
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                         != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_BYTE, comm)                   != MPI_SUCCESS)) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (MPI_Unpack(buf, bufsize, &position, &out->hists[i].size, sizeof(out->hists[i].size), MPI_BYTE, comm)                != MPI_SUCCESS) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            if (out->hists[i].size &&
                (!(out->hists[i].buckets = arrays->acquire_array<double>(out->hists[i].size))      ||
                 !(out->hists[i].counts = arrays->acquire_array<std::size_t>(out->hists[i].size)))) {
                responses->release(out);
                return TRANSPORT_ERROR;
            }

            for(std::size_t j = 0; j < out->hists[i].size; j++) {
                if ((MPI_Unpack(buf, bufsize, &position, &out->hists[i].buckets[j], 1, MPI_DOUBLE, comm)                            != MPI_SUCCESS) ||
                    (MPI_Unpack(buf, bufsize, &position, &out->hists[i].counts[j], sizeof(out->hists[i].counts[j]), MPI_BYTE, comm) != MPI_SUCCESS)) {
                    responses->release(out);
                    return TRANSPORT_ERROR;
                }
            }

            out->count++;
        }
    }

    *bhist = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Message *msg, const void *buf, const std::size_t bufsize, int *position) {
    if (!msg || !buf || !position) {
        return TRANSPORT_ERROR;
    }

    msg->clean = true;

    if ((MPI_Unpack(buf, bufsize, position, &msg->direction, sizeof(msg->direction), MPI_BYTE, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->type,      sizeof(msg->type),      MPI_BYTE, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->src,       1,                      MPI_INT,  comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->dst,       1,                      MPI_INT,  comm) != MPI_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Request *req, const void *buf, const std::size_t bufsize, int *position) {
    if (unpack(comm, static_cast<Message *>(req), buf, bufsize, position) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Response *res, const void *buf, const std::size_t bufsize, int *position) {
    if (unpack(comm, static_cast<Message *>(res), buf, bufsize, position) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Message **msg, const void *buf, const std::size_t bufsize, FixedBufferPool *fbp, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    if (!msg || !buf) {
        return TRANSPORT_ERROR;
    }

    // unpack the header
    int position = 0;
    Message *out = fbp->acquire<Message>(Message::NONE, Message::INVALID, arrays, buffers);
    if (unpack(comm, out, buf, bufsize, &position) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    *msg = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Request **req, const void *buf, const std::size_t bufsize, const Message::Type type, FixedBufferPool *requests, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    switch (type) {
        case Message::PUT:
            {
                Request::Put *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::GET:
            {
                Request::Get *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::DELETE:
            {
                Request::Delete *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::HISTOGRAM:
            {
                Request::Histogram *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::BPUT:
            {
                Request::BPut *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::BGET:
            {
                Request::BGet *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::BGETOP:
            {
                Request::BGetOp *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        case Message::BDELETE:
            {
                Request::BDelete *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize, requests, arrays, buffers);
                *req = request;
            }
            break;
        default:
            break;
    }

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Response **res, const void *buf, const std::size_t bufsize, const Message::Type type, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    switch (type) {
        case Message::PUT:
            {
                Response::Put *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::GET:
            {
                Response::Get *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::DELETE:
            {
                Response::Delete *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::HISTOGRAM:
            {
                Response::Histogram *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::BPUT:
            {
                Response::BPut *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::BGET:
            {
                Response::BGet *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::BGETOP:
            {
                Response::BGetOp *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        case Message::BDELETE:
            {
                Response::BDelete *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize, responses, arrays, buffers);
                *res = response;
            }
            break;
        default:
            break;
    }

    return ret;
}

}
}
