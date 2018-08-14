#include "transport/backend/MPI/Unpacker.hpp"

namespace Transport {
namespace MPI {

int Unpacker::any(const MPI_Comm comm, Message **msg, const void *buf, const std::size_t bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!msg || !buf) {
        return ret;
    }

    // unpack the header
    Message *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // use the header to unpack the rest of the data
    switch (basemsg->direction) {
        case Message::REQUEST:
            {
                Request::Request *req = nullptr;
                ret = unpack(comm, &req, buf, bufsize, basemsg->type);
                *msg = req;
            }
            break;
        case Message::RESPONSE:
            {
                Response::Response *res = nullptr;
                ret = unpack(comm, &res, buf, bufsize, basemsg->type);
                *msg = res;
            }
            break;
        default:
            break;
    }

    delete basemsg;

    if (ret != TRANSPORT_SUCCESS) {
        delete *msg;
        *msg = nullptr;
    }

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Request **req, const void *buf, const std::size_t bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!req || !buf) {
        return ret;
    }

    // unpack the header
    Message *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // make sure the data represents a request
    if (basemsg->direction != Message::REQUEST) {
        delete basemsg;
        return TRANSPORT_ERROR;
    }

    // unpack the rest of the data
    Request::Request *request = nullptr;
    if ((ret = unpack(comm, &request, buf, bufsize, basemsg->type)) == TRANSPORT_SUCCESS){
        *req = request;
    }
    else {
        delete request;
    }

    delete basemsg;

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Put **pm, const void *buf, const std::size_t bufsize) {
    if (!pm){
        return TRANSPORT_ERROR;
    }

    Request::Put *out = new Request::Put();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_type, sizeof(out->object_type), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_len, sizeof(out->object_len), MPI_CHAR, comm)       != MPI_SUCCESS)) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = ::operator new (out->subject_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_CHAR, comm)               != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = ::operator new (out->predicate_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_CHAR, comm)           != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    if (out->object_len) {
        if (!(out->object = ::operator new (out->object_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->object, out->object_len, MPI_CHAR, comm)                 != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    *pm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Get **gm, const void *buf, const std::size_t bufsize) {
    if (!gm){
        return TRANSPORT_ERROR;
    }

    Request::Get *out = new Request::Get();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_type, sizeof(out->object_type), MPI_CHAR, comm)     != MPI_SUCCESS)) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = ::operator new (out->subject_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_CHAR, comm)               != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = ::operator new (out->predicate_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_CHAR, comm)           != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    *gm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Delete **dm, const void *buf, const std::size_t bufsize) {
    if (!dm){
        return TRANSPORT_ERROR;
    }

    Request::Delete *out = new Request::Delete();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = ::operator new (out->subject_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_CHAR, comm)               != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = ::operator new (out->predicate_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_CHAR, comm)           != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    *dm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BPut **bpm, const void *buf, const std::size_t bufsize) {
    Request::BPut *out = new Request::BPut();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_lens[i], sizeof(out->object_lens[i]), MPI_CHAR, comm)       != MPI_SUCCESS) ||
                (!(out->subjects[i] = ::operator new(out->subject_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = ::operator new(out->predicate_lens[i])))                                                              ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS) ||
                (!(out->objects[i] = ::operator new(out->object_lens[i])))                                                                    ||
                (MPI_Unpack(buf, bufsize, &position, out->objects[i], out->object_lens[i], MPI_CHAR, comm)                    != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bpm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BGet **bgm, const void *buf, const std::size_t bufsize) {
    Request::BGet *out = new Request::BGet();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (!(out->subjects[i] = ::operator new(out->subject_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = ::operator new(out->predicate_lens[i])))                                                              ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BGetOp **bgm, const void *buf, const std::size_t bufsize) {
    Request::BGetOp *out = new Request::BGetOp();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->num_recs[i], sizeof(out->num_recs[i]), MPI_CHAR, comm)             != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->ops[i], sizeof(out->ops[i]), MPI_CHAR, comm)                       != MPI_SUCCESS) ||
                (!(out->subjects[i] = ::operator new(out->subject_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = ::operator new(out->predicate_lens[i])))                                                              ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::BDelete **bdm, const void *buf, const std::size_t bufsize) {
    Request::BDelete *out = new Request::BDelete();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Request::Request *>(out), buf, bufsize, &position)                                           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                (!(out->subjects[i] = ::operator new(out->subject_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = ::operator new(out->predicate_lens[i])))                                                              ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bdm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Response **res, const void *buf, const std::size_t bufsize) {
    int ret = TRANSPORT_ERROR;
    if (!res || !buf) {
        return ret;
    }

    // unpack the header
    Message *basemsg = nullptr;
    if (unpack(comm, &basemsg, buf, bufsize) != TRANSPORT_SUCCESS) {
        return ret;
    }

    // make sure the data represents a response
    if (basemsg->direction != Message::RESPONSE) {
        delete basemsg;
        return TRANSPORT_ERROR;
    }

    // unpack the rest of the data
    Response::Response *response = nullptr;
    if ((ret = unpack(comm, &response, buf, bufsize, basemsg->type)) == TRANSPORT_SUCCESS){
        *res = response;
    }
    else {
        delete response;
    }

    delete basemsg;

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Put **pm, const void *buf, const std::size_t bufsize) {
    Response::Put *out = new Response::Put();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                 != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return TRANSPORT_ERROR;
    }

    *pm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Get **gm, const void *buf, const std::size_t bufsize) {
    if (!gm){
        return TRANSPORT_ERROR;
    }

    Response::Get *out = new Response::Get();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                         != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_CHAR, comm)               != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->subject_len, sizeof(out->subject_len), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->predicate_len, sizeof(out->predicate_len), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_type, sizeof(out->object_type), MPI_CHAR, comm)     != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->object_len, sizeof(out->object_len), MPI_CHAR, comm)       != MPI_SUCCESS)) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if (out->subject_len) {
        if (!(out->subject = ::operator new (out->subject_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->subject, out->subject_len, MPI_CHAR, comm)               != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    if (out->predicate_len) {
        if (!(out->predicate = ::operator new (out->predicate_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->predicate, out->predicate_len, MPI_CHAR, comm)           != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    if (out->object_len) {
        if (!(out->object = ::operator new (out->object_len))) {
            delete out;
            return TRANSPORT_ERROR;
        }

        if (MPI_Unpack(buf, bufsize, &position, out->object, out->object_len, MPI_CHAR, comm)                 != MPI_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }
    }

    *gm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Delete **dm, const void *buf, const std::size_t bufsize) {
    Response::Delete *out = new Response::Delete();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)           != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offset, 1, MPI_INT, comm)                 != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, &position, &out->status, sizeof(out->status), MPI_CHAR, comm) != MPI_SUCCESS)) {
        delete out;
        return TRANSPORT_ERROR;
    }

    *dm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BPut **bpm, const void *buf, const std::size_t bufsize) {
    Response::BPut *out = new Response::BPut();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                             != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                       != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_CHAR, comm) != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bpm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BGet **bgm, const void *buf, const std::size_t bufsize) {
    Response::BGet *out = new Response::BGet();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                                         != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_CHAR, comm)             != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_lens[i], sizeof(out->object_lens[i]), MPI_CHAR, comm)       != MPI_SUCCESS) ||
                (!(out->subjects[i] = ::operator new(out->subject_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = ::operator new(out->predicate_lens[i])))                                                              ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS) ||
                (!(out->objects[i] = ::operator new(out->object_lens[i])))                                                                    ||
                (MPI_Unpack(buf, bufsize, &position, out->objects[i], out->object_lens[i], MPI_CHAR, comm)                    != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BGetOp **bgm, const void *buf, const std::size_t bufsize) {
    Response::BGetOp *out = new Response::BGetOp();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                                         != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                                   != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_CHAR, comm)             != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->subject_lens[i], sizeof(out->subject_lens[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->predicate_lens[i], sizeof(out->predicate_lens[i]), MPI_CHAR, comm) != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_types[i], sizeof(out->object_types[i]), MPI_CHAR, comm)     != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->object_lens[i], sizeof(out->object_lens[i]), MPI_CHAR, comm)       != MPI_SUCCESS) ||
                (!(out->subjects[i] = ::operator new(out->subject_lens[i])))                                                                  ||
                (MPI_Unpack(buf, bufsize, &position, out->subjects[i], out->subject_lens[i], MPI_CHAR, comm)                  != MPI_SUCCESS) ||
                (!(out->predicates[i] = ::operator new(out->predicate_lens[i])))                                                              ||
                (MPI_Unpack(buf, bufsize, &position, out->predicates[i], out->predicate_lens[i], MPI_CHAR, comm)              != MPI_SUCCESS) ||
                (!(out->objects[i] = ::operator new(out->object_lens[i])))                                                                    ||
                (MPI_Unpack(buf, bufsize, &position, out->objects[i], out->object_lens[i], MPI_CHAR, comm)                    != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bgm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Response::BDelete **bdm, const void *buf, const std::size_t bufsize) {
    Response::BDelete *out = new Response::BDelete();
    if (!out) {
        return TRANSPORT_ERROR;
    }

    int position = 0;
    if (unpack(comm, static_cast<Response::Response *>(out), buf, bufsize, &position)                             != TRANSPORT_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    std::size_t count = 0;
    if (MPI_Unpack(buf, bufsize, &position, &count, sizeof(out->count), MPI_CHAR, comm)                                       != MPI_SUCCESS) {
        delete out;
        return TRANSPORT_ERROR;
    }

    // If there are keys/values, allocate space for them and unpack
    if (count) {
        if (out->alloc(count) != TRANSPORT_SUCCESS) {
            delete out;
            return TRANSPORT_ERROR;
        }

        for(std::size_t i = 0; i < count; i++) {
            if ((MPI_Unpack(buf, bufsize, &position, &out->ds_offsets[i], 1, MPI_INT, comm)                       != MPI_SUCCESS) ||
                (MPI_Unpack(buf, bufsize, &position, &out->statuses[i], sizeof(out->statuses[i]), MPI_CHAR, comm) != MPI_SUCCESS)) {
                delete out;
                return TRANSPORT_ERROR;
            }

            out->count++;
        }
    }

    *bdm = out;

    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Message *msg, const void *buf, const std::size_t bufsize, int *position) {
    if (!msg || !buf || !position) {
        return TRANSPORT_ERROR;
    }

    msg->clean = true;

    std::size_t givensize = 0;
    if ((MPI_Unpack(buf, bufsize, position, &msg->direction, sizeof(msg->direction), MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->type,      sizeof(msg->type),      MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &givensize,      sizeof(givensize),      MPI_CHAR, comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->src,       1,                      MPI_INT,  comm) != MPI_SUCCESS) ||
        (MPI_Unpack(buf, bufsize, position, &msg->dst,       1,                      MPI_INT,  comm) != MPI_SUCCESS)) {
        return TRANSPORT_ERROR;
    }

    return (givensize <= bufsize)?TRANSPORT_SUCCESS:TRANSPORT_ERROR;
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

int Unpacker::unpack(const MPI_Comm comm, Message **msg, const void *buf, const std::size_t bufsize) {
    if (!msg || !buf) {
        return TRANSPORT_ERROR;
    }

    // unpack the header
    Message *out = nullptr;
    if (unpack(comm, &out, buf, bufsize) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    *msg = out;
    return TRANSPORT_SUCCESS;
}

int Unpacker::unpack(const MPI_Comm comm, Request::Request **req, const void *buf, const std::size_t bufsize, const Message::Type type) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    switch (type) {
        case Message::PUT:
            {
                Request::Put *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        case Message::GET:
            {
                Request::Get *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        case Message::DELETE:
            {
                Request::Delete *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        case Message::BPUT:
            {
                Request::BPut *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        case Message::BGET:
            {
                Request::BGet *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        case Message::BGETOP:
            {
                Request::BGetOp *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        case Message::BDELETE:
            {
                Request::BDelete *request = nullptr;
                ret = unpack(comm, &request, buf, bufsize);
                *req = request;
            }
            break;
        default:
            break;
    }

    return ret;
}

int Unpacker::unpack(const MPI_Comm comm, Response::Response **res, const void *buf, const std::size_t bufsize, const Message::Type type) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    switch (type) {
        case Message::PUT:
            {
                Response::Put *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
                *res = response;
            }
            break;
        case Message::GET:
            {
                Response::Get *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
                *res = response;
            }
            break;
        case Message::DELETE:
            {
                Response::Delete *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
                *res = response;
            }
            break;
        case Message::BPUT:
            {
                Response::BPut *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
                *res = response;
            }
            break;
        case Message::BGET:
            {
                Response::BGet *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
                *res = response;
            }
            break;
        case Message::BGETOP:
            {
                Response::BGetOp *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
                *res = response;
            }
            break;
        case Message::BDELETE:
            {
                Response::BDelete *response = nullptr;
                ret = unpack(comm, &response, buf, bufsize);
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
