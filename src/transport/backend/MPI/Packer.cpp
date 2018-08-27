#include "transport/backend/MPI/Packer.hpp"

namespace Transport {
namespace MPI {

int Packer::pack(const MPI_Comm comm, const Request::Request *req, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    switch (req->type) {
        case Message::PUT:
            ret = pack(comm, static_cast<const Request::Put *>(req), buf, bufsize, packed);
            break;
        case Message::GET:
            ret = pack(comm, static_cast<const Request::Get *>(req), buf, bufsize, packed);
            break;
        case Message::DELETE:
            ret = pack(comm, static_cast<const Request::Delete *>(req), buf, bufsize, packed);
            break;
        case Message::HISTOGRAM:
            ret = pack(comm, static_cast<const Request::Histogram *>(req), buf, bufsize, packed);
            break;
        case Message::BPUT:
            ret = pack(comm, static_cast<const Request::BPut *>(req), buf, bufsize, packed);
            break;
        case Message::BGET:
            ret = pack(comm, static_cast<const Request::BGet *>(req), buf, bufsize, packed);
            break;
        case Message::BGETOP:
            ret = pack(comm, static_cast<const Request::BGetOp *>(req), buf, bufsize, packed);
            break;
        case Message::BDELETE:
            ret = pack(comm, static_cast<const Request::BDelete *>(req), buf, bufsize, packed);
            break;
        default:
            break;
    }

    return ret;
}

int Packer::pack(const MPI_Comm comm, const Request::Put *pm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(pm), buf, bufsize, &position, packed)              != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&pm->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) ||
        (MPI_Pack(&pm->subject_len, sizeof(pm->subject_len), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&pm->predicate_len, sizeof(pm->predicate_len), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&pm->object_type, sizeof(pm->object_type), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&pm->object_len, sizeof(pm->object_len), MPI_BYTE, *buf, *bufsize, &position, comm)       != MPI_SUCCESS) ||
        (MPI_Pack(pm->subject, pm->subject_len, MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
        (MPI_Pack(pm->predicate, pm->predicate_len, MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS) ||
        (MPI_Pack(pm->object, pm->object_len, MPI_BYTE, *buf, *bufsize, &position, comm)                    != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::Get *gm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(gm), buf, bufsize, &position, packed)              != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&gm->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) ||
        (MPI_Pack(&gm->subject_len, sizeof(gm->subject_len), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&gm->predicate_len, sizeof(gm->predicate_len), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&gm->object_type, sizeof(gm->object_type), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(gm->subject, gm->subject_len, MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
        (MPI_Pack(gm->predicate, gm->predicate_len, MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::Delete *dm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(dm), buf, bufsize, &position, packed)              != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&dm->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) ||
        (MPI_Pack(&dm->subject_len, sizeof(dm->subject_len), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&dm->predicate_len, sizeof(dm->predicate_len), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(dm->subject, dm->subject_len, MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
        (MPI_Pack(dm->predicate, dm->predicate_len, MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::Histogram *hist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(hist), buf, bufsize, &position, packed) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&hist->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)               != MPI_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::BPut *bpm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(bpm), buf, bufsize, &position, packed)                           != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bpm->count, sizeof(bpm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        if (
            (MPI_Pack(&bpm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                   != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->subject_lens[i], sizeof(bpm->subject_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->predicate_lens[i], sizeof(bpm->predicate_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->object_types[i], sizeof(bpm->object_types[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->object_lens[i], sizeof(bpm->object_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)       != MPI_SUCCESS) ||
            (MPI_Pack(bpm->subjects[i], bpm->subject_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(bpm->predicates[i], bpm->predicate_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS) ||
            (MPI_Pack(bpm->objects[i], bpm->object_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                    != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::BGet *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(bgm), buf, bufsize, &position, packed)                           != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bgm->count, sizeof(bgm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if ((MPI_Pack(&bgm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                   != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->subject_lens[i], sizeof(bgm->subject_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->object_types[i], sizeof(bgm->object_types[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(bgm->subjects[i], bgm->subject_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(bgm->predicates[i], bgm->predicate_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::BGetOp *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(bgm), buf, bufsize, &position, packed)                           != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bgm->count, sizeof(bgm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if ((MPI_Pack(&bgm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                   != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->subject_lens[i], sizeof(bgm->subject_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->object_types[i], sizeof(bgm->object_types[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->num_recs[i], sizeof(bgm->num_recs[i]), MPI_BYTE, *buf, *bufsize, &position, comm)             != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->ops[i], sizeof(bgm->ops[i]), MPI_BYTE, *buf, *bufsize, &position, comm)                       != MPI_SUCCESS) ||
            (MPI_Pack(bgm->subjects[i], bgm->subject_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(bgm->predicates[i], bgm->predicate_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::BDelete *bdm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(bdm), buf, bufsize, &position, packed)                           != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bdm->count, sizeof(bdm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bdm->count; i++) {
        if ((MPI_Pack(&bdm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                   != MPI_SUCCESS) ||
            (MPI_Pack(&bdm->subject_lens[i], sizeof(bdm->subject_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bdm->predicate_lens[i], sizeof(bdm->predicate_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(bdm->subjects[i], bdm->subject_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(bdm->predicates[i], bdm->predicate_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::BHistogram *bhist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Request::Request *>(bhist), buf, bufsize, &position, packed)!= TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bhist->count, sizeof(bhist->count), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bhist->count; i++) {
        if (MPI_Pack(&bhist->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)         != MPI_SUCCESS) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::Response *res, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    switch (res->type) {
        case Message::PUT:
            ret = pack(comm, static_cast<const Response::Put *>(res), buf, bufsize, packed);
            break;
        case Message::GET:
            ret = pack(comm, static_cast<const Response::Get *>(res), buf, bufsize, packed);
            break;
        case Message::DELETE:
            ret = pack(comm, static_cast<const Response::Delete *>(res), buf, bufsize, packed);
            break;
        case Message::HISTOGRAM:
            ret = pack(comm, static_cast<const Response::Histogram *>(res), buf, bufsize, packed);
            break;
        case Message::BPUT:
            ret = pack(comm, static_cast<const Response::BPut *>(res), buf, bufsize, packed);
            break;
        case Message::BGET:
            ret = pack(comm, static_cast<const Response::BGet *>(res), buf, bufsize, packed);
            break;
        case Message::BGETOP:
            ret = pack(comm, static_cast<const Response::BGetOp *>(res), buf, bufsize, packed);
            break;
        case Message::BDELETE:
            ret = pack(comm, static_cast<const Response::BDelete *>(res), buf, bufsize, packed);
            break;
        default:
            break;
    }

    return ret;
}

int Packer::pack(const MPI_Comm comm, const Response::Put *pm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(pm), buf, bufsize, &position, packed)    != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&pm->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&pm->status, sizeof(pm->status), MPI_BYTE, *buf, *bufsize, &position, comm)       != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::Get *gm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(gm), buf, bufsize, &position, packed)            != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&gm->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) ||
        (MPI_Pack(&gm->status, sizeof(gm->status), MPI_BYTE, *buf, *bufsize, &position, comm)               != MPI_SUCCESS) ||
        (MPI_Pack(&gm->subject_len, sizeof(gm->subject_len), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(&gm->predicate_len, sizeof(gm->predicate_len), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&gm->object_type, sizeof(gm->object_type), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
        (MPI_Pack(gm->subject, gm->subject_len, MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
        (MPI_Pack(gm->predicate, gm->predicate_len, MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    // only write object if the status is HXHIM_SUCCESS
    if (gm->status == HXHIM_SUCCESS) {
        if ((MPI_Pack(&gm->object_len, sizeof(gm->object_len), MPI_BYTE, *buf, *bufsize, &position, comm)   != MPI_SUCCESS) ||
            (MPI_Pack(gm->object, gm->object_len, MPI_BYTE, *buf, *bufsize, &position, comm)                != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::Delete *dm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(dm), buf, bufsize, &position, packed) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&dm->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)                != MPI_SUCCESS) ||
        (MPI_Pack(&dm->status, sizeof(dm->status), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::Histogram *hist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(hist), buf, bufsize, &position, packed)                      != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if ((MPI_Pack(&hist->ds_offset, 1, MPI_INT, *buf, *bufsize, &position, comm)                                        != MPI_SUCCESS) ||
        (MPI_Pack(&hist->status, sizeof(hist->status), MPI_BYTE, *buf, *bufsize, &position, comm)                       != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    const std::size_t size = hist->hist.size;
    if (MPI_Pack(&size, sizeof(size), MPI_BYTE, *buf, *bufsize, &position, comm)                                        != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < size; i++) {
        if ((MPI_Pack(&hist->hist.buckets[i], 1, MPI_DOUBLE, *buf, *bufsize, &position, comm)                           != MPI_SUCCESS) ||
            (MPI_Pack(&hist->hist.counts[i], sizeof(hist->hist.counts[i]), MPI_BYTE, *buf, *bufsize, &position, comm)   != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::BPut *bpm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(bpm), buf, bufsize, &position, packed)                 != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bpm->count, sizeof(bpm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                      != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        if ((MPI_Pack(&bpm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                           != MPI_SUCCESS) ||
            (MPI_Pack(&bpm->statuses[i], sizeof(bpm->statuses[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::BGet *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(bgm), buf, bufsize, &position, packed)                         != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bgm->count, sizeof(bgm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if ((MPI_Pack(&bgm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                   != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->statuses[i], sizeof(bgm->statuses[i]), MPI_BYTE, *buf, *bufsize, &position, comm)             != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->subject_lens[i], sizeof(bgm->subject_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->object_types[i], sizeof(bgm->object_types[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(bgm->subjects[i], bgm->subject_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(bgm->predicates[i], bgm->predicate_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }

        // only write object if the status is HXHIM_SUCCESS
        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            if ((MPI_Pack(&bgm->object_lens[i], sizeof(bgm->object_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)   != MPI_SUCCESS) ||
                (MPI_Pack(bgm->objects[i], bgm->object_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                != MPI_SUCCESS)) {
                cleanup(buf, bufsize, packed);
                return TRANSPORT_ERROR;
            }
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::BGetOp *bgm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(bgm), buf, bufsize, &position, packed)                         != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bgm->count, sizeof(bgm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                              != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if ((MPI_Pack(&bgm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                   != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->statuses[i], sizeof(bgm->statuses[i]), MPI_BYTE, *buf, *bufsize, &position, comm)             != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->subject_lens[i], sizeof(bgm->subject_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS) ||
            (MPI_Pack(&bgm->object_types[i], sizeof(bgm->object_types[i]), MPI_BYTE, *buf, *bufsize, &position, comm)     != MPI_SUCCESS) ||
            (MPI_Pack(bgm->subjects[i], bgm->subject_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) ||
            (MPI_Pack(bgm->predicates[i], bgm->predicate_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)              != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }

        // only write object if the status is HXHIM_SUCCESS
        if (bgm->statuses[i] == HXHIM_SUCCESS) {
            if ((MPI_Pack(&bgm->object_lens[i], sizeof(bgm->object_lens[i]), MPI_BYTE, *buf, *bufsize, &position, comm)   != MPI_SUCCESS) ||
                (MPI_Pack(bgm->objects[i], bgm->object_lens[i], MPI_BYTE, *buf, *bufsize, &position, comm)                != MPI_SUCCESS)) {
                cleanup(buf, bufsize, packed);
                return TRANSPORT_ERROR;
            }
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::BDelete *bdm, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(bdm), buf, bufsize, &position, packed)             != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bdm->count, sizeof(bdm->count), MPI_BYTE, *buf, *bufsize, &position, comm)                  != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bdm->count; i++) {
        if ((MPI_Pack(&bdm->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                       != MPI_SUCCESS) ||
            (MPI_Pack(&bdm->statuses[i], sizeof(bdm->statuses[i]), MPI_BYTE, *buf, *bufsize, &position, comm) != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::BHistogram *bhist, void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    int position = 0;
    if (pack(comm, static_cast<const Response::Response *>(bhist), buf, bufsize, &position, packed)                                  != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (MPI_Pack(&bhist->count, sizeof(bhist->count), MPI_BYTE, *buf, *bufsize, &position, comm)                                     != MPI_SUCCESS) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bhist->count; i++) {
        if ((MPI_Pack(&bhist->ds_offsets[i], 1, MPI_INT, *buf, *bufsize, &position, comm)                                            != MPI_SUCCESS) ||
            (MPI_Pack(&bhist->statuses[i], sizeof(bhist->statuses[i]), MPI_BYTE, *buf, *bufsize, &position, comm)                    != MPI_SUCCESS)) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }

        const std::size_t size = bhist->hists[i].size;
        if (MPI_Pack(&size, sizeof(size), MPI_BYTE, *buf, *bufsize, &position, comm)                                                 != MPI_SUCCESS) {
            cleanup(buf, bufsize, packed);
            return TRANSPORT_ERROR;
        }

        for(std::size_t j = 0; j < size; j++) {
            if ((MPI_Pack(&bhist->hists[i].buckets[j], 1, MPI_DOUBLE, *buf, *bufsize, &position, comm)                               != MPI_SUCCESS) ||
                (MPI_Pack(&bhist->hists[i].counts[j], sizeof(bhist->hists[i].counts[j]), MPI_BYTE, *buf, *bufsize, &position, comm)  != MPI_SUCCESS)) {
                cleanup(buf, bufsize, packed);
                return TRANSPORT_ERROR;
            }
        }
    }

    *bufsize = position;

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Message *msg, void **buf, std::size_t *bufsize, int *position, FixedBufferPool *packed) {
    // *bufsize should have been set
    if (!msg || !buf || !bufsize || !position) {
        return TRANSPORT_ERROR;
    }

    // Allocate the buffer
    *bufsize = msg->size();
    if (!(*buf = packed->acquire(*bufsize))) {
        return TRANSPORT_ERROR;
    }

    // Pack the comment fields
    if ((MPI_Pack(&msg->direction, sizeof(msg->direction), MPI_BYTE, *buf, *bufsize, position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&msg->type,      sizeof(msg->type),      MPI_BYTE, *buf, *bufsize, position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&msg->src,       1,                      MPI_INT,  *buf, *bufsize, position, comm) != MPI_SUCCESS) ||
        (MPI_Pack(&msg->dst,       1,                      MPI_INT,  *buf, *bufsize, position, comm) != MPI_SUCCESS)) {
        cleanup(buf, bufsize, packed);
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Request::Request *req, void **buf, std::size_t *bufsize, int *position, FixedBufferPool *packed) {
    if (pack(comm, static_cast<const Message *>(req), buf, bufsize, position, packed) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const MPI_Comm comm, const Response::Response *res, void **buf, std::size_t *bufsize, int *position, FixedBufferPool *packed) {
    if (pack(comm, static_cast<const Message *>(res), buf, bufsize, position, packed) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

void Packer::cleanup(void **buf, std::size_t *bufsize, FixedBufferPool *packed) {
    if (buf) {
        packed->release(*buf);
        *buf = nullptr;
    }

    if (bufsize) {
        *bufsize = 0;
    }
}

}
}
