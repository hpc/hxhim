#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/Packer.hpp"

namespace Transport {
namespace Thallium {

int Packer::any(const Message *msg, std::string &buf) {
    int ret = TRANSPORT_ERROR;
    if (!msg) {
        return ret;
    }

    switch (msg->direction) {
        case Message::REQUEST:
            ret = pack(static_cast<const Request::Request *>(msg), buf);
            break;
        case Message::RESPONSE:
            ret = pack(static_cast<const Response::Response *>(msg), buf);
            break;
        default:
            break;
    }

    return ret;
}

int Packer::pack(const Request::Request *req, std::string &buf) {
    int ret = TRANSPORT_ERROR;
    if (!req) {
        return ret;
    }

    switch (req->type) {
        case Message::PUT:
            ret = pack(static_cast<const Request::Put *>(req), buf);
            break;
        case Message::GET:
            ret = pack(static_cast<const Request::Get *>(req), buf);
            break;
        case Message::DELETE:
            ret = pack(static_cast<const Request::Delete *>(req), buf);
            break;
        case Message::HISTOGRAM:
            ret = pack(static_cast<const Request::Histogram *>(req), buf);
            break;
        case Message::BPUT:
            ret = pack(static_cast<const Request::BPut *>(req), buf);
            break;
        case Message::BGET:
            ret = pack(static_cast<const Request::BGet *>(req), buf);
            break;
        case Message::BGETOP:
            ret = pack(static_cast<const Request::BGetOp *>(req), buf);
            break;
        case Message::BDELETE:
            ret = pack(static_cast<const Request::BDelete *>(req), buf);
            break;
        default:
            break;
    }

    return ret;
}

int Packer::pack(const Request::Put *pm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(pm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &pm->ds_offset, sizeof(pm->ds_offset))
        .write((char *) &pm->subject_len, sizeof(pm->subject_len))
        .write((char *) &pm->predicate_len, sizeof(pm->predicate_len))
        .write((char *) &pm->object_len, sizeof(pm->object_len))
        .write((char *) &pm->object_type, sizeof(pm->object_type))
        .write((char *) pm->subject, pm->subject_len)
        .write((char *) pm->predicate, pm->predicate_len)
        .write((char *) pm->object, pm->object_len)) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Get *gm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(gm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &gm->ds_offset, sizeof(gm->ds_offset))
        .write((char *) &gm->subject_len, sizeof(gm->subject_len))
        .write((char *) &gm->predicate_len, sizeof(gm->predicate_len))
        .write((char *) &gm->object_type, sizeof(gm->object_type))
        .write((char *) gm->subject, gm->subject_len)
        .write((char *) gm->predicate, gm->predicate_len)) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Delete *dm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(dm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &dm->ds_offset, sizeof(dm->ds_offset))
        .write((char *) &dm->subject_len, sizeof(dm->subject_len))
        .write((char *) &dm->predicate_len, sizeof(dm->predicate_len))
        .write((char *) dm->subject, dm->subject_len)
        .write((char *) dm->predicate, dm->predicate_len)) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::Histogram *hist, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(hist), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &hist->ds_offset, sizeof(hist->ds_offset))) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BPut *bpm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(bpm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bpm->count, sizeof(bpm->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bpm->count; i++) {
        if (!s
            .write((char *) &bpm->ds_offsets[i], sizeof(bpm->ds_offsets[i]))
            .write((char *) &bpm->subject_lens[i], sizeof(bpm->subject_lens[i]))
            .write((char *) &bpm->predicate_lens[i], sizeof(bpm->predicate_lens[i]))
            .write((char *) &bpm->object_lens[i], sizeof(bpm->object_lens[i]))
            .write((char *) &bpm->object_types[i], sizeof(bpm->object_types[i]))
            .write((char *) bpm->subjects[i], bpm->subject_lens[i])
            .write((char *) bpm->predicates[i], bpm->predicate_lens[i])
            .write((char *) bpm->objects[i], bpm->object_lens[i])) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGet *bgm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(bgm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bgm->count, sizeof(bgm->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if (!s
            .write((char *) &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]))
            .write((char *) &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]))
            .write((char *) &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]))
            .write((char *) &bgm->object_types[i], sizeof(bgm->object_types[i]))
            .write((char *) bgm->subjects[i], bgm->subject_lens[i])
            .write((char *) bgm->predicates[i], bgm->predicate_lens[i])) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BGetOp *bgm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(bgm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bgm->count, sizeof(bgm->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if (!s
            .write((char *) &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]))
            .write((char *) &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]))
            .write((char *) &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]))
            .write((char *) &bgm->object_types[i], sizeof(bgm->object_types[i]))
            .write((char *) &bgm->num_recs[i], sizeof(bgm->num_recs[i]))
            .write((char *) &bgm->ops[i], sizeof(bgm->ops[i]))
            .write((char *) bgm->subjects[i], bgm->subject_lens[i])
            .write((char *) bgm->predicates[i], bgm->predicate_lens[i])) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BDelete *bdm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(bdm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bdm->count, sizeof(bdm->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bdm->count; i++) {
        if (!s
            .write((char *) &bdm->ds_offsets[i], sizeof(bdm->ds_offsets[i]))
            .write((char *) &bdm->subject_lens[i], sizeof(bdm->subject_lens[i]))
            .write((char *) &bdm->predicate_lens[i], sizeof(bdm->predicate_lens[i]))
            .write((char *) bdm->subjects[i], bdm->subject_lens[i])
            .write((char *) bdm->predicates[i], bdm->predicate_lens[i])) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Request::BHistogram *bhist, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Request::Request *>(bhist), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bhist->count, sizeof(bhist->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bhist->count; i++) {
        if (!s
            .write((char *) &bhist->ds_offsets[i], sizeof(bhist->ds_offsets[i]))) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Response *res, std::string &buf) {
    int ret = TRANSPORT_ERROR;
    if (!res) {
        return ret;
    }

    switch (res->type) {
        case Message::PUT:
            ret = pack(static_cast<const Response::Put *>(res), buf);
            break;
        case Message::GET:
            ret = pack(static_cast<const Response::Get *>(res), buf);
            break;
        case Message::DELETE:
            ret = pack(static_cast<const Response::Delete *>(res), buf);
            break;
        case Message::HISTOGRAM:
            ret = pack(static_cast<const Response::Histogram *>(res), buf);
            break;
        case Message::BPUT:
            ret = pack(static_cast<const Response::BPut *>(res), buf);
            break;
        case Message::BGET:
            ret = pack(static_cast<const Response::BGet *>(res), buf);
            break;
        case Message::BGETOP:
            ret = pack(static_cast<const Response::BGetOp *>(res), buf);
            break;
        case Message::BDELETE:
            ret = pack(static_cast<const Response::BDelete *>(res), buf);
            break;
        default:
            break;
    }

    return ret;
}

int Packer::pack(const Response::Put *pm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(pm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &pm->status, sizeof(pm->status))
        .write((char *) &pm->ds_offset, sizeof(pm->ds_offset))) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Get *gm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(gm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &gm->status, sizeof(gm->status))
        .write((char *) &gm->ds_offset, sizeof(gm->ds_offset))
        .write((char *) &gm->subject_len, sizeof(gm->subject_len))
        .write((char *) &gm->predicate_len, sizeof(gm->predicate_len))
        .write((char *) &gm->object_type, sizeof(gm->object_type))
        .write((char *) gm->subject, gm->subject_len)
        .write((char *) gm->predicate, gm->predicate_len)) {
        return TRANSPORT_ERROR;
    }

    // only write the rest of the data if it exists
    if (gm->status == HXHIM_SUCCESS) {
        if (!s
            .write((char *) &gm->object_len, sizeof(gm->object_len))
            .write((char *) gm->object, gm->object_len)) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Delete *dm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(dm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &dm->status, sizeof(dm->status))
        .write((char *) &dm->ds_offset, sizeof(dm->ds_offset))) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Histogram *hist, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(hist), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &hist->status, sizeof(hist->status))
        .write((char *) &hist->ds_offset, sizeof(hist->ds_offset))) {
        return TRANSPORT_ERROR;
    }

    const std::size_t size = hist->hist.size;

    if (!s
        .write((char *) &size, sizeof(size))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < size; i++) {

        if (!s
            .write((char *) &hist->hist.buckets[i], sizeof(hist->hist.buckets[i]))
            .write((char *) &hist->hist.counts[i], sizeof(hist->hist.counts[i]))) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BPut *bpm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(bpm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &bpm->count, sizeof(bpm->count))
        .write((char *) bpm->ds_offsets, sizeof(*bpm->ds_offsets) * bpm->count)
        .write((char *) bpm->statuses, sizeof(*bpm->statuses) * bpm->count)) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGet *bgm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(bgm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bgm->count, sizeof(bgm->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if (!s
            .write((char *) &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]))
            .write((char *) &bgm->statuses[i], sizeof(bgm->statuses[i]))
            .write((char *) &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]))
            .write((char *) &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]))
            .write((char *) &bgm->object_types[i], sizeof(bgm->object_types[i]))
            .write((char *) bgm->subjects[i], bgm->subject_lens[i])
            .write((char *) bgm->predicates[i], bgm->predicate_lens[i])) {
            return TRANSPORT_ERROR;
        }

        if ((bgm->statuses[i] == HXHIM_SUCCESS) &&
            !s
            .write((char *) &bgm->object_lens[i], sizeof(bgm->object_lens[i]))
            .write((char *) bgm->objects[i], bgm->object_lens[i])) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BGetOp *bgm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(bgm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s.write((char *) &bgm->count, sizeof(bgm->count))) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bgm->count; i++) {
        if (!s
            .write((char *) &bgm->ds_offsets[i], sizeof(bgm->ds_offsets[i]))
            .write((char *) &bgm->statuses[i], sizeof(bgm->statuses[i]))
            .write((char *) &bgm->subject_lens[i], sizeof(bgm->subject_lens[i]))
            .write((char *) &bgm->predicate_lens[i], sizeof(bgm->predicate_lens[i]))
            .write((char *) &bgm->object_types[i], sizeof(bgm->object_types[i]))
            .write((char *) bgm->subjects[i], bgm->subject_lens[i])
            .write((char *) bgm->predicates[i], bgm->predicate_lens[i])) {
            return TRANSPORT_ERROR;
        }

        if ((bgm->statuses[i] == HXHIM_SUCCESS) &&
            !s
            .write((char *) &bgm->object_lens[i], sizeof(bgm->object_lens[i]))
            .write((char *) bgm->objects[i], bgm->object_lens[i])) {
            return TRANSPORT_ERROR;
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BDelete *bdm, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(bdm), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &bdm->count, sizeof(bdm->count))
        .write((char *) bdm->ds_offsets, sizeof(*bdm->ds_offsets) * bdm->count)
        .write((char *) bdm->statuses, sizeof(*bdm->statuses) * bdm->count)) {
        return TRANSPORT_ERROR;
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::BHistogram *bhist, std::string &buf) {
    std::stringstream s;
    if (pack(static_cast<const Response::Response *>(bhist), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    if (!s
        .write((char *) &bhist->count, sizeof(bhist->count))
        .write((char *) bhist->ds_offsets, sizeof(*bhist->ds_offsets) * bhist->count)
        .write((char *) bhist->statuses, sizeof(*bhist->statuses) * bhist->count)) {
        return TRANSPORT_ERROR;
    }

    for(std::size_t i = 0; i < bhist->count; i++) {
        const std::size_t size = bhist->hists[i].size;
        if (!s
            .write((char *) &size, sizeof(size))) {
            return TRANSPORT_ERROR;
        }
        for(std::size_t j = 0; j < size; j++) {
            if (!s
                .write((char *) &bhist->hists[i].buckets[j], sizeof(&bhist->hists[i].buckets[j]))
                .write((char *) &bhist->hists[i].counts[j], sizeof(bhist->hists[i].counts[j]))) {
                return TRANSPORT_ERROR;
            }
        }
    }

    buf = s.str();
    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Message *msg, std::stringstream &s) {
    if (!msg) {
        return TRANSPORT_ERROR;
    }

    return s
        .write((char *)&msg->direction, sizeof(msg->direction))
        .write((char *)&msg->type, sizeof(msg->type))
        .write((char *) &msg->src, sizeof(msg->src))
        .write((char *) &msg->dst, sizeof(msg->dst))?TRANSPORT_SUCCESS:TRANSPORT_ERROR;
}

int Packer::pack(const Request::Request *req, std::stringstream &s) {
    if (pack(static_cast<const Message *>(req), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

int Packer::pack(const Response::Response *res, std::stringstream &s) {
    if (pack(static_cast<const Message *>(res), s) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    return TRANSPORT_SUCCESS;
}

}
}

#endif
