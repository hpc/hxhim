#include <utility>

#include "datastore/datastore.hpp"
#include "hxhim/private/cache.hpp"
#include "hxhim/private/hxhim.hpp"

hxhim::UserData::UserData(hxhim_t *hx, const int id)
    : ds_id(id),
      ds_rank(hxhim::datastore::get_rank(hx, id)),
      timestamps(construct<::Stats::Send>())
{
    timestamps->cached.start = ::Stats::now();
}

hxhim::UserData::~UserData() {
    destruct(timestamps);
}

int hxhim::UserData::steal(Transport::Request::Request *req) {
    if (!req) {
        return HXHIM_ERROR;
    }

    if (req->count >= req->max_count) {
        return HXHIM_ERROR;
    }

    // ds_id and ds_rank should have already been set and is the same for everyone
    req->timestamps.reqs[req->count] = timestamps;

    timestamps = nullptr;

    // count not updated here

    return HXHIM_SUCCESS;
}

hxhim::SubjectPredicate::SubjectPredicate(hxhim_t *hx,
                                          Blob subject, Blob predicate)
    : UserData(hx,
               hx->p->hash.func(hx,
                                subject.data(), subject.size(),
                                predicate.data(), predicate.size(),
                                hx->p->hash.args)),
      subject(subject),
      predicate(predicate)
{}

hxhim::SubjectPredicate::~SubjectPredicate() {}

int hxhim::SubjectPredicate::steal(Transport::Request::Request *req) {
    // cannot steal subject+predicate
    // since Transport::Request::Request does not have them
    return UserData::steal(req);
}

hxhim::PutData::PutData(hxhim_t *hx,
                        Blob subject, Blob predicate,
                        const hxhim_object_type_t object_type, Blob object)
    : SP_t(hx, subject, predicate),
      object_type(object_type),
      object(object),
      prev(nullptr),
      next(nullptr)
{}

int hxhim::PutData::moveto(Transport::Request::BPut *bput) {
    if (SubjectPredicate::steal(bput) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    bput->subjects[bput->count] = subject;
    bput->predicates[bput->count] = predicate;
    bput->object_types[bput->count] = object_type;
    bput->objects[bput->count] = object;

    bput->orig.subjects[bput->count] = bput->subjects[bput->count].data();
    bput->orig.predicates[bput->count] = bput->predicates[bput->count].data();

    bput->count++;

    return HXHIM_SUCCESS;
}

hxhim::GetData::GetData(hxhim_t *hx,
                        Blob subject, Blob predicate,
                        const hxhim_object_type_t object_type)
    : SP_t(hx, subject, predicate),
      object_type(object_type),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetData::~GetData() {}

int hxhim::GetData::moveto(Transport::Request::BGet *bget) {
    if (SubjectPredicate::steal(bget) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    bget->subjects[bget->count] = subject;
    bget->predicates[bget->count] = predicate;
    bget->object_types[bget->count] = object_type;

    bget->orig.subjects[bget->count] = bget->subjects[bget->count].data();
    bget->orig.predicates[bget->count] = bget->predicates[bget->count].data();

    bget->count++;

    return HXHIM_SUCCESS;
}

hxhim::GetOpData::GetOpData(hxhim_t *hx,
                            Blob subject, Blob predicate,
                            const hxhim_object_type_t object_type,
                            const std::size_t num_recs,
                            const hxhim_getop_t op)
    : SP_t(hx, subject, predicate),
      object_type(object_type),
      num_recs(num_recs),
      op(op),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetOpData::~GetOpData() {}

int hxhim::GetOpData::moveto(Transport::Request::BGetOp *bgetop) {
    if (SubjectPredicate::steal(bgetop) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    bgetop->subjects[bgetop->count] = subject;
    bgetop->predicates[bgetop->count] = predicate;
    bgetop->object_types[bgetop->count] = object_type;
    bgetop->num_recs[bgetop->count] = num_recs;
    bgetop->ops[bgetop->count] = op;

    bgetop->count++;

    return HXHIM_SUCCESS;
}

hxhim::DeleteData::DeleteData(hxhim_t *hx,
                              Blob subject, Blob predicate)
    : SP_t(hx, subject, predicate),
      prev(nullptr),
      next(nullptr)
{}

int hxhim::DeleteData::moveto(Transport::Request::BDelete *bdel) {
    if (SubjectPredicate::steal(bdel) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    bdel->subjects[bdel->count] = subject;
    bdel->predicates[bdel->count] = predicate;

    bdel->orig.subjects[bdel->count] = bdel->subjects[bdel->count].data();
    bdel->orig.predicates[bdel->count] = bdel->predicates[bdel->count].data();

    bdel->count++;

    return HXHIM_SUCCESS;
}

const Blob hxhim::HistogramData::subject = {};
const Blob hxhim::HistogramData::predicate = {};

hxhim::HistogramData::HistogramData(hxhim_t *hx,
                                    const int id)
    : UserData(hx, id),
      prev(nullptr),
      next(nullptr)
{}

int hxhim::HistogramData::moveto(Transport::Request::BHistogram *bhist) {
    if (UserData::steal(bhist) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    bhist->count++;

    return HXHIM_SUCCESS;

}
