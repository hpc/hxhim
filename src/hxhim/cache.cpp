#include <utility>

#include "hxhim/private/cache.hpp"

hxhim::UserData::UserData()
    : ds_id(-1),
      ds_rank(-1),
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

hxhim::SubjectPredicate::SubjectPredicate()
    : UserData(),
      subject(),
      predicate()
{}

hxhim::SubjectPredicate::~SubjectPredicate() {}

int hxhim::SubjectPredicate::steal(Transport::Request::Request *req) {
    // cannot steal subject+predicate
    // since Transport::Request::Request does not have them
    return UserData::steal(req);
}

hxhim::PutData::PutData()
    : SP_t(),
      object_type(HXHIM_OBJECT_TYPE_INVALID),
      object(),
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

hxhim::GetData::GetData()
    : SP_t(),
      object_type(HXHIM_OBJECT_TYPE_INVALID),
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

hxhim::GetOpData::GetOpData()
    : SP_t(),
      object_type(HXHIM_OBJECT_TYPE_INVALID),
      num_recs(0),
      op(HXHIM_GETOP_INVALID),
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

hxhim::DeleteData::DeleteData()
    : SP_t(),
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

hxhim::HistogramData::HistogramData()
    : UserData(),
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
