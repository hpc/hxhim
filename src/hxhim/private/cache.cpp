#include <utility>

#include "hxhim/private/cache.hpp"

hxhim::UserData::UserData()
    : ds_id(-1),
      ds_rank(-1),
      ds_offset(-1),
      timestamps()
{
    timestamps.cached = ::Stats::now();
}

hxhim::UserData::~UserData() {}

hxhim::SubjectPredicate::SubjectPredicate()
    : UserData(),
      subject(),
      predicate()
{}

hxhim::SubjectPredicate::~SubjectPredicate() {}

hxhim::PutData::PutData()
    : SP_t(),
      object_type(HXHIM_OBJECT_TYPE_INVALID),
      object(),
      prev(nullptr),
      next(nullptr)
{}

int hxhim::PutData::moveto(Transport::Request::BPut *bput) {
    if (!bput) {
        return HXHIM_ERROR;
    }

    if (bput->count >= bput->max_count) {
        return HXHIM_ERROR;
    }

    bput->ds_offsets[bput->count] = ds_offset;
    bput->timestamps.reqs[bput->count] = std::move(timestamps);
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
    if (!bget) {
        return HXHIM_ERROR;
    }

    if (bget->count >= bget->max_count) {
        return HXHIM_ERROR;
    }

    bget->ds_offsets[bget->count] = ds_offset;
    bget->timestamps.reqs[bget->count] = std::move(timestamps);
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
      op(HXHIM_GET_INVALID),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetOpData::~GetOpData() {}

int hxhim::GetOpData::moveto(Transport::Request::BGetOp *bgetop) {
    if (!bgetop) {
        return HXHIM_ERROR;
    }

    if (bgetop->count >= bgetop->max_count) {
        return HXHIM_ERROR;
    }

    bgetop->ds_offsets[bgetop->count] = ds_offset;
    bgetop->timestamps.reqs[bgetop->count] = std::move(timestamps);
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
    if (!bdel) {
        return HXHIM_ERROR;
    }

    if (bdel->count >= bdel->max_count) {
        return HXHIM_ERROR;
    }

    bdel->ds_offsets[bdel->count] = ds_offset;
    bdel->timestamps.reqs[bdel->count] = std::move(timestamps);
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
    if (!bhist) {
        return HXHIM_ERROR;
    }

    if (bhist->count >= bhist->max_count) {
        return HXHIM_ERROR;
    }

    bhist->ds_offsets[bhist->count] = ds_offset;
    bhist->timestamps.reqs[bhist->count] = std::move(timestamps);

    bhist->count++;

    return HXHIM_SUCCESS;

}
