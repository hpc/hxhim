#include "hxhim/private/cache.hpp"
#include "utils/Blob.hpp"

hxhim::UserData::~UserData() {}

hxhim::SubjectPredicate::SubjectPredicate()
    : UserData(),
      subject(nullptr),
      predicate(nullptr)
{}

hxhim::SubjectPredicate::~SubjectPredicate() {}

hxhim::PutData::PutData()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      object(nullptr),
      prev(nullptr),
      next(nullptr)
{}

int hxhim::PutData::moveto(Transport::Request::BPut *bput, const int ds_offset) {
    if (!bput) {
        return HXHIM_ERROR;
    }

    bput->ds_offsets[bput->count] = ds_offset;
    bput->subjects[bput->count] = subject; subject = nullptr;
    bput->predicates[bput->count] = predicate; predicate = nullptr;
    bput->object_types[bput->count] = object_type;
    bput->objects[bput->count] = object; object = nullptr;
    bput->count++;

    return HXHIM_SUCCESS;
}

hxhim::GetData::GetData()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetData::~GetData() {}

int hxhim::GetData::moveto(Transport::Request::BGet *bget, const int ds_offset) {
    if (!bget) {
        return HXHIM_ERROR;
    }

    bget->ds_offsets[bget->count] = ds_offset;
    bget->subjects[bget->count] = subject; subject = nullptr;
    bget->predicates[bget->count] = predicate; predicate = nullptr;
    bget->object_types[bget->count] = object_type;

    bget->orig.subjects[bget->count] = bget->subjects[bget->count]->data();
    bget->orig.predicates[bget->count] = bget->predicates[bget->count]->data();

    bget->count++;

    return HXHIM_SUCCESS;

}

hxhim::GetOpData::GetOpData()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      num_recs(0),
      op(HXHIM_GET_INVALID),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetOpData::~GetOpData() {}

int hxhim::GetOpData::moveto(Transport::Request::BGetOp *bgetop, const int ds_offset) {
    if (!bgetop) {
        return HXHIM_ERROR;
    }

    bgetop->ds_offsets[bgetop->count] = ds_offset;
    bgetop->subjects[bgetop->count] = subject; subject = nullptr;
    bgetop->predicates[bgetop->count] = predicate; predicate = nullptr;
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

int hxhim::DeleteData::moveto(Transport::Request::BDelete *bdel, const int ds_offset) {
    if (!bdel) {
        return HXHIM_ERROR;
    }

    bdel->ds_offsets[bdel->count] = ds_offset;
    bdel->subjects[bdel->count] = subject; subject = nullptr;
    bdel->predicates[bdel->count] = predicate; predicate = nullptr;
    bdel->count++;

    return HXHIM_SUCCESS;

}
