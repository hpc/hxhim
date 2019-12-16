#include "hxhim/cache.hpp"

hxhim::UserData::~UserData() {}

hxhim::SubjectPredicate::SubjectPredicate()
    : UserData(),
      subject(nullptr),
      subject_len(0),
      predicate(nullptr),
      predicate_len(0)
{}

hxhim::SubjectPredicate::~SubjectPredicate() {}

hxhim::SubjectPredicateObject::SubjectPredicateObject()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      object(nullptr),
      object_len(0)
{}

hxhim::SubjectPredicateObject::~SubjectPredicateObject() {}

hxhim::PutData::PutData()
    : SPO_t(),
      prev(nullptr),
      next(nullptr)
{}

int hxhim::PutData::moveto(Transport::Request::BPut *bput, const int ds_offset) const {
    if (!bput) {
        return HXHIM_ERROR;
    }

    bput->ds_offsets[bput->count] = ds_offset;
    bput->subjects[bput->count] = construct<ReferenceBlob>(subject, subject_len);
    bput->predicates[bput->count] = construct<ReferenceBlob>(predicate, predicate_len);
    bput->object_types[bput->count] = object_type;
    bput->objects[bput->count] = construct<ReferenceBlob>(object, object_len);
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

int hxhim::GetData::moveto(Transport::Request::BGet *bget, const int ds_offset) const {
    if (!bget) {
        return HXHIM_ERROR;
    }

    bget->ds_offsets[bget->count] = ds_offset;
    bget->subjects[bget->count] = construct<ReferenceBlob>(subject, subject_len);
    bget->predicates[bget->count] = construct<ReferenceBlob>(predicate, predicate_len);
    bget->object_types[bget->count] = object_type;
    bget->count++;

    return HXHIM_SUCCESS;
}

hxhim::GetData2::GetData2()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      object(nullptr),
      object_len(nullptr),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetData2::~GetData2() {}

int hxhim::GetData2::moveto(Transport::Request::BGet2 *bget, const int ds_offset) const {
    if (!bget) {
        return HXHIM_ERROR;
    }

    bget->ds_offsets[bget->count] = ds_offset;
    bget->subjects[bget->count] = construct<ReferenceBlob>(subject, subject_len);
    bget->predicates[bget->count] = construct<ReferenceBlob>(predicate, predicate_len);
    bget->object_types[bget->count] = object_type;
    bget->objects[bget->count] = construct<ReferenceBlob>();
    // bget->objects[bget->count]->ptr = object;
    // bget->objects[bget->count]->len = *object_len;

    // save a copy of the object and object_len pointer
    bget->orig.objects[bget->count] = object;
    bget->orig.object_lens[bget->count] = object_len;

    bget->count++;

    return HXHIM_SUCCESS;

}

hxhim::GetOpData::GetOpData()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      num_recs(0),
      op(HXHIM_GET_OP_MAX),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetOpData::~GetOpData() {}

int hxhim::GetOpData::moveto(Transport::Request::BGetOp *bgetop, const int ds_offset) const {
    if (!bgetop) {
        return HXHIM_ERROR;
    }

    bgetop->ds_offsets[bgetop->count] = ds_offset;
    bgetop->subjects[bgetop->count] = construct<ReferenceBlob>(subject, subject_len);
    bgetop->predicates[bgetop->count] = construct<ReferenceBlob>(predicate, predicate_len);
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

int hxhim::DeleteData::moveto(Transport::Request::BDelete *bdel, const int ds_offset) const {
    if (!bdel) {
        return HXHIM_ERROR;
    }

    bdel->ds_offsets[bdel->count] = ds_offset;
    bdel->subjects[bdel->count] = construct<ReferenceBlob>(subject, subject_len);
    bdel->predicates[bdel->count] = construct<ReferenceBlob>(predicate, predicate_len);
    bdel->count++;

    return HXHIM_SUCCESS;

}

hxhim::BHistogramData::BHistogramData()
    : UserData()
{}

int hxhim::BHistogramData::moveto(Transport::Request::BHistogram *bhist, const int ds_offset) const {
    if (!bhist) {
        return HXHIM_ERROR;
    }

    bhist->ds_offsets[bhist->count] = ds_offset;
    bhist->count++;

    return HXHIM_SUCCESS;
}
