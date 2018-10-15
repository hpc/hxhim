#include "hxhim/cache.hpp"

hxhim::SubjectPredicate::SubjectPredicate()
    : subject(nullptr),
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

hxhim::GetData::GetData()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetData::~GetData() {}

hxhim::GetOpData::GetOpData()
    : SP_t(),
      object_type(HXHIM_INVALID_TYPE),
      num_recs(0),
      op(HXHIM_GET_OP_MAX),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetOpData::~GetOpData() {}

hxhim::DeleteData::DeleteData()
    : SP_t(),
      prev(nullptr),
      next(nullptr)
{}
