#include "hxhim/cache.hpp"

hxhim::SubjectPredicate::SubjectPredicate(FixedBufferPool *arrays, const std::size_t count)
    : arrays(arrays),
      count(count),
      subjects(arrays->acquire_array<void *>(count)),
      subject_lens(arrays->acquire_array<std::size_t>(count)),
      predicates(arrays->acquire_array<void *>(count)),
      predicate_lens(arrays->acquire_array<std::size_t>(count))
{}

hxhim::SubjectPredicate::~SubjectPredicate() {
    arrays->release_array(subjects, count);
    arrays->release_array(subject_lens, count);
    arrays->release_array(predicates, count);
    arrays->release_array(predicate_lens, count);
}

hxhim::SubjectPredicateObject::SubjectPredicateObject(FixedBufferPool *arrays, const std::size_t count)
    : SP_t(arrays, count),
      object_types(arrays->acquire_array<hxhim_type_t>(count)),
      objects(arrays->acquire_array<void *>(count)),
      object_lens(arrays->acquire_array<std::size_t>(count))
{}

hxhim::SubjectPredicateObject::~SubjectPredicateObject() {
    arrays->release_array(object_types, count);
    arrays->release_array(objects, count);
    arrays->release_array(object_lens, count);
}

hxhim::PutData::PutData(FixedBufferPool *arrays, const std::size_t count)
    : SPO_t(arrays, count),
      prev(nullptr),
      next(nullptr)
{}

hxhim::GetData::GetData(FixedBufferPool *arrays, const std::size_t count)
    : SP_t(arrays, count),
      object_types(arrays->acquire_array<hxhim_type_t>(count)),
      next(nullptr)
{}

hxhim::GetData::~GetData() {
    arrays->release_array(object_types, count);
}

hxhim::GetOpData::GetOpData(FixedBufferPool *arrays, const std::size_t count)
    : SP_t(arrays, count),
      object_types(arrays->acquire_array<hxhim_type_t>(count)),
      num_recs(arrays->acquire_array<std::size_t>(count)),
      ops(arrays->acquire_array<hxhim_get_op_t>(count)),
      next(nullptr)
{}

hxhim::GetOpData::~GetOpData() {
    arrays->release_array(object_types, count);
    arrays->release_array(num_recs, count);
    arrays->release_array(ops, count);
}

hxhim::DeleteData::DeleteData(FixedBufferPool *arrays, const std::size_t count)
    : SP_t(arrays, count),
      next(nullptr)
{}
