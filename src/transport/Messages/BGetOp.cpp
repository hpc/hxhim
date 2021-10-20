#include "transport/Messages/BGetOp.hpp"

Transport::Request::BGetOp::BGetOp(const std::size_t max)
    : SubjectPredicate(hxhim_op_t::HXHIM_GETOP),
      object_types(nullptr),
      num_recs(nullptr),
      ops(nullptr)
{
    alloc(max);
}

Transport::Request::BGetOp::~BGetOp() {
    cleanup();
}

void Transport::Request::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        SubjectPredicate::alloc(max);
        object_types = alloc_array<hxhim_data_t>(max);
        num_recs     = alloc_array<std::size_t>(max);
        ops          = alloc_array<hxhim_getop_t>(max);
    }
}

std::size_t Transport::Request::BGetOp::add(Blob subject, Blob predicate,
                                            hxhim_data_t object_type,
                                            std::size_t num_rec,
                                            hxhim_getop_t op) {
    subjects[count] = subject;
    predicates[count] = predicate;
    object_types[count] = object_type;
    num_recs[count] = num_rec;
    ops[count] = op;

    // use Request::add instead of SubjectPredicate::add
    // to have mroe control of values added
    // original subject and predicate addresses are not added
    if ((op != hxhim_getop_t::HXHIM_GETOP_FIRST) &&
        (op != hxhim_getop_t::HXHIM_GETOP_LAST)) {
        Request::add(subject.pack_size(true) +
                     predicate.pack_size(true),
                     false);
    }

    return Request::add(sizeof(object_type) +
                 sizeof(num_rec) + sizeof(op),
                 true);
}

int Transport::Request::BGetOp::steal(Transport::Request::BGetOp *from, const std::size_t i) {
    if (SubjectPredicate::steal(from, i) != TRANSPORT_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    object_types[count] = from->object_types[i];
    num_recs[count]     = from->num_recs[i];
    ops[count]          = from->ops[i];

    count++;

    return TRANSPORT_SUCCESS;
}

int Transport::Request::BGetOp::cleanup() {
    dealloc_array(subjects, max_count);
    subjects = nullptr;

    dealloc_array(predicates, max_count);
    predicates = nullptr;

    dealloc_array(object_types, max_count);
    object_types = nullptr;

    dealloc_array(num_recs, max_count);
    num_recs = nullptr;

    dealloc_array(ops, max_count);
    ops = nullptr;

    return SubjectPredicate::cleanup();
}

Transport::Response::BGetOp::BGetOp(const std::size_t max)
    : Response(hxhim_op_t::HXHIM_GETOP),
      num_recs(nullptr),
      subjects(nullptr),
      predicates(nullptr),
      objects(nullptr)
{
    alloc(max);
}

Transport::Response::BGetOp::~BGetOp() {
    cleanup();
}

void Transport::Response::BGetOp::alloc(const std::size_t max) {
    cleanup();

    if (max) {
        Response::alloc(max);
        num_recs     = alloc_array<std::size_t>(max);
        subjects     = alloc_array<Blob *>(max);
        predicates   = alloc_array<Blob *>(max);
        objects      = alloc_array<Blob *>(max);
    }
}

std::size_t Transport::Response::BGetOp::add(Blob *subject,
                                             Blob *predicate,
                                             Blob *object,
                                             std::size_t num_rec,
                                             int status) {

    size_t ds = sizeof(num_rec);
    for(std::size_t i = 0; i < num_rec; i++) {
        ds += subject[i].pack_size(true) +
            predicate[i].pack_size(true) +
            object[i].pack_size(true);
    }

    subjects[count] = subject;
    predicates[count] = predicate;
    objects[count] = object;
    num_recs[count] = num_rec;

    // status is shared by all responses
    return Response::add(status, sizeof(num_rec) + ds, true);
}

std::size_t Transport::Response::BGetOp::update_size(const std::size_t index) {
    for(std::size_t i = 0; i < num_recs[index]; i++) {
        Message::add(subjects[index][i].pack_size(true) + predicates[index][i].pack_size(true), false);

        // all records from this response share the same status
        if (statuses[index] == DATASTORE_SUCCESS) {
            Message::add(objects[index][i].pack_size(true), false);
        }
    }

    return size();
}

int Transport::Response::BGetOp::steal(Transport::Response::BGetOp *from, const std::size_t i) {
    add(from->subjects[i],
        from->predicates[i],
        from->objects[i],
        from->num_recs[i],
        from->statuses[i]);

    from->subjects[i]   = nullptr;
    from->predicates[i] = nullptr;
    from->objects[i]    = nullptr;

    return HXHIM_SUCCESS;
}

int Transport::Response::BGetOp::cleanup() {
    for(std::size_t i = 0; i < count; i++) {
        dealloc_array(subjects[i],   num_recs[i]);
        dealloc_array(predicates[i], num_recs[i]);
        dealloc_array(objects[i],    num_recs[i]);
    }

    dealloc_array(num_recs, max_count);
    num_recs = nullptr;

    dealloc_array(subjects, max_count);
    subjects = nullptr;

    dealloc_array(predicates, max_count);
    predicates = nullptr;

    dealloc_array(objects, max_count);
    objects = nullptr;

    return Response::cleanup();
}
