#ifndef DATASTORE_TPP
#define DATASTORE_TPP

#include "transport/Messages/Messages.hpp"

template <typename Key_t, typename Value_t>
void datastore::Datastore::BGetOp_copy_response(const Key_t &key,
                                                const Value_t &value,
                                                Transport::Request::BGetOp *req,
                                                Transport::Response::BGetOp *res,
                                                const std::size_t i,
                                                const std::size_t j,
                                                datastore::Datastore::Stats::Event &event) const {
    if (res->statuses[i] == DATASTORE_UNSET) {
        // extract the encoded subject and predicate from the key
        Blob encoded_subject;
        Blob encoded_predicate;
        key_to_sp(key.data(), key.size(), encoded_subject, encoded_predicate, false);

        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        void *object = nullptr;
        std::size_t object_len = 0;

        // decode the subject and predicate
        if ((decode(encoded_subject,   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (decode(encoded_predicate, &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (decode(ReferenceBlob((void *) value.data(), value.size(), req->object_types[i]),
                    &object, &object_len)                          == DATASTORE_SUCCESS)) {
            res->subjects[i][j]   = std::move(Blob(subject, subject_len, req->subjects[i].data_type(), true));
            res->predicates[i][j] = std::move(Blob(predicate, predicate_len, req->predicates[i].data_type(), true));
            res->objects[i][j]    = std::move(Blob(object, object_len,  req->object_types[i], true));
            res->num_recs[i]++;
            event.size += key.size() + value.size();
        }
        else {
            res->statuses[i] = DATASTORE_ERROR;
        }
    }
}

#endif
