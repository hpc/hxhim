#ifndef DATASTORE_TPP
#define DATASTORE_TPP

#include "message/Messages.hpp"

/**
 * key_to_sp
 * Splits a key into a subject key pair.
 *
 * @param key            the key
 * @param subject        the subject of the triple
 * @param predicate      the predicate of the triple
 * @paral copy           whether the subject and predicate are copies or references to the key
 * @return HXHIM_SUCCESS, or HXHIM_ERROR on error
 */
template <typename Key_t>
int key_to_sp(const Key_t &key,
              Blob &subject,
              Blob &predicate,
              const bool copy) {
    // cannot call string constructor because key might not be string
    return key_to_sp(ReferenceBlob((void *) key.data(), key.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                     subject, predicate,
                     copy);
}

template <typename Key_t, typename Value_t>
void Datastore::Datastore::BGetOp_copy_response(Transform::Callbacks *callbacks,
                                                const Key_t &key,
                                                const Value_t &value,
                                                Message::Request::BGetOp *req,
                                                Message::Response::BGetOp *res,
                                                const std::size_t i,
                                                const std::size_t j,
                                                Datastore::Datastore::Stats::Event &event) {
    if (res->statuses[i] == DATASTORE_UNSET) {
        // extract the encoded subject and predicate from the key
        Blob encoded_subject;
        Blob encoded_predicate;
        key_to_sp(key, encoded_subject, encoded_predicate, false);
        encoded_subject.set_type(req->subjects[i].data_type());
        encoded_predicate.set_type(req->predicates[i].data_type());

        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        void *object = nullptr;
        std::size_t object_len = 0;

        // decode the subject and predicate
        if ((decode(callbacks, encoded_subject,   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (decode(callbacks, encoded_predicate, &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (decode(callbacks, ReferenceBlob((void *) value.data(), value.size(), req->object_types[i]),
                    &object, &object_len)                                     == DATASTORE_SUCCESS)) {
            res->subjects[i][j]   = std::move(RealBlob(subject,   subject_len,   req->subjects[i].data_type()));
            res->predicates[i][j] = std::move(RealBlob(predicate, predicate_len, req->predicates[i].data_type()));
            res->objects[i][j]    = std::move(RealBlob(object,    object_len,    req->object_types[i]));
            res->num_recs[i]++;
            event.size += key.size() + value.size();
        }
        else {
            res->statuses[i] = DATASTORE_ERROR;
        }
    }
}

#endif
