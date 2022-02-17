#ifndef DATASTORE_TPP
#define DATASTORE_TPP

#include "datastore/triplestore.hpp"
#include "message/Messages.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

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
        // cannot call string constructor because key might not be string
        Blob encoded_subject;
        Blob encoded_predicate;
        key_to_sp(ReferenceBlob((void *) key.data(), key.size(), hxhim_data_t::HXHIM_DATA_BYTE),
                  encoded_subject, encoded_predicate, false);

        if (encoded_subject.data_type() != req->subjects[i].data_type()) {
            mlog(DATASTORE_WARN, "GETOP Decoded subject data type (%s) does not match provided data type (%s). Using decoded type.",
                 HXHIM_DATA_STR[encoded_subject.data_type()],
                 HXHIM_DATA_STR[req->subjects[i].data_type()]);
        }

        if (encoded_predicate.data_type() != req->predicates[i].data_type()) {
            mlog(DATASTORE_WARN, "GETOP Decoded predicate data type (%s) does not match provided data type (%s).  Using decoded type.",
                 HXHIM_DATA_STR[encoded_predicate.data_type()],
                 HXHIM_DATA_STR[req->predicates[i].data_type()]);
        }

        std::size_t value_len = value.size();
        hxhim_data_t value_type = remove_type((char *) value.data(), value_len);
        Blob encoded_object = ReferenceBlob((char *) value.data(), value_len, value_type);
        if (encoded_object.data_type() != req->object_types[i]) {
            mlog(DATASTORE_WARN, "GETOP Decoded object data type (%s) does not match provided data type (%s). Using decoded type.",
                 HXHIM_DATA_STR[encoded_object.data_type()],
                 HXHIM_DATA_STR[req->object_types[i]]);
        }

        void *subject = nullptr;
        std::size_t subject_len = 0;
        void *predicate = nullptr;
        std::size_t predicate_len = 0;
        void *object = nullptr;
        std::size_t object_len = 0;

        // decode the subject and predicate
        if ((decode(callbacks, encoded_subject,   &subject,   &subject_len)   == DATASTORE_SUCCESS) &&
            (decode(callbacks, encoded_predicate, &predicate, &predicate_len) == DATASTORE_SUCCESS) &&
            (decode(callbacks, encoded_object,    &object, &object_len)       == DATASTORE_SUCCESS)) {
            res->subjects[i][j]   = std::move(RealBlob(subject,   subject_len,   encoded_subject.data_type()));
            res->predicates[i][j] = std::move(RealBlob(predicate, predicate_len, encoded_predicate.data_type()));
            res->objects[i][j]    = std::move(RealBlob(object,    object_len,    encoded_object.data_type()));
            res->num_recs[i]++;
            event.size += key.size() + value.size();
        }
        else {
            res->statuses[i] = DATASTORE_ERROR;
        }
    }
}

#endif
