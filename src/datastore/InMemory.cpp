#include <ctime>
#include <sstream>
#include <stdexcept>

#include "datastore/InMemory.hpp"
#include "hxhim/private.hpp"
#include "hxhim/triplestore.hpp"
#include "utils/Blob.hpp"
#include "utils/elapsed.h"
#include "utils/memory.hpp"

namespace hxhim {
namespace datastore {

using namespace Transport;

InMemory::InMemory(hxhim_t *hx,
                   Histogram::Histogram *hist,
                   const std::string &exact_name)
    : Datastore(hx, 0, hist),
      db()
{
    Datastore::Open(exact_name);
}

InMemory::InMemory(hxhim_t *hx,
                   const int id,
                   Histogram::Histogram *hist,
                   const std::string &basename)
    : Datastore(hx, id, hist),
      db()
{
    Datastore::Open(basename);
}

InMemory::~InMemory() {
    Close();
}

bool InMemory::OpenImpl(const std::string &) {
    return true;
}

void InMemory::CloseImpl() {
    db.clear();
}

/**
 * BPut
 * Performs a bulk PUT in
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Response::BPut *InMemory::BPutImpl(Transport::Request::BPut *req) {
    Response::BPut *res = construct<Response::BPut>(req->count);

    struct timespec start = {};
    struct timespec end = {};

    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        db[std::string((char *) key, key_len)] = std::string((char *) req->objects[i]->ptr, req->objects[i]->len);
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        stats.puts++;
        stats.put_times += nano(start, end);

        // always successful
        res->statuses[i] = HXHIM_SUCCESS;
    }

    res->count = req->count;

    stats.put_times += nano(start, end);

    return res;
}

/**
 * BGet
 * Performs a bulk GET in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Response::BGet2 *InMemory::BGetImpl2(Transport::Request::BGet2 *req) {
    Response::BGet2 *res = construct<Response::BGet2>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        struct timespec start = {};
        struct timespec end = {};
        std::string value_str;

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        // move data into res
        res->subjects[i] = construct<RealBlob>(req->subjects[i]->ptr, req->subjects[i]->len);
        req->subjects[i]->ptr = nullptr;
        req->subjects[i]->len = 0;
        res->predicates[i] = construct<RealBlob>(req->predicates[i]->ptr, req->predicates[i]->len);
        req->predicates[i]->ptr = nullptr;
        req->predicates[i]->len = 0;
        res->object_types[i] = req->object_types[i];

        // copy requester addresses
        res->orig.subjects[i] = req->orig.subjects[i];
        res->orig.predicates[i] = req->orig.predicates[i];
        res->orig.objects[i] = req->orig.objects[i];
        res->orig.object_lens[i] = req->orig.object_lens[i];

        res->statuses[i] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;

        if (res->statuses[i] == HXHIM_SUCCESS) {
            res->objects[i] = construct<RealBlob>(it->second.size(), it->second.data());
        }

        stats.gets++;
        stats.get_times += nano(start, end);
    }

    res->count = req->count;

    return res;
}

/**
 * BGetOp
 * Performs a GetOp in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return pointer to a list of results
 */
Response::BGetOp *InMemory::BGetOpImpl(Transport::Request::BGetOp *req) {
    Response::BGetOp *res = construct<Response::BGetOp>(0);
    Response::BGetOp *curr = res;

    for(std::size_t i = 0; i < req->count; i++) {
        Response::BGetOp *response = construct<Response::BGetOp>(req->num_recs[i]);

        struct timespec start = {};
        struct timespec end = {};

        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        // add in the time to get the first key-value without adding to the counter
        clock_gettime(CLOCK_MONOTONIC, &start);
        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        clock_gettime(CLOCK_MONOTONIC, &end);

        dealloc(key);

        decltype(db)::const_reverse_iterator rit = std::make_reverse_iterator(it);

        stats.get_times += nano(start, end);

        if (it != db.end()) {
            for(std::size_t j = 0; j < req->num_recs[i] && (it != db.end()) && (rit != db.rend()); j++) {
                response->statuses[j] = (it != db.end())?HXHIM_SUCCESS:HXHIM_ERROR;
                if (response->statuses[j] == HXHIM_SUCCESS) {
                    key_to_sp((void *) it->first.data(), it->first.size(), &response->subjects[j]->ptr, &response->subjects[j]->len, &response->predicates[j]->ptr, &response->predicates[j]->len);
                    response->object_types[j] = req->object_types[i];
                    response->objects[j]->ptr = alloc(it->second.size());
                    memcpy(response->objects[j]->ptr, it->second.data(), it->second.size());
                }

                clock_gettime(CLOCK_MONOTONIC, &start);
                response->count++;

                // move to next iterator according to operation
                switch (req->ops[i]) {
                    case hxhim_get_op_t::HXHIM_GET_NEXT:
                        it++;
                        break;
                    case hxhim_get_op_t::HXHIM_GET_PREV:
                        it--;
                        break;
                    case hxhim_get_op_t::HXHIM_GET_FIRST:
                        it++;
                        break;
                    case hxhim_get_op_t::HXHIM_GET_LAST:
                        it--;
                        break;
                    default:
                        break;
                }

                clock_gettime(CLOCK_MONOTONIC, &end);
                stats.gets++;
                stats.get_times += nano(start, end);
            }

            curr = (curr->next = response);
        }
    }

    // first result is empty
    return res->next;
}

/**
 * BDelete
 * Performs a bulk DELETE in InMemory
 *
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @return pointer to a list of results
 */
Response::BDelete *InMemory::BDeleteImpl(Transport::Request::BDelete *req) {
    Response::BDelete *res = construct<Response::BDelete>(req->count);

    for(std::size_t i = 0; i < req->count; i++) {
        void *key = nullptr;
        std::size_t key_len = 0;
        sp_to_key(req->subjects[i]->ptr, req->subjects[i]->len, req->predicates[i]->ptr, req->predicates[i]->len, &key, &key_len);

        decltype(db)::const_iterator it = db.find(std::string((char *) key, key_len));
        if (it != db.end()) {
            db.erase(it);
            res->statuses[i] = HXHIM_SUCCESS;
        }
        else {
            res->statuses[i] = HXHIM_ERROR;
        }

        dealloc(key);
    }

    res->count = req->count;

    return res;
}

/**
 * Sync
 * NOOP
 *
 * @return HXHIM_SUCCESS
 */
int InMemory::SyncImpl() {
    return HXHIM_SUCCESS;
}

}
}
