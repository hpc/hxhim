#include <algorithm>
#include <cmath>
#include <cstring>
#include <list>

#include "hxhim/Results_private.hpp"
#include "hxhim/backend/backends.hpp"
#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/hxhim.h"
#include "hxhim/hxhim.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "hxhim/types_struct.hpp"
#include "utils/elen.hpp"

/**
 * encode
 * Converts the contents of a void * into another
 * format if the type is floating point.
 * This is allowed because the pointers are coming
 * from the internal arrays, and thus can be
 * overwritten/replaced with a new pointer.
 *
 * @param type   the underlying type of this value
 * @param ptr    address of the value
 * @param len    size of the memory being pointed to
 * @param copied whether or not the original ptr was replaced by a copy that needs to be deallocated
 * @param HXHIM_SUCCESS or HXHIM_ERROR
 */
static int encode(const hxhim_spo_type_t type, void *&ptr, std::size_t &len, bool &copied) {
    if (!ptr) {
        return HXHIM_ERROR;
    }

    switch (type) {
        case HXHIM_SPO_FLOAT_TYPE:
            {
                const std::string str = elen::encode::floating_point(* (float *) ptr);
                len = str.size();
                ptr = ::operator new(len);
                memcpy(ptr, str.c_str(), len);
                copied = true;
            }
            break;
        case HXHIM_SPO_DOUBLE_TYPE:
            {
                const std::string str = elen::encode::floating_point(* (double *) ptr);
                len = str.size();
                ptr = ::operator new(len);
                memcpy(ptr, str.c_str(), len);
                copied = true;
            }
            break;
        case HXHIM_SPO_INT_TYPE:
        case HXHIM_SPO_SIZE_TYPE:
        case HXHIM_SPO_INT64_TYPE:
        case HXHIM_SPO_BYTE_TYPE:
            copied = false;
            break;
        default:
            return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * put_core
 * The core functionality for putting a single batch of SPO triples into the backend
 *
 * @param hx      the HXHIM context
 * @param head    the head of the list of SPO triple batches to send
 * @param count   the number of triples in each batch
 * @return Pointer to return value wrapper
 */
static hxhim::Results *put_core(hxhim_t *hx, hxhim::PutData *head, const std::size_t count) {
    if (!count) {
        return nullptr;
    }

    const std::size_t total = HXHIM_PUT_MULTIPLER * count;

    void **subjects = new void *[total]();
    std::size_t *subject_lens = new std::size_t[total]();
    void **predicates = new void *[total]();
    std::size_t *predicate_lens = new std::size_t[total]();
    void **objects = new void *[total]();
    std::size_t *object_lens = new std::size_t[total]();

    std::list <void *> ptrs;
    std::size_t offset = 0;
    for(std::size_t i = 0; i < count; i++) {
        // alias the values
        void *subject = head->subjects[i];
        std::size_t subject_len = head->subject_lens[i];
        void *predicate = head->predicates[i];
        std::size_t predicate_len = head->predicate_lens[i];
        void *object = head->objects[i];
        std::size_t object_len = head->object_lens[i];

        // add the value to the histogram
        double add_to_hist = 0;
        switch (hx->p->types.object) {
            case HXHIM_SPO_INT_TYPE:
                add_to_hist = (double) * (int *) object;
                break;
            case HXHIM_SPO_SIZE_TYPE:
                add_to_hist = (double) * (std::size_t *) object;
                break;
            case HXHIM_SPO_INT64_TYPE:
                add_to_hist = (double) * (int64_t *) object;
                break;
            case HXHIM_SPO_FLOAT_TYPE:
                add_to_hist = (double) * (float *) object;
                break;
            case HXHIM_SPO_DOUBLE_TYPE:
                add_to_hist = (double) * (double *) object;
                break;
            case HXHIM_SPO_BYTE_TYPE:
                add_to_hist = (double) * (char *) object;
                break;
        }

        hx->p->histogram->add(add_to_hist);

        // encode the values
        bool copied = false;
        encode(hx->p->types.subject, subject, subject_len, copied);
        if (copied) {
            ptrs.push_back(subject);
        }

        encode(hx->p->types.predicate, predicate, predicate_len, copied);
        if (copied) {
            ptrs.push_back(predicate);
        }

        encode(hx->p->types.object, object, object_len, copied);
        if (copied) {
            ptrs.push_back(object);
        }

        // SP -> O
        subjects[offset] = subject;
        subject_lens[offset] = subject_len;
        predicates[offset] = predicate;
        predicate_lens[offset] = predicate_len;
        objects[offset] = object;
        object_lens[offset] = object_len;
        offset++;

        // // SO -> P
        // subjects[offset] = subject;
        // subject_lens[offset] = subject_len;
        // predicates[offset] = object;
        // predicate_lens[offset] = object_len;
        // objects[offset] = predicate;
        // object_lens[offset] = predicate_len;
        // offset++;

        // // PO -> S
        // subjects[offset] = predicate;
        // subject_lens[offset] = predicate_len;
        // predicates[offset] = object;
        // predicate_lens[offset] = object_len;
        // objects[offset] = subject;
        // object_lens[offset] = subject_len;
        // offset++;

        // // PS -> O
        // subjects[offset] = predicate;
        // subject_lens[offset] = predicate_len;
        // predicates[offset] = subject;
        // predicate_lens[offset] = subject_len;
        // objects[offset] = object;
        // object_lens[offset] = object_len;
        // offset++;
    }

    // PUT the batch
    hxhim::Results *res = hx->p->backend->BPut(subjects, subject_lens, predicates, predicate_lens, objects, object_lens, total);

    // cleanup
    for(void *ptr : ptrs) {
        ::operator delete(ptr);
    }

    delete [] subjects;
    delete [] subject_lens;
    delete [] predicates;
    delete [] predicate_lens;
    delete [] objects;
    delete [] object_lens;

    return res;
}

/**
 * backgroundPUT
 * The thread that runs when the number of full batches crosses the queued bputs watermark
 *
 * @param args   hx typecast to void *
 */
static void backgroundPUT(void *args) {
    hxhim_t *hx = (hxhim_t *) args;
    if (!hx || !hx->p) {
        return;
    }

    while (hx->p->running) {
        hxhim::PutData *head = nullptr;    // the first batch of PUTs to process

        bool force = false;                // whether or not FlushPuts was called
        hxhim::PutData *last = nullptr;    // pointer to the last batch of PUTs; only valid if force is true
        std::size_t last_count = 0;        // number of SPO triples in the last batch

        // hold unsent.mutex just long enough to move queued PUTs to send queue
        {
            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            while (hx->p->running && (unsent.full_batches < hx->p->queued_bputs) && !unsent.force) {
                unsent.start_processing.wait(lock, [&]() -> bool { return !hx->p->running || (unsent.full_batches >= hx->p->queued_bputs) || unsent.force; });
            }

            // record whether or not this loop was forced, since the lock is not held
            force = unsent.force;

            if (hx->p->running) {
                unsent.full_batches = 0;

                // nothing to do
                if (!unsent.head) {
                    if (force) {
                        unsent.force = false;
                        unsent.done_processing.notify_all();
                    }
                    continue;
                }

                if (force) {
                    // current batch is not the last one
                    if (unsent.head->next) {
                        head = unsent.head;
                        last = unsent.tail;

                        // disconnect the tail from the previous batches
                        last->prev->next = nullptr;
                    }
                    // current batch is the last one
                    else {
                        head = nullptr;
                        last = unsent.head;
                    }

                    // keep track of the size of the last batch
                    last_count = unsent.last_count;
                    unsent.last_count = 0;
                    unsent.force = false;

                    // remove all of the PUTs from the queue
                    unsent.head = nullptr;
                    unsent.tail = nullptr;
                }
                // not forced
                else {
                    // current batch is not the last one
                    if (unsent.head->next) {
                        head = unsent.head;
                        unsent.head = unsent.tail;

                        // disconnect the tail from the previous batches
                        unsent.tail->prev->next = nullptr;
                    }
                    // current batch is the last one
                    else {
                        continue;
                    }
                }
            }
        }

        // process the queued PUTs
        while (hx->p->running && head) {
            // process the batch and save the results
            hxhim::Results *res = put_core(hx, head, HXHIM_MAX_BULK_PUT_OPS);

            // go to the next batch
            hxhim::PutData *next = head->next;
            delete head;
            head = next;

            {
                std::unique_lock<std::mutex> lock(hx->p->put_results_mutex);
                hx->p->put_results->Append(res);
            }

            delete res;
        }

        // if this flush was forced, notify FlushPuts
        if (force) {
            if (hx->p->running) {
                // process the batch
                hxhim::Results *res = put_core(hx, last, last_count);

                delete last;
                last = nullptr;

                {
                    std::unique_lock<std::mutex> lock(hx->p->put_results_mutex);
                    hx->p->put_results->Append(res);
                }

                delete res;
            }

            hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
            std::unique_lock<std::mutex> lock(unsent.mutex);
            unsent.done_processing.notify_all();
        }

        // clean up in case previous loop stopped early
        hxhim::clean(head);
        delete last;
    }
}

/**
 * Open
 * Start a HXHIM session
 *
 * @param hx   the HXHIM session
 * @param opts the HXHIM options to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Open(hxhim_t *hx, hxhim_options_t *opts) {
    if (!hx || !opts || !opts->p) {
        return HXHIM_ERROR;
    }

    hx->mpi = opts->p->mpi;

    if (!(hx->p = new hxhim_private_t())) {
        return HXHIM_ERROR;
    }

    hx->p->running = true;
    hx->p->types.subject = opts->p->subject_type;
    hx->p->types.predicate = opts->p->predicate_type;
    hx->p->types.object = opts->p->object_type;

    // Set up queued PUT results list
    if (!(hx->p->put_results = new hxhim::Results())) {
        Close(hx);
        return HXHIM_ERROR;
    }

    // Start the background thread
    hx->p->background_put_thread = std::thread(backgroundPUT, hx);

    // Setup the histogram

    // Find Histogram Bucket Generation Method Extra Arguments
    void *bucket_gen_extra = nullptr;
    std::map <std::string, void *>::const_iterator extra_args_it = HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS.find(opts->p->histogram_bucket_gen_method);
    if (extra_args_it != HXHIM_HISTOGRAM_BUCKET_GENERATOR_EXTRA_ARGS.end()) {
        bucket_gen_extra = extra_args_it->second;
    }

    // Get the bucket generator and create the histogram
    hx->p->histogram = new Histogram::Histogram(opts->p->histogram_first_n,
                                                HXHIM_HISTOGRAM_BUCKET_GENERATORS.at(opts->p->histogram_bucket_gen_method),
                                                bucket_gen_extra);

    if (!hx->p->histogram) {
        Close(hx);
        return HXHIM_ERROR;
    }

    // Start the backend
    switch (opts->p->backend) {
        case HXHIM_BACKEND_MDHIM:
            {
                hxhim_mdhim_config_t *config = static_cast<hxhim_mdhim_config_t *>(opts->p->backend_config);
                hx->p->backend = new hxhim::backend::mdhim(hx, config->path);
            }
            break;
        case HXHIM_BACKEND_LEVELDB:
            {
                hxhim_leveldb_config_t *config = static_cast<hxhim_leveldb_config_t *>(opts->p->backend_config);
                hx->p->backend = new hxhim::backend::leveldb(hx, config->path, config->create_if_missing);
            }
            break;
        case HXHIM_BACKEND_IN_MEMORY:
            {
                hx->p->backend = new hxhim::backend::InMemory(hx);
            }
            break;
    }

    if (!hx->p->backend) {
        Close(hx);
        return HXHIM_ERROR;
    }

    MPI_Barrier(hx->mpi.comm);
    return HXHIM_SUCCESS;
}

/**
 * hxhimOpen
 * Start a HXHIM session
 *
 * @param hx             the HXHIM session
 * @param bootstrap_comm the MPI communicator used to boostrap MDHIM
 * @param filename       the name of the file to open (for now, it is only the mdhim configuration)
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimOpen(hxhim_t *hx, hxhim_options_t *opts) {
    return hxhim::Open(hx, opts);
}

/**
 * Close
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Close(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    MPI_Barrier(hx->mpi.comm);

    hx->p->running = false;
    hx->p->puts.start_processing.notify_all();
    hx->p->puts.done_processing.notify_all();

    if (hx->p->background_put_thread.joinable()) {
        hx->p->background_put_thread.join();
    }

    // clear out unflushed work in the work queue
    hxhim::clean(hx->p->puts.head);
    hxhim::clean(hx->p->gets.head);
    hxhim::clean(hx->p->getops.head);
    hxhim::clean(hx->p->deletes.head);

    {
        std::unique_lock<std::mutex>(hx->p->put_results_mutex);
        delete hx->p->put_results;
        hx->p->put_results = nullptr;
    }

    // clean up histogram;
    delete hx->p->histogram;
    hx->p->histogram = nullptr;

    // clean up backend
    if (hx->p->backend) {
        hx->p->backend->Close();
        delete hx->p->backend;
        hx->p->backend = nullptr;
    }

    // clean up pointer to private data
    delete hx->p;
    hx->p = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hxhimClose
 * Terminates a HXHIM session
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimClose(hxhim_t *hx) {
    return hxhim::Close(hx);
}

/**
 * Commit
 * Commits all flushed data to disk
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Commit(hxhim_t *hx) {
    if (!hx || !hx->p || !hx->p->backend) {
        return HXHIM_ERROR;
    }

    return hx->p->backend->Commit();
}

/**
 * hxhimCommit
 * Commits all flushed data to disk
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimCommit(hxhim_t *hx) {
    return hxhim::Commit(hx);
}

/**
 * StatFlush
 * Flushes the MDHIM statistics
 * Mainly needed for BGetOp
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::StatFlush(hxhim_t *hx) {
    if (!hx || !hx->p || !hx->p->backend) {
        return HXHIM_ERROR;
    }

    return hx->p->backend->StatFlush();
}

/**
 * hxhimStatFlush
 * Flushes the MDHIM statistics
 * Mainly needed for BGetOp
 *
 * @param hx the HXHIM session to terminate
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimStatFlush(hxhim_t *hx) {
    return hxhim::StatFlush(hx);
}

/**
 * FlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushPuts(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::PutData> &unsent = hx->p->puts;
    std::unique_lock<std::mutex> lock(unsent.mutex);
    unsent.force = true;
    unsent.start_processing.notify_all();

    // wait for flush to complete
    while (hx->p->running && unsent.force) {
        unsent.done_processing.wait(lock, [&](){ return !hx->p->running || !unsent.force; });
    }

    // retrieve all results
    std::unique_lock<std::mutex> results_lock(hx->p->put_results_mutex);
    hxhim::Results *res = hx->p->put_results;
    hx->p->put_results = new hxhim::Results();

    return res;
}

/**
 * hxhimFlushPuts
 * Flushes all queued PUTs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushPuts(hxhim_t *hx) {
    return hxhim_results_init(hxhim::FlushPuts(hx));
}

/**
 * encode_sp
 * Encodes the subjects and predicates in the given arrays
 * If the encoding results in an allocation, the pointer
 * is added into the ptrs list for later deallocation.
 *
 * @param subject_type     the types of the subjects
 * @param subjects         the array of subjects
 * @param subject_lens     the lengths of the subjects
 * @param predicate_type   the types of the predicate
 * @param predicates       the array of predicates
 * @param predicate_lens   the lengths of the predicates
 * @param count            the number of subjects/predicates
 * @param ptrs             the list of pointers to deallocate later
 */
static void encode_sp(hxhim_spo_type_t subject_type, void **subjects, std::size_t *subject_lens,
                      hxhim_spo_type_t predicate_type, void **predicates, std::size_t *predicate_lens,
                      std::size_t count,
                      std::list <void *> &ptrs) {
    bool copied = false;
    for(std::size_t i = 0; i < count; i++) {
        copied = false;
        encode(subject_type, subjects[i], subject_lens[i], copied);
        if (copied) {
            ptrs.push_back(subjects[i]);
        }

        copied = false;
        encode(predicate_type, predicates[i], predicate_lens[i], copied);
        if (copied) {
            ptrs.push_back(predicates[i]);
        }
    }
}

/**
 * FlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGets(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    hxhim::GetData *curr = gets.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Results *res = new hxhim::Results();
    std::list <void *> ptrs;

    // write complete batches
    while (curr->next) {
        // encode the batch
        encode_sp(hx->p->types.subject, curr->subjects, curr->subject_lens,
                  hx->p->types.predicate, curr->predicates, curr->predicate_lens,
                  HXHIM_MAX_BULK_GET_OPS, ptrs);

        // GET the batch
        hxhim::Results *ret = hx->p->backend->BGet(curr->subjects, curr->subject_lens, curr->predicates, curr->predicate_lens, HXHIM_MAX_BULK_GET_OPS);
        res->Append(ret);
        delete ret;

        // go to the next batch
        hxhim::GetData *next = curr->next;
        delete curr;
        curr = next;
    }

    // encode the batch
    encode_sp(hx->p->types.subject, curr->subjects, curr->subject_lens,
              hx->p->types.predicate, curr->predicates, curr->predicate_lens,
              gets.last_count, ptrs);

    // write final (possibly incomplete) batch
    hxhim::Results *ret = hx->p->backend->BGet(curr->subjects, curr->subject_lens, curr->predicates, curr->predicate_lens, gets.last_count);
    res->Append(ret);
    delete ret;

    // delete the last batch
    delete curr;

    for(void *ptr : ptrs) {
        ::operator delete(ptr);
    }

    gets.head = gets.tail = nullptr;

    return res;
}

/**
 * hxhimFlushGets
 * Flushes all queued GETs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGets(hxhim_t *hx) {
    return hxhim_results_init(hxhim::FlushGets(hx));
}

/**
 * FlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushGetOps(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    hxhim::GetOpData *curr = getops.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Results *res = new hxhim::Results();
    std::list <void *> ptrs;

    // write complete batches
    while (curr->next) {
        // encode the batch
        encode_sp(hx->p->types.subject, curr->subjects, curr->subject_lens,
                  hx->p->types.predicate, curr->predicates, curr->predicate_lens,
                  HXHIM_MAX_BULK_GET_OPS, ptrs);

        for(std::size_t i = 0; i < HXHIM_MAX_BULK_GET_OPS; i++) {
            // GETOP the key
            hxhim::Results *ret = hx->p->backend->BGetOp(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], curr->counts[i], curr->ops[i]);
            res->Append(ret);
            delete ret;
        }

        // go to the next batch
        hxhim::GetOpData *next = curr->next;
        delete curr;
        curr = next;
    }

    // encode the batch
    encode_sp(hx->p->types.subject, curr->subjects, curr->subject_lens,
              hx->p->types.predicate, curr->predicates, curr->predicate_lens,
              getops.last_count, ptrs);

    // write final (possibly incomplete) batch
    for(std::size_t i = 0; i < getops.last_count; i++) {
        hxhim::Results *ret = hx->p->backend->BGetOp(curr->subjects[i], curr->subject_lens[i], curr->predicates[i], curr->predicate_lens[i], curr->counts[i], curr->ops[i]);
        res->Append(ret);
        delete ret;
    }

    // delete the last batch
    delete curr;

    for(void *ptr : ptrs) {
        ::operator delete(ptr);
    }

    getops.head = getops.tail = nullptr;

    return res;
}

/**
 * hxhimFlushGetOps
 * Flushes all queued GETs with specific operations
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushGetOps(hxhim_t *hx) {
    return hxhim_results_init(hxhim::FlushGetOps(hx));
}

/**
 * FlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim::Results *hxhim::FlushDeletes(hxhim_t *hx) {
    if (!hx || !hx->p) {
        return nullptr;
    }

    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    hxhim::DeleteData *curr = dels.head;
    if (!curr) {
        return HXHIM_SUCCESS;
    }

    hxhim::Results *res = new hxhim::Results();
    std::list <void *> ptrs;

    // write complete batches
    while (curr->next) {
        // encode the batch
        encode_sp(hx->p->types.subject, curr->subjects, curr->subject_lens,
                  hx->p->types.predicate, curr->predicates, curr->predicate_lens,
                  HXHIM_MAX_BULK_DEL_OPS, ptrs);

        // DEL the batch
        hxhim::Results *ret = hx->p->backend->BDelete(curr->subjects, curr->subject_lens, curr->predicates, curr->predicate_lens, HXHIM_MAX_BULK_DEL_OPS);
        res->Append(ret);
        delete ret;

        // go to the next batch
        hxhim::DeleteData *next = curr->next;
        delete curr;
        curr = next;
    }

    // encode the batch
    encode_sp(hx->p->types.subject, curr->subjects, curr->subject_lens,
              hx->p->types.predicate, curr->predicates, curr->predicate_lens,
              dels.last_count, ptrs);

    // write final (possibly incomplete) batch
    hxhim::Results *ret = hx->p->backend->BDelete(curr->subjects, curr->subject_lens, curr->predicates, curr->predicate_lens, dels.last_count);
    res->Append(ret);
    delete ret;

    // delete the last batch
    delete curr;

    for(void *ptr : ptrs) {
        ::operator delete(ptr);
    }

    dels.head = dels.tail = nullptr;

    return res;
}

/**
 * hxhimFlushDeletes
 * Flushes all queued DELs
 * The internal queue is cleared, even on error
 *
 * @param hx the HXHIM session to terminate
 * @return Pointer to return value wrapper
 */
hxhim_results_t *hxhimFlushDeletes(hxhim_t *hx) {
    return hxhim_results_init(hxhim::FlushDeletes(hx));
}

/**
 * Flush
 *     1. Do all PUTs
 *     2. Do all GETs
 *     3. Do all GET_OPs
 *     4. Do all DELs
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim::Results *hxhim::Flush(hxhim_t *hx) {
    hxhim::Results *res    = new hxhim::Results();
    hxhim::Results *puts   = FlushPuts(hx);
    hxhim::Results *gets   = FlushGets(hx);
    hxhim::Results *getops = FlushGetOps(hx);
    hxhim::Results *dels   = FlushDeletes(hx);

    res->Append(puts);   delete puts;
    res->Append(gets);   delete gets;
    res->Append(getops); delete getops;
    res->Append(dels);   delete dels;

    return res;
}

/**
 * hxhimFlush
 * Push all queued work into MDHIM
 *
 * @param hx
 * @return An array of results (3 values)
 */
hxhim_results_t *hxhimFlush(hxhim_t *hx) {
    return hxhim_results_init(hxhim::Flush(hx));
}

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               void *object, std::size_t object_len) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::PutData> &puts = hx->p->puts;
    std::lock_guard<std::mutex> lock(puts.mutex);

    // no previous batch
    if (!puts.tail) {
        puts.head       = new hxhim::PutData();
        puts.tail       = puts.head;
        puts.last_count = 0;
    }

    // filled the current batch
    if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
        hxhim::PutData *next = new hxhim::PutData();
        next->prev      = puts.tail;
        puts.tail->next = next;
        puts.tail       = next;
        puts.last_count = 0;
        puts.full_batches++;
        puts.start_processing.notify_one();
    }

    std::size_t &i = puts.last_count;
    puts.tail->subjects[i] = subject;
    puts.tail->subject_lens[i] = subject_len;
    puts.tail->predicates[i] = predicate;
    puts.tail->predicate_lens[i] = predicate_len;
    puts.tail->objects[i] = object;
    puts.tail->object_lens[i] = object_len;
    i++;

    return HXHIM_SUCCESS;
}

/**
 * hxhimPut
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the object to put
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPut(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len,
             void *object, std::size_t object_len) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      object, object_len);
}

/**
 * Get
 * Add a GET into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to get
 * @param subject_len  the length of the subject to get
 * @param prediate     the prediate to get
 * @param prediate_len the length of the prediate to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Get(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
    std::lock_guard<std::mutex> lock(gets.mutex);

    // no previous batch
    if (!gets.tail) {
        gets.head       = new hxhim::GetData();
        gets.tail       = gets.head;
        gets.last_count = 0;
    }

    // filled the current batch
    if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
        gets.tail->next = new hxhim::GetData();
        gets.tail       = gets.tail->next;
        gets.last_count = 0;
        gets.full_batches++;
    }

    std::size_t &i = gets.last_count;
    gets.tail->subjects[i] = subject;
    gets.tail->subject_lens[i] = subject_len;
    gets.tail->predicates[i] = predicate;
    gets.tail->predicate_lens[i] = predicate_len;
    i++;
    return HXHIM_SUCCESS;
}

/**
 * hxhimGet
 * Add a GET into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to get
 * @param subject_len  the length of the subject to get
 * @param prediate     the prediate to get
 * @param prediate_len the length of the prediate to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGet(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len) {
    return hxhim::Get(hx,
                      subject, subject_len,
                      predicate, predicate_len);
}

/**
 * Delete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Delete(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len) {
    if (!hx || !hx->p) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
    std::lock_guard<std::mutex> lock(dels.mutex);

    // no previous batch
    if (!dels.tail) {
        dels.head       = new hxhim::DeleteData();
        dels.tail       = dels.head;
        dels.last_count = 0;
    }

    // filled the current batch
    if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
        dels.tail->next = new hxhim::DeleteData();
        dels.tail       = dels.tail->next;
        dels.last_count = 0;
        dels.full_batches++;
    }

    std::size_t &i = dels.last_count;
    dels.tail->subjects[i] = subject;
    dels.tail->subject_lens[i] = subject_len;
    dels.tail->predicates[i] = predicate;
    dels.tail->predicate_lens[i] = predicate_len;
    i++;

    return HXHIM_SUCCESS;
}

/**
 * hxhimDelete
 * Add a DELETE into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to delete
 * @param subject_len  the length of the subject to delete
 * @param prediate     the prediate to delete
 * @param prediate_len the length of the prediate to delete
 * @param object       the object associated with the key
 * @param object_len   the length of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimDelete(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len) {
    return hxhim::Delete(hx,
                         subject, subject_len,
                         predicate, predicate_len);
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                void **objects, std::size_t *object_lens,
                std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens ||
        !objects    || !object_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::PutData> &puts = hx->p->puts;
        std::lock_guard<std::mutex> lock(puts.mutex);

        // no previous batch
        if (!puts.tail) {
            puts.head       = new hxhim::PutData();
            puts.tail       = puts.head;
            puts.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (puts.last_count == HXHIM_MAX_BULK_PUT_OPS) {
                hxhim::PutData *next = new hxhim::PutData();
                next->prev      = puts.tail;
                puts.tail->next = next;
                puts.tail       = next;
                puts.last_count = 0;
                puts.full_batches++;
                puts.start_processing.notify_one();
            }

            std::size_t &i = puts.last_count;
            puts.tail->subjects[i] = subjects[c];
            puts.tail->subject_lens[i] = subject_lens[c];
            puts.tail->predicates[i] = predicates[c];
            puts.tail->predicate_lens[i] = predicate_lens[c];
            puts.tail->objects[i] = objects[c];
            puts.tail->object_lens[i] = object_lens[c];
            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the objects to put
 * @param object_lens   the lengths of the objects
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPut(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              void **objects, std::size_t *object_lens,
              std::size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       objects, object_lens,
                       count);
}

/**
 * BGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGet(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::GetData> &gets = hx->p->gets;
        std::lock_guard<std::mutex> lock(gets.mutex);

        // no previous batch
        if (!gets.tail) {
            gets.head       = new hxhim::GetData();
            gets.tail       = gets.head;
            gets.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (gets.last_count == HXHIM_MAX_BULK_GET_OPS) {
                gets.tail->next = new hxhim::GetData();
                gets.tail       = gets.tail->next;
                gets.last_count = 0;
                gets.full_batches++;
            }

            std::size_t &i = gets.last_count;
            gets.tail->subjects[i] = subjects[c];
            gets.tail->subject_lens[i] = subject_lens[c];
            gets.tail->predicates[i] = predicates[c];
            gets.tail->predicate_lens[i] = predicate_lens[c];
            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGet
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGet(hxhim_t *hx,
              void **subjects, std::size_t *subject_lens,
              void **predicates, std::size_t *predicate_lens,
              std::size_t count) {
    return hxhim::BGet(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       count);
}

/**
 * BGetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOp(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len,
                  std::size_t num_records, enum hxhim_get_op op) {
    if (!hx      || !hx->p       ||
        !subject || !subject_len) {
        return HXHIM_ERROR;
    }

    hxhim::Unsent<hxhim::GetOpData> &getops = hx->p->getops;
    std::lock_guard<std::mutex> lock(getops.mutex);

    // no previous batch
    if (!getops.tail) {
        getops.head       = new hxhim::GetOpData();
        getops.tail       = getops.head;
        getops.last_count = 0;
    }

    // filled the current batch
    if (getops.last_count == HXHIM_MAX_BULK_GET_OPS) {
        getops.tail->next = new hxhim::GetOpData();
        getops.tail       = getops.tail->next;
        getops.last_count = 0;
        getops.full_batches++;
    }

    std::size_t &i = getops.last_count;
    getops.tail->subjects[i] = subject;
    getops.tail->subject_lens[i] = subject_len;
    getops.tail->predicates[i] = predicate;
    getops.tail->predicate_lens[i] = predicate_len;
    getops.tail->counts[i] = num_records;
    getops.tail->ops[i] = op;
    i++;

    return HXHIM_SUCCESS;
}

/**
 * hxhimBGetOp
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param num_records   the number of key value pairs to get back
 * @param op            the operation to do
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOp(hxhim_t *hx,
                void *subject, std::size_t subject_len,
                void *predicate, std::size_t predicate_len,
                std::size_t num_records, enum hxhim_get_op op) {
    return hxhim::BGetOp(hx,
                         subject, subject_len,
                         predicate, predicate_len,
                         num_records, op);
}

/**
 * BDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BDelete(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens,
                   void **predicates, std::size_t *predicate_lens,
                   std::size_t count) {
    if (!hx         || !hx->p          ||
        !subjects   || !subject_lens   ||
        !predicates || !predicate_lens) {
        return HXHIM_ERROR;
    }

    if (count) {
        hxhim::Unsent<hxhim::DeleteData> &dels = hx->p->deletes;
        std::lock_guard<std::mutex> lock(dels.mutex);

        // no previous batch
        if (!dels.tail) {
            dels.head     = new hxhim::DeleteData();
            dels.tail     = dels.head;
            dels.last_count = 0;
        }

        for(std::size_t c = 0; c < count; c++) {
            // filled the current batch
            if (dels.last_count == HXHIM_MAX_BULK_DEL_OPS) {
                dels.tail->next = new hxhim::DeleteData();
                dels.tail       = dels.tail->next;
                dels.last_count = 0;
                dels.full_batches++;
            }

            std::size_t &i = dels.last_count;
            dels.tail->subjects[i] = subjects[c];
            dels.tail->subject_lens[i] = subject_lens[c];
            dels.tail->predicates[i] = predicates[c];
            dels.tail->predicate_lens[i] = predicate_lens[c];
            i++;
        }
    }

    return HXHIM_SUCCESS;
}

/**
 * hxhimBDelete
 * Add a BDEL into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to delete
 * @param subject_lens  the lengths of the subjects to delete
 * @param prediates     the prediates to delete
 * @param prediate_lens the lengths of the prediates to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBDelete(hxhim_t *hx,
                 void **subjects, std::size_t *subject_lens,
                 void **predicates, std::size_t *predicate_lens,
                 std::size_t count) {
    return hxhim::BDelete(hx,
                          subjects, subject_lens,
                          predicates, predicate_lens,
                          count);
}

/**
 * GetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param rank           the rank that is collecting the data
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetStats(hxhim_t *hx, const int rank,
                    const bool get_put_times, long double *put_times,
                    const bool get_num_puts, std::size_t *num_puts,
                    const bool get_get_times, long double *get_times,
                    const bool get_num_gets, std::size_t *num_gets) {
    return hx->p->backend->GetStats(rank,
                                    get_put_times, put_times,
                                    get_num_puts, num_puts,
                                    get_get_times, get_times,
                                    get_num_gets, num_gets);
}

/**
 * hxhimGetStats
 * Collective operation
 * Each desired pointer should be preallocated with space for md->size values
 *
 * @param hx             the HXHIM session
 * @param rank           the rank that is collecting the data
 * @param get_put_times  whether or not to get put_times
 * @param put_times      the array of put times from each rank
 * @param get_num_puts   whether or not to get num_puts
 * @param num_puts       the array of number of puts from each rank
 * @param get_get_times  whether or not to get get_times
 * @param get_times      the array of get times from each rank
 * @param get_num_gets   whether or not to get num_gets
 * @param num_gets       the array of number of gets from each rank
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetStats(hxhim_t *hx, const int rank,
                  const int get_put_times, long double *put_times,
                  const int get_num_puts, std::size_t *num_puts,
                  const int get_get_times, long double *get_times,
                  const int get_num_gets, std::size_t *num_gets) {
    return hxhim::GetStats(hx, rank,
                           get_put_times, put_times,
                           get_num_puts, num_puts,
                           get_get_times, get_times,
                           get_num_gets, num_gets);
}

/**
 * SubjectType
 *
 * @param hx   the HXHIM session
 * @param type the type of subjects used in this HXHIM session
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if the subject type was not copied into the type variable
 */
int hxhim::SubjectType(hxhim_t *hx, hxhim_spo_type_t *type) {
    if (!hx || !hx->p || !type) {
        return HXHIM_ERROR;
    }

    *type = hx->p->types.subject;
    return HXHIM_SUCCESS;
}

/**
 * hxhimSubjectType
 *
 * @param hx   the HXHIM session
 * @param type the type of subjects used in this HXHIM session
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if the subject type was not copied into the type variable
 */
int hxhimSubjectType(hxhim_t *hx, hxhim_spo_type_t *type) {
    return hxhim::SubjectType(hx, type);
}

/**
 * PredicateType
 *
 * @param hx   the HXHIM session
 * @param type the type of predicates used in this HXHIM session
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if the predicate type was not copied into the type variable
 */
int hxhim::PredicateType(hxhim_t *hx, hxhim_spo_type_t *type) {
    if (!hx || !hx->p || !type) {
        return HXHIM_ERROR;
    }

    *type = hx->p->types.predicate;
    return HXHIM_SUCCESS;
}

/**
 * hxhimPredicateType
 *
 * @param hx   the HXHIM session
 * @param type the type of predicates used in this HXHIM session
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if the predicate type was not copied into the type variable
 */
int hxhimPredicateType(hxhim_t *hx, hxhim_spo_type_t *type) {
    return hxhim::PredicateType(hx, type);
}

/**
 * ObjectType
 *
 * @param hx   the HXHIM session
 * @param type the type of objects used in this HXHIM session
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if the object type was not copied into the type variable
 */
int hxhim::ObjectType(hxhim_t *hx, hxhim_spo_type_t *type) {
    if (!hx || !hx->p || !type) {
        return HXHIM_ERROR;
    }

    *type = hx->p->types.object;
    return HXHIM_SUCCESS;
}

/**
 * hxhimObjectType
 *
 * @param hx   the HXHIM session
 * @param type the type of objects used in this HXHIM session
 * @return HXHIM_SUCCESS, or HXHIM_ERROR if the object type was not copied into the type variable
 */
int hxhimObjectType(hxhim_t *hx, hxhim_spo_type_t *type) {
    return hxhim::ObjectType(hx, type);
}

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the float to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               float *object) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      (void *) object, sizeof(float));
}

/**
 * hxhimPutFloat
 * Add a PUT that points to a float into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the float to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPutFloat(hxhim_t *hx,
                  void *subject, std::size_t subject_len,
                  void *predicate, std::size_t predicate_len,
                  void *object, std::size_t object_len) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      (void *) object, sizeof(float));
}

/**
 * Put
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the double to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::Put(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               double *object) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      (void *) object, sizeof(double));
}

/**
 * hxhimPutDouble
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the double to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPutDouble(hxhim_t *hx,
             void *subject, std::size_t subject_len,
             void *predicate, std::size_t predicate_len,
             double *object) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      (void *) object, sizeof(double));
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the floats to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                float **objects,
                std::size_t count) {
    std::size_t *lens = new std::size_t[count];
    memset((void *) lens, sizeof(float), sizeof(std::size_t) * count);
    const int ret = hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       (void **) objects, lens,
                       count);
    delete [] lens;
    return ret;
}

/**
 * hxhimBPutFloat
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the floats to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPutFloat(hxhim_t *hx,
                   void **subjects, std::size_t *subject_lens,
                   void **predicates, std::size_t *predicate_lens,
                   float **objects,
                   std::size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       objects,
                       count);
}

/**
 * BPut
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the floats to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPut(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                double **objects,
                std::size_t count) {
    std::size_t *lens = new std::size_t[count];
    memset((void *) lens, sizeof(float), sizeof(std::size_t) * count);
    const int ret = hxhim::BPut(hx,
                                subjects, subject_lens,
                                predicates, predicate_lens,
                                (void **) objects, lens,
                                count);
    delete [] lens;
    return ret;
}

/**
 * hxhimBPutDouble
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the doubles to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPutDouble(hxhim_t *hx,
                    void **subjects, std::size_t *subject_lens,
                    void **predicates, std::size_t *predicate_lens,
                    double **objects,
                    std::size_t count) {
    return hxhim::BPut(hx,
                       subjects, subject_lens,
                       predicates, predicate_lens,
                       objects,
                       count);
}

/**
 * BGetOp
 * This version of BGetOp gets data based on the prefix of the key
 *
 * @param hx            the HXHIM session
 * @param prefix        the prefix to search for; should be equivalent to a subject
 * @param prefix_len    the length of the prefix
 * @param num_records   the number of key value pairs to get back
 * @param op            the operation to do
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOp(hxhim_t *hx,
                  void *prefix, std::size_t prefix_len,
                  std::size_t num_records, enum hxhim_get_op op) {
    return hxhim::BGetOp(hx,
                         prefix, prefix_len,
                         nullptr, 0,
                         num_records, op);
}

/**
 * GetHistogram
 * Get access to the histogram
 *
 * @param hx            the HXHIM session
 * @param histogram     variable to place the histogram pointer into
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetHistogram(hxhim_t *hx, Histogram::Histogram **histogram) {
    if (!hx || !hx->p || !histogram) {
        return HXHIM_ERROR;
    }

    *histogram = hx->p->histogram;
    return HXHIM_SUCCESS;
}

/**
 * hxhimgetHistogram
 * C version of hxhim::GetHistogram
 * Converts a Histogram::Histogram pointer into a histogram_t
 *
 * @param hx            the HXHIM session
 * @param histogram     variable to place the histogram pointer into
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetHistogram(hxhim_t *hx, histogram_t *histogram) {
    return hxhim::GetHistogram(hx, &histogram->histogram);
}
