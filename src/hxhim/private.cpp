#include <cfloat>

#include "hxhim/backend/backends.hpp"
#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/options_private.hpp"
#include "hxhim/private.hpp"
#include "utils/elen.hpp"
#include "utils/reverse_bytes.h"

hxhim_private::hxhim_private()
    : backend(nullptr),
      puts(),
      gets(),
      getops(),
      deletes(),
      running(false),
      queued_bputs(0),
      background_put_thread(),
      put_results_mutex(),
      put_results(nullptr),
      histogram(nullptr)
{}

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
int hxhim::encode(const hxhim_spo_type_t type, void *&ptr, std::size_t &len, bool &copied) {
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
            {
                // should only do this if little endian is detected
                void *src = ptr;
                ptr = ::operator new(len);
                reverse_bytes(src, len, ptr);
                copied = true;
            }
            break;
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

        hxhim_spo_type_t object_type = head->object_types[i];
        void *object = head->objects[i];
        std::size_t object_len = head->object_lens[i];

        // add the value to the histogram
        double add_to_hist = 0;
        switch (object_type) {
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

        // encode the object
        // the subject and predicate are provided by the user
        bool copied = false;
        hxhim::encode(object_type, object, object_len, copied);
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
 * valid
 * Checks if hx and opts are ready to be used
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @param true if ready, else false
 */
static bool valid(hxhim_t *hx, hxhim_options_t *opts) {
    return hx && hx->p && opts && opts->p;
}

/**
 * types
 * Sets the types of the subjects, predicates, and objects.
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::types(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}

/**
 * backend
 * Sets up and starts the backend
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::backend(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
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

    return hx->p->backend?HXHIM_SUCCESS:HXHIM_ERROR;
}

int hxhim::init::one_backend(hxhim_t *hx, hxhim_options_t *opts, const std::string &name) {
    if (destroy::backend(hx) != HXHIM_SUCCESS) {
        return HXHIM_ERROR;
    }

    // Start the backend
    switch (opts->p->backend) {
        case HXHIM_BACKEND_LEVELDB:
            hx->p->backend = new hxhim::backend::leveldb(hx, name);
            break;
        default:
            break;
    }

    return hx->p->backend?HXHIM_SUCCESS:HXHIM_ERROR;

}

/**
 * background_thread
 * Starts up the background thread that does asynchronous PUTs.
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::background_thread(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

    hx->p->running = true;

    // Set up queued PUT results list
    if (!(hx->p->put_results = new hxhim::Results())) {
        return HXHIM_ERROR;
    }

    // Start the background thread
    hx->p->background_put_thread = std::thread(backgroundPUT, hx);

    return HXHIM_SUCCESS;
}

/**
 * histogram
 * Creates the histogram used to collect statistics
 *
 * @param hx   the HXHIM instance
 * @param opts the HXHIM options
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::init::histogram(hxhim_t *hx, hxhim_options_t *opts) {
    if (!valid(hx, opts)) {
        return HXHIM_ERROR;
    }

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

    return hx->p->histogram?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * valid
 * Checks if hx can be closed
 *
 * @param hx   the HXHIM instance
 * @param true if ready, else false
 */
static bool valid(hxhim_t *hx) {
    return hx && hx->p;
}

/**
 * types
 * Does nothing
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::types(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }
    return HXHIM_SUCCESS;
}

/**
 * backend
 * Cleans up the backend
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::backend(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    if (hx->p->backend) {
        hx->p->backend->Close();
        delete hx->p->backend;
        hx->p->backend = nullptr;
    }

    return HXHIM_SUCCESS;
}

/**
 * background_thread
 * Stops the background thread and cleans up the variables used by it
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::background_thread(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    // stop the thread
    hx->p->running = false;
    hx->p->puts.start_processing.notify_all();
    hx->p->puts.done_processing.notify_all();

    if (hx->p->background_put_thread.joinable()) {
        hx->p->background_put_thread.join();
    }

    // clear out unflushed work in the work queue
    hxhim::clean(hx->p->puts.head);
    hx->p->puts.head = nullptr;
    hxhim::clean(hx->p->gets.head);
    hx->p->gets.head = nullptr;
    hxhim::clean(hx->p->getops.head);
    hx->p->getops.head = nullptr;
    hxhim::clean(hx->p->deletes.head);
    hx->p->deletes.head = nullptr;

    {
        std::unique_lock<std::mutex>(hx->p->put_results_mutex);
        delete hx->p->put_results;
        hx->p->put_results = nullptr;
    }

    return HXHIM_SUCCESS;
}

/**
 * histogram
 * Destroys the histogram in hx
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::histogram(hxhim_t *hx) {
    if (!valid(hx)) {
        return HXHIM_ERROR;
    }

    background_thread(hx);

    delete hx->p->histogram;
    hx->p->histogram = nullptr;

    return HXHIM_SUCCESS;
}
