#ifndef HXHIM_HPP
#define HXHIM_HPP

#include "hxhim/Results.hpp"
#include "hxhim/accessors.hpp"
#include "hxhim/config.h"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/hash.h"
#include "hxhim/options.h"
#include "hxhim/options.hpp"
#include "hxhim/struct.h"
#include "utils/Histogram.hpp"

namespace hxhim {

/** @description Starts an HXHIM instance */
int Open(hxhim_t *hx, hxhim_options_t *opts);
int OpenOne(hxhim_t *hx, hxhim_options_t *opts, const std::string &db_path);

/** @description Stops an HXHIM instance */
int Close(hxhim_t *hx);

/** @description Functions for flushing HXHIM queues */
Results *FlushPuts(hxhim_t *hx);
Results *FlushGets(hxhim_t *hx);
Results *FlushGetOps(hxhim_t *hx);
Results *FlushDeletes(hxhim_t *hx);
Results *Flush(hxhim_t *hx);

/** @description Function that forces the datastores to flush to the underlying storage */
Results *Sync(hxhim_t *hx);

/** @description Function that opens new datastores */
Results *ChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args);

/** @description Functions for queuing operations to perform on the underlying storage */
int Put(hxhim_t *hx,
        void *subject, std::size_t subject_len,
        void *predicate, std::size_t predicate_len,
        hxhim_type_t object_type, void *object, std::size_t object_len);

int Get(hxhim_t *hx,
        void *subject, std::size_t subject_len,
        void *predicate, std::size_t predicate_len,
        hxhim_type_t object_type);

int Delete(hxhim_t *hx,
           void *subject, std::size_t subject_len,
           void *predicate, std::size_t predicate_len);

int BPut(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens,
         void **predicates, std::size_t *predicate_lens,
         hxhim_type_t *object_types, void **objects, std::size_t *object_lens,
         std::size_t count);

int BGet(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens,
         void **predicates, std::size_t *predicate_lens,
         hxhim_type_t *object_types,
         std::size_t count);

int BGetOp(hxhim_t *hx,
           void *subject, std::size_t subject_len,
           void *predicate, std::size_t predicate_len,
           hxhim_type_t object_type,
           std::size_t num_records, enum hxhim_get_op_t op);

int BDelete(hxhim_t *hx,
            void **subjects, std::size_t *subject_lens,
            void **predicates, std::size_t *predicate_lens,
            std::size_t count);

/** @description Utility Functions */
int GetStats(hxhim_t *hx, const int rank,
             const bool get_put_times, long double *put_times,
             const bool get_num_puts, std::size_t *num_puts,
             const bool get_get_times, long double *get_times,
             const bool get_num_gets, std::size_t *num_gets);

hxhim::Results *GetHistogram(hxhim_t *hx, const int datastore);
hxhim::Results *GetBHistogram(hxhim_t *hx, const int *datastores, const std::size_t count);

}

#include "hxhim/float.hpp"
#include "hxhim/double.hpp"
#include "hxhim/single_type.hpp"

#endif
