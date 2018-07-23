#ifndef HXHIM_HPP
#define HXHIM_HPP

#include "Results.hpp"
#include "config.h"
#include "constants.h"
#include "options.h"
#include "struct.h"
#include "utils/Histogram.hpp"

namespace hxhim {

/** @description Starts an HXHIM instance */
int Open(hxhim_t *hx, hxhim_options_t *opts);

/** @description Stops an HXHIM instance */
int Close(hxhim_t *hx);

/** @description Commits all flushed data on local databases to disk */
int Commit(hxhim_t *hx);

/** @description Flushes the internal statistics */
int StatFlush(hxhim_t *hx);

/** @description Flush safe HXHIM queues */
Results *FlushPuts(hxhim_t *hx);
Results *FlushGets(hxhim_t *hx);
Results *FlushGetOps(hxhim_t *hx);
Results *FlushDeletes(hxhim_t *hx);
Results *Flush(hxhim_t *hx);

/** @description Functions for queuing operations to perform on the underlying storage */
int Put(hxhim_t *hx,
        void *subject, std::size_t subject_len,
        void *predicate, std::size_t predicate_len,
        void *object, std::size_t object_len);

int Get(hxhim_t *hx,
        void *subject, std::size_t subject_len,
        void *predicate, std::size_t predicate_len);

int Delete(hxhim_t *hx,
           void *subject, std::size_t subject_len,
           void *predicate, std::size_t predicate_len);

int BPut(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens,
         void **predicates, std::size_t *predicate_lens,
         void **objects, std::size_t *object_lens,
         std::size_t count);

int BGet(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens,
         void **predicates, std::size_t *predicate_lens,
         std::size_t count);

int BGetOp(hxhim_t *hx,
           void *subject, std::size_t subject_len,
           void *predicate, std::size_t predicate_len,
           std::size_t num_records, enum hxhim_get_op op);

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

int SubjectType(hxhim_t *hx, int *type);
int PredicateType(hxhim_t *hx, int *type);
int ObjectType(hxhim_t *hx, int *type);

int Put(hxhim_t *hx,
        void *subject, std::size_t subject_len,
        void *predicate, std::size_t predicate_len,
        float *object);

int Put(hxhim_t *hx,
        void *subject, std::size_t subject_len,
        void *predicate, std::size_t predicate_len,
        double *object);

int BPut(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens,
         void **predicates, std::size_t *predicate_lens,
         float **objects,
         std::size_t count);

int BPut(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens,
         void **predicates, std::size_t *predicate_lens,
         double **objects,
         std::size_t count);

int BGetOp(hxhim_t *hx,
           void *prefix, std::size_t prefix_len,
           std::size_t num_records, enum hxhim_get_op op);

int GetHistogram(hxhim_t *hx, Histogram::Histogram **histogram);

}

#endif
