#ifndef HXHIM_HPP
#define HXHIM_HPP

#include <mpi.h>

#include "hxhim-types.h"
#include "return.hpp"
#include "transport_constants.h"

namespace hxhim {

/** @description Starts an HXHIM instance */
int Open(hxhim_t *hx, const MPI_Comm bootstrap_comm);

/** @description Stops an HXHIM instance */
int Close(hxhim_t *hx);

/** @description Commits all flushed data on local databases to disk */
int Commit(hxhim_t *hx);

/** @description Flushes the internal statistics */
int StatFlush(hxhim_t *hx);

/** @description Flush safe and unsafe HXHIM queues */
Return *FlushAllPuts(hxhim_t *hx);
Return *FlushAllGets(hxhim_t *hx);
Return *FlushAllGetOps(hxhim_t *hx);
Return *FlushAllDeletes(hxhim_t *hx);
Return *FlushAll(hxhim_t *hx);

/** @description Flush safe HXHIM queues */
Return *FlushPuts(hxhim_t *hx);
Return *FlushGets(hxhim_t *hx);
Return *FlushGetOps(hxhim_t *hx);
Return *FlushDeletes(hxhim_t *hx);
Return *Flush(hxhim_t *hx);

/** @description Standard functions for storing and retrieving records */
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
           std::size_t num_records, enum TransportGetMessageOp op);

int BDelete(hxhim_t *hx,
            void **subjects, std::size_t *subject_lens,
            void **predicates, std::size_t *predicate_lens,
            std::size_t count);

int GetStats(hxhim_t *hx, const int rank,
             const bool get_put_times, long double *put_times,
             const bool get_num_puts, std::size_t *num_puts,
             const bool get_get_times, long double *get_times,
             const bool get_num_gets, std::size_t *num_gets);

/** @description Analagous to their standard function counterparts,
    with the ability to send keys to user specified databases,
    rather than the database determined by the indexing */
namespace Unsafe {
    Return *FlushPuts(hxhim_t *hx);
    Return *FlushGets(hxhim_t *hx);
    Return *FlushGetOps(hxhim_t *hx);
    Return *FlushDeletes(hxhim_t *hx);
    Return *Flush(hxhim_t *hx);

    int Put(hxhim_t *hx,
            void *subject, std::size_t subject_len,
            void *predicate, std::size_t predicate_len,
            void *object, std::size_t object_len,
            const int database);

    int Get(hxhim_t *hx,
            void *subject, std::size_t subject_len,
            void *predicate, std::size_t predicate_len,
            const int database);

    int Delete(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               const int database);

    int BPut(hxhim_t *hx,
             void **subjects, std::size_t *subject_lens,
             void **predicates, std::size_t *predicate_lens,
             void **objects, std::size_t *object_lens,
             const int *databases, std::size_t count);

    int BGet(hxhim_t *hx,
             void **subjects, std::size_t *subject_lens,
             void **predicates, std::size_t *predicate_lens,
             const int *databases, std::size_t count);

    int BGetOp(hxhim_t *hx,
               void *subject, std::size_t subject_len,
               void *predicate, std::size_t predicate_len,
               std::size_t num_records, enum TransportGetMessageOp op,
               const int database);

    int BDelete(hxhim_t *hx,
                void **subjects, std::size_t *subject_lens,
                void **predicates, std::size_t *predicate_lens,
                const int *databases, std::size_t count);
}

}

#endif
