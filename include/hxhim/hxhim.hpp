#ifndef HXHIM_HPP
#define HXHIM_HPP

#include "hxhim/Results.hpp"
#include "hxhim/accessors.hpp"
#include "hxhim/config.hpp"
#include "hxhim/constants.h"
#include "hxhim/double.hpp"
#include "hxhim/float.hpp"
#include "hxhim/hxhim.h"
#include "hxhim/options.hpp"
#include "hxhim/single_type.hpp"
#include "hxhim/struct.h"

namespace hxhim {

/** @description Starts an HXHIM instance */
int Open(hxhim_t *hx, hxhim_options_t *opts);
int OpenOne(hxhim_t *hx, hxhim_options_t *opts, const std::string &db_path);

/** @description Stops an HXHIM instance */
int Close(hxhim_t *hx);

/** @description Functions for flushing HXHIM queues */
Results *FlushPuts(hxhim_t *hx);
Results *FlushGets(hxhim_t *hx);
Results *FlushGets(hxhim_t *hx);
Results *FlushGetOps(hxhim_t *hx);
Results *FlushDeletes(hxhim_t *hx);
Results *FlushHistograms(hxhim_t *hx);
Results *Flush(hxhim_t *hx);

/** @description Function that forces the datastores to flush to the underlying storage */
Results *Sync(hxhim_t *hx);

/** @description Function that opens new datastores */
Results *ChangeHash(hxhim_t *hx, const char *name, hxhim_hash_t func, void *args);

/** @description Functions for queuing operations to perform on the underlying storage */
/*  all buffers are user buffers */
int Put(hxhim_t *hx,
        void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
        void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
        void *object, std::size_t object_len, enum hxhim_data_t object_type,
        const hxhim_put_permutation_t permutations);

int Get(hxhim_t *hx,
        void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
        void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
        enum hxhim_data_t object_type);

int GetOp(hxhim_t *hx,
          void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
          void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
          enum hxhim_data_t object_type,
          std::size_t num_records, hxhim_getop_t op);

int Delete(hxhim_t *hx,
           void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
           void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type);

int BPut(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
         void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
         void **objects, std::size_t *object_lens, enum hxhim_data_t *object_types,
         const hxhim_put_permutation_t *permutations,
         const std::size_t count);

int BGet(hxhim_t *hx,
         void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
         void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
         enum hxhim_data_t *object_types,
         const std::size_t count);

int BGetOp(hxhim_t *hx,
           void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
           void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
           enum hxhim_data_t *object_types,
           std::size_t *num_records, hxhim_getop_t *ops,
           const std::size_t count);

int BDelete(hxhim_t *hx,
            void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
            void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
            const std::size_t count);

/** @description Utility Functions */
int GetStats(hxhim_t *hx, const int dst_rank,
             uint64_t    *put_times,
             std::size_t *num_puts,
             uint64_t    *get_times,
             std::size_t *num_gets);

int Histogram(hxhim_t *hx,
              int rs_id,
              const char *name, const std::size_t name_len);

int BHistogram(hxhim_t *hx,
               int *rs_ids,
               const char **names, const std::size_t *name_lens,
               const std::size_t count);

// int GetMinFilled(hxhim_t *hx, const int dst_rank,
//                  const bool get_bput, long double *bput,
//                  const bool get_bget, long double *bget,
//                  const bool get_bgetop, long double *bgetop,
//                  const bool get_bdel, long double *bdel);

// int GetAverageFilled(hxhim_t *hx, const int dst_rank,
//                      const bool get_bput, long double *bput,
//                      const bool get_bget, long double *bget,
//                      const bool get_bgetop, long double *bgetop,
//                      const bool get_bdel, long double *bdel);

// int GetMaxFilled(hxhim_t *hx, const int dst_rank,
//                  const bool get_bput, long double *bput,
//                  const bool get_bget, long double *bget,
//                  const bool get_bgetop, long double *bgetop,
//                  const bool get_bdel, long double *bdel);

}

#endif
