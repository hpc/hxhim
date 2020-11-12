#include <cstring>

#include "hxhim/constants.h"
#include "hxhim/double.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/single_type.hpp"

/**
 * PutDouble
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the double object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::PutDouble(hxhim_t *hx,
                     void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
                     void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
                     double *object) {
    return hxhim::Put(hx,
                      subject, subject_len, subject_type,
                      predicate, predicate_len, predicate_type,
                      object, sizeof(double), hxhim_data_t::HXHIM_DATA_DOUBLE);
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
 * @param object       the double object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPutDouble(hxhim_t *hx,
                   void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                   void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
                   double *object) {
    return hxhim::PutDouble(hx,
                            subject, subject_len, subject_type,
                            predicate, predicate_len, predicate_type,
                            object);
}

/**
 * GetDouble
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetDouble(hxhim_t *hx,
                     void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
                     void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type) {
    return hxhim::Get(hx,
                      subject, subject_len, subject_type,
                      predicate, predicate_len, predicate_type,
                      hxhim_data_t::HXHIM_DATA_DOUBLE);
}

/**
 * hxhimGetDouble
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetDouble(hxhim_t *hx,
                   void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                   void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type) {
    return hxhim::GetDouble(hx,
                            subject, subject_len, subject_type,
                            predicate, predicate_len, predicate_type);
}

/**
 * GetOp
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subjects to get
 * @param subject_len    the lengths of the subjects to get
 * @param predicate      the predicates to get
 * @param predicate_len  the lengths of the predicates to get
 * @param num_records    maximum number of records to GET
 * @param op             the operation to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetOpDouble(hxhim_t *hx,
                       void *subject, std::size_t subject_len, enum hxhim_data_t subject_type,
                       void *predicate, std::size_t predicate_len, enum hxhim_data_t predicate_type,
                       std::size_t num_records, enum hxhim_getop_t op) {
    return hxhim::GetOp(hx,
                        subject, subject_len, subject_type,
                        predicate, predicate_len, predicate_type,
                        hxhim_data_t::HXHIM_DATA_DOUBLE,
                        num_records, op);
}


/**
 * hxhimGetOpDouble
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subjects to get
 * @param subject_len    the lengths of the subjects to get
 * @param predicate      the predicates to get
 * @param predicate_len  the lengths of the predicates to get
 * @param num_records    maximum number of records to GET
 * @param op             the operation to use
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetOpDouble(hxhim_t *hx,
                     void *subject, size_t subject_len, enum hxhim_data_t subject_type,
                     void *predicate, size_t predicate_len, enum hxhim_data_t predicate_type,
                     size_t num_records, enum hxhim_getop_t op) {
    return hxhim::GetOpDouble(hx,
                              subject, subject_len, subject_type,
                              predicate, predicate_len, predicate_type,
                              num_records, op);
}

/**
 * BPutDouble
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the doubles to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPutDouble(hxhim_t *hx,
                      void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                      void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                      double **objects,
                      std::size_t count) {
    std::size_t *object_lens = alloc_array<std::size_t>(count, sizeof(double));

    const int rc = hxhim::BPutSingleType(hx,
                                         subjects, subject_lens, subject_types,
                                         predicates, predicate_lens, predicate_types,
                                         (void **) objects, object_lens, hxhim_data_t::HXHIM_DATA_DOUBLE,
                                         count);

    dealloc_array(object_lens, count);
    return rc;
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
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPutDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                    void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                    double **objects,
                    std::size_t count) {
    return hxhim::BPutDouble(hx,
                             subjects, subject_lens, subject_types,
                             predicates, predicate_lens, predicate_types,
                             objects,
                             count);
}

/**
 * hxhimBGetDouble
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param count         the number of ingets
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetDouble(hxhim_t *hx,
                      void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                      void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                      std::size_t count) {
    return hxhim::BGetSingleType(hx,
                                 subjects, subject_lens, subject_types,
                                 predicates, predicate_lens, predicate_types,
                                 hxhim_data_t::HXHIM_DATA_DOUBLE,
                                 count);
}

/**
 * hxhimBGetDouble
 * Add a BGET into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to get
 * @param subject_lens  the lengths of the subjects to get
 * @param prediates     the prediates to get
 * @param prediate_lens the lengths of the prediates to get
 * @param count         the number of ingets
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                    void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                    size_t count) {
    return hxhim::BGetDouble(hx,
                             subjects, subject_lens, subject_types,
                             predicates, predicate_lens, predicate_types,
                             count);
}

/**
 * BGetOpDouble
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param num_records    maximum numbers of records to GET
 * @param op             the operations to use
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOpDouble(hxhim_t *hx,
                        void **subjects, std::size_t *subject_lens, enum hxhim_data_t *subject_types,
                        void **predicates, std::size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                        std::size_t *num_records, enum hxhim_getop_t *ops,
                        const std::size_t count) {
    return hxhim::BGetOpSingleType(hx,
                                   subjects, subject_lens, subject_types,
                                   predicates, predicate_lens, predicate_types,
                                   hxhim_data_t::HXHIM_DATA_DOUBLE,
                                   num_records, ops,
                                   count);
}

/**
 * hxhimBGetOpDouble
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @param num_records    maximum numbers of records to GET
 * @param op             the operations to use
 * @param count          the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOpDouble(hxhim_t *hx,
                      void **subjects, size_t *subject_lens, enum hxhim_data_t *subject_types,
                      void **predicates, size_t *predicate_lens, enum hxhim_data_t *predicate_types,
                      size_t *num_records, enum hxhim_getop_t *ops,
                      const size_t count) {
    return hxhim::BGetOpDouble(hx,
                               subjects, subject_lens, subject_types,
                               predicates, predicate_lens, predicate_types,
                               num_records, ops,
                               count);
}
