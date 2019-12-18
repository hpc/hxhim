#include "hxhim/double.h"
#include "hxhim/double.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private.hpp"
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
                     void *subject, std::size_t subject_len,
                     void *predicate, std::size_t predicate_len,
                     double *object) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      HXHIM_DOUBLE_TYPE, (void *) object, sizeof(double));
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
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   double *object) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      HXHIM_DOUBLE_TYPE, (void *) object, sizeof(double));
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
 * @param object         address of where to place the double
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetDouble(hxhim_t *hx,
                     void *subject, size_t subject_len,
                     void *predicate, size_t predicate_len,
                     double *object) {
    static std::size_t double_len = sizeof(double);
    return hxhim::Get2(hx,
                       subject, subject_len,
                       predicate, predicate_len,
                       HXHIM_DOUBLE_TYPE,
                       object, &double_len);
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
 * @param object         address of where to place the double
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetDouble(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len,
                   double *object) {
    return hxhim::GetDouble(hx,
                            subject, subject_len,
                            predicate, predicate_len,
                            object);
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
                      void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      double **objects,
                      std::size_t count) {
    std::size_t *lens = new std::size_t[count];
    for(std::size_t i = 0; i < count; i++) {
        lens[i] = sizeof(double);
    }

    const int ret = hxhim::BPutSingleType(hx,
                                          subjects, subject_lens,
                                          predicates, predicate_lens,
                                          HXHIM_DOUBLE_TYPE, (void **) objects, lens,
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
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPutDouble(hxhim_t *hx,
                    void **subjects, std::size_t *subject_lens,
                    void **predicates, std::size_t *predicate_lens,
                    double **objects,
                    std::size_t count) {
    return hxhim::BPutDouble(hx,
                             subjects, subject_lens,
                             predicates, predicate_lens,
                             objects,
                             count);
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
int hxhim::BGetDouble(hxhim_t *hx,
                      void **subjects, size_t *subject_lens,
                      void **predicates, size_t *predicate_lens,
                      double **objects,
                      std::size_t count) {
    return hxhim::BGetSingleType(hx,
                                 subjects, subject_lens,
                                 predicates, predicate_lens,
                                 HXHIM_DOUBLE_TYPE, (void **) objects, sizeof(double),
                                 count);
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
int hxhimBGetDouble(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    double **objects,
                    size_t count) {
    return hxhim::BGetDouble(hx,
                             subjects, subject_lens,
                             predicates, predicate_lens,
                             objects,
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
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOpDouble(hxhim_t *hx,
                        void *subject, size_t subject_len,
                        void *predicate, size_t predicate_len,
                        size_t num_records, enum hxhim_get_op_t op) {
    return hxhim::BGetOp(hx,
                         subject, subject_len,
                         predicate, predicate_len,
                         HXHIM_DOUBLE_TYPE,
                         num_records, op);
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
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOpDouble(hxhim_t *hx,
                      void *subject, size_t subject_len,
                      void *predicate, size_t predicate_len,
                      size_t num_records, enum hxhim_get_op_t op) {
    return hxhim::BGetOpDouble(hx,
                               subject, subject_len,
                               predicate, predicate_len,
                               num_records, op);
}
