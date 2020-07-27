#include <cstring>

#include "hxhim/constants.h"
#include "hxhim/float.hpp"
#include "hxhim/hxhim.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/single_type.hpp"

/**
 * PutFloat
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the float object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::PutFloat(hxhim_t *hx,
                     void *subject, std::size_t subject_len,
                     void *predicate, std::size_t predicate_len,
                     float *object) {
    return hxhim::Put(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      hxhim_object_type_t::HXHIM_OBJECT_TYPE_FLOAT,
                      object, sizeof(float));
}

/**
 * hxhimPutFloat
 * Add a PUT into the work queue
 *
 * @param hx           the HXHIM session
 * @param subject      the subject to put
 * @param subject_len  the length of the subject to put
 * @param prediate     the prediate to put
 * @param prediate_len the length of the prediate to put
 * @param object       the float object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimPutFloat(hxhim_t *hx,
                   void *subject, std::size_t subject_len,
                   void *predicate, std::size_t predicate_len,
                   float *object) {
    return hxhim::PutFloat(hx,
                            subject, subject_len,
                            predicate, predicate_len,
                            object);
}

/**
 * GetFloat
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetFloat(hxhim_t *hx,
                     void *subject, size_t subject_len,
                     void *predicate, size_t predicate_len) {
    return hxhim::Get(hx,
                      subject, subject_len,
                      predicate, predicate_len,
                      hxhim_object_type_t::HXHIM_OBJECT_TYPE_FLOAT);
}

/**
 * hxhimGetFloat
 * Add a GET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subject        the subject to put
 * @param subject_len    the length of the subject to put
 * @param predicate      the prediate to put
 * @param predicate_len  the length of the prediate to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimGetFloat(hxhim_t *hx,
                   void *subject, size_t subject_len,
                   void *predicate, size_t predicate_len) {
    return hxhim::GetFloat(hx,
                            subject, subject_len,
                            predicate, predicate_len);
}

/**
 * BPutFloat
 * Add a BPUT into the work queue
 *
 * @param hx            the HXHIM session
 * @param subjects      the subjects to put
 * @param subject_lens  the lengths of the subjects to put
 * @param prediates     the prediates to put
 * @param prediate_lens the lengths of the prediates to put
 * @param objects       the floats to put
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BPutFloat(hxhim_t *hx,
                      void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      float **objects,
                      std::size_t count) {
    std::size_t *object_lens = alloc_array<std::size_t>(count, sizeof(float));

    const int rc = hxhim::BPutSingleType(hx,
                                         subjects, subject_lens,
                                         predicates, predicate_lens,
                                         hxhim_object_type_t::HXHIM_OBJECT_TYPE_FLOAT,
                                         (void **) objects, object_lens,
                                         count);

    dealloc_array(object_lens, count);
    return rc;
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
 * @param count         the number of inputs
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBPutFloat(hxhim_t *hx,
                    void **subjects, std::size_t *subject_lens,
                    void **predicates, std::size_t *predicate_lens,
                    float **objects,
                    std::size_t count) {
    return hxhim::BPutFloat(hx,
                             subjects, subject_lens,
                             predicates, predicate_lens,
                             objects,
                             count);
}

/**
 * hxhimBGetFloat
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
int hxhim::BGetFloat(hxhim_t *hx,
                      void **subjects, size_t *subject_lens,
                      void **predicates, size_t *predicate_lens,
                      std::size_t count) {
    return hxhim::BGetSingleType(hx,
                                 subjects, subject_lens,
                                 predicates, predicate_lens,
                                 hxhim_object_type_t::HXHIM_OBJECT_TYPE_FLOAT,
                                 count);
}

/**
 * hxhimBGetFloat
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
int hxhimBGetFloat(hxhim_t *hx,
                    void **subjects, size_t *subject_lens,
                    void **predicates, size_t *predicate_lens,
                    size_t count) {
    return hxhim::BGetFloat(hx,
                             subjects, subject_lens,
                             predicates, predicate_lens,
                             count);
}

/**
 * BGetOpFloat
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::BGetOpFloat(hxhim_t *hx,
                        void *subject, size_t subject_len,
                        void *predicate, size_t predicate_len,
                        size_t num_records, enum hxhim_get_op_t op) {
    return hxhim::BGetOp(hx,
                         subject, subject_len,
                         predicate, predicate_len,
                         hxhim_object_type_t::HXHIM_OBJECT_TYPE_FLOAT,
                         num_records, op);
}

/**
 * hxhimBGetOpFloat
 * Add a BGET into the work queue
 *
 * @param hx             the HXHIM session
 * @param subjects       the subjects to get
 * @param subject_lens   the lengths of the subjects to get
 * @param predicates     the predicates to get
 * @param predicate_lens the lengths of the predicates to get
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhimBGetOpFloat(hxhim_t *hx,
                      void *subject, size_t subject_len,
                      void *predicate, size_t predicate_len,
                      size_t num_records, enum hxhim_get_op_t op) {
    return hxhim::BGetOpFloat(hx,
                               subject, subject_len,
                               predicate, predicate_len,
                               num_records, op);
}
