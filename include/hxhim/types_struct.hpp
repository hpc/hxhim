#ifndef HXHIM_SPO_TYPES_STRUCT_HPP
#define HXHIM_SPO_TYPES_STRUCT_HPP

#include "hxhim/constants.h"

typedef struct SPO_Types {
    SPO_Types(hxhim_spo_type_t subject_type = HXHIM_SPO_BYTE_TYPE,
              hxhim_spo_type_t predicate_type = HXHIM_SPO_BYTE_TYPE,
              hxhim_spo_type_t object_type = HXHIM_SPO_BYTE_TYPE)
        : subject(subject_type),
          predicate(predicate_type),
          object(object_type)
        {}

    hxhim_spo_type_t subject;
    hxhim_spo_type_t predicate;
    hxhim_spo_type_t object;
} SPO_Types_t;

#endif
