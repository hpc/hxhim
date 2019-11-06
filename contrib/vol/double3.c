#include "double3.h"

hid_t H5VL_hxhim_create_double3_type() {
    hid_t double3_id = H5Tcreate(H5T_COMPOUND, sizeof(struct double3_t));
    H5Tinsert(double3_id, "subject",   HOFFSET(struct double3_t, subject),   H5T_NATIVE_DOUBLE);
    H5Tinsert(double3_id, "predicate", HOFFSET(struct double3_t, predicate), H5T_NATIVE_DOUBLE);
    H5Tinsert(double3_id, "object",    HOFFSET(struct double3_t, object),    H5T_NATIVE_DOUBLE);

    return double3_id;
}
