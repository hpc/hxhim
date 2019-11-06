#include "float3.h"

hid_t H5VL_hxhim_create_float3_type() {
    hid_t float3_id = H5Tcreate(H5T_COMPOUND, sizeof(struct float3_t));
    H5Tinsert(float3_id, "subject",   HOFFSET(struct float3_t, subject),   H5T_NATIVE_FLOAT);
    H5Tinsert(float3_id, "predicate", HOFFSET(struct float3_t, predicate), H5T_NATIVE_FLOAT);
    H5Tinsert(float3_id, "object",    HOFFSET(struct float3_t, object),    H5T_NATIVE_FLOAT);

    return float3_id;
}
