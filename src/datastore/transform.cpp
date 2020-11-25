#include <cstring>

#include "datastore/transform.h"
#include "datastore/transform.hpp"
#include "utils/elen.hpp"
#include "utils/memory.hpp"

datastore::Transform::NumericExtra::NumericExtra()
    : neg(elen::NEG_SYMBOL),
      pos(elen::POS_SYMBOL),
      float_precision(elen::encode::FLOAT_PRECISION),
      double_precision(elen::encode::DOUBLE_PRECISION)
{}

datastore::Transform::Callbacks *datastore::Transform::default_callbacks() {
    Callbacks *callbacks = construct<Callbacks>();

    callbacks->encode.emplace(HXHIM_DATA_INT32,
                               std::make_pair(encode::integers<int32_t>,
                                              &callbacks->numeric_extra));
    callbacks->encode.emplace(HXHIM_DATA_INT64,
                               std::make_pair(encode::integers<int64_t>,
                                              &callbacks->numeric_extra));
    callbacks->encode.emplace(HXHIM_DATA_UINT32,
                               std::make_pair(encode::integers<uint32_t>,
                                              &callbacks->numeric_extra));
    callbacks->encode.emplace(HXHIM_DATA_UINT64,
                               std::make_pair(encode::integers<uint64_t>,
                                              &callbacks->numeric_extra));
    callbacks->encode.emplace(HXHIM_DATA_FLOAT,
                               std::make_pair(encode::floating_point<float>,
                                              &callbacks->numeric_extra));
    callbacks->encode.emplace(HXHIM_DATA_DOUBLE,
                               std::make_pair(encode::floating_point<double>,
                                              &callbacks->numeric_extra));
    callbacks->encode.emplace(HXHIM_DATA_BYTE,    std::make_pair(nullptr, nullptr));
    callbacks->encode.emplace(HXHIM_DATA_POINTER, std::make_pair(nullptr, nullptr));
    callbacks->encode.emplace(HXHIM_DATA_TRACKED, std::make_pair(nullptr, nullptr));

    callbacks->decode.emplace(HXHIM_DATA_INT32,
                               std::make_pair(decode::integers<int32_t>,
                                              &callbacks->numeric_extra));
    callbacks->decode.emplace(HXHIM_DATA_INT64,
                               std::make_pair(decode::integers<int64_t>,
                                              &callbacks->numeric_extra));
    callbacks->decode.emplace(HXHIM_DATA_UINT32,
                               std::make_pair(decode::integers<uint32_t>,
                                              &callbacks->numeric_extra));
    callbacks->decode.emplace(HXHIM_DATA_UINT64,
                               std::make_pair(decode::integers<uint64_t>,
                                              &callbacks->numeric_extra));
    callbacks->decode.emplace(HXHIM_DATA_FLOAT,
                               std::make_pair(decode::floating_point<float>,
                                              &callbacks->numeric_extra));
    callbacks->decode.emplace(HXHIM_DATA_DOUBLE,
                               std::make_pair(decode::floating_point<double>,
                                              &callbacks->numeric_extra));
    callbacks->decode.emplace(HXHIM_DATA_BYTE,    std::make_pair(nullptr, nullptr));
    callbacks->decode.emplace(HXHIM_DATA_POINTER, std::make_pair(nullptr, nullptr));
    callbacks->decode.emplace(HXHIM_DATA_TRACKED, std::make_pair(nullptr, nullptr));

    return callbacks;
}
