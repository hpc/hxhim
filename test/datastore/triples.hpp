#ifndef TEST_DATASTORE_TRIPLES_HPP
#define TEST_DATASTORE_TRIPLES_HPP

#include <string>

#include "hxhim/Blob.hpp"

const std::string subjects[]   = {"sub0", "sub1"};
const std::string predicates[] = {"pred0", "pred1"};
const std::string objects[]    = {"obj0", "obj1"};

const std::size_t count = sizeof(objects) / sizeof(std::string);

#define BLOB(str) ReferenceBlob((void *) str.data(), str.size(), hxhim_data_t::HXHIM_DATA_BYTE)

#endif
