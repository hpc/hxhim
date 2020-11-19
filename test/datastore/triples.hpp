#ifndef TEST_DATASTORE_TRIPLES_HPP
#define TEST_DATASTORE_TRIPLES_HPP

#include <string>

#include "hxhim/Blob.hpp"

const std::string subjects[]   = {"sub0",  "sub1",  "sub2"};
const std::string predicates[] = {"pred0", "pred1", "pred2"};
const std::string objects[]    = {"obj0",  "obj1",  "obj2"};

const std::size_t count = (sizeof(objects) / sizeof(objects[0])) - 1;

Blob ReferenceBlob(const std::string &str, const hxhim_data_t type = hxhim_data_t::HXHIM_DATA_BYTE);

#endif
