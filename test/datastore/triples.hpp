#ifndef TEST_DATASTORE_TRIPLES_HPP
#define TEST_DATASTORE_TRIPLES_HPP

#include <string>

#include "utils/Blob.hpp"

// use different sized strings
const std::string subjects[]   = {"sub0",     "sub1",    "sub2"};
const std::string predicates[] = {"pred0",    "pred1",   "pred2"};
const std::string objects[]    = {"object0",  "object1", "object2"};

// insert the first 2 SPOs, but query all 3
const std::size_t count = (sizeof(objects) / sizeof(objects[0])) - 1;

#endif
