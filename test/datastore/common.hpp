#ifndef TEST_DATASTORE_COMMON_HPP
#define TEST_DATASTORE_COMMON_HPP

#include <vector>

#include "datastore/datastore.hpp"
#include "hxhim/Blob.hpp"

class Triple {
    public:
        Triple(const char *sub,
               const char *pred,
               const char *obj);

        std::size_t all_keys_size() const;

        Blob get_sub() const;
        Blob get_pred() const;
        Blob get_obj() const;

    private:
        std::string sub;
        std::string pred;
        std::string obj;
};

const std::vector <Triple> triples = {
    {"sub0", "pred0", "obj0"},
    {"sub1", "pred1", "obj1"},
};

const std::size_t count = triples.size();

std::size_t all_keys_size();

#endif
