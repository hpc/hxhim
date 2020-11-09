#ifndef TEST_DATASTORE_COMMON_HPP
#define TEST_DATASTORE_COMMON_HPP

#include <vector>

#include "datastore/datastore.hpp"
#include "utils/Blob.hpp"

class Triple {
    public:
        Triple(const char *sub,
               const char *pred,
               const hxhim_object_type_t type,
               const char *obj);

        std::size_t all_keys_size() const;

        Blob get_sub() const;
        Blob get_pred() const;
        hxhim_object_type_t get_type() const;
        Blob get_obj() const;

    private:
        std::string sub;
        std::string pred;
        hxhim_object_type_t type;
        std::string obj;
};

const std::vector <Triple> triples = {
    {"sub0", "pred0", hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE, "obj0"},
    {"sub1", "pred1", hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE, "obj1"},
};

const std::size_t count = triples.size();

std::size_t all_keys_size();

#endif
