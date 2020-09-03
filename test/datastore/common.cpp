#include <cstring>

#include "common.hpp"

Triple::Triple(const char *sub,
               const char *pred,
               const hxhim_object_type_t type,
               const char *obj)
    : sub(sub, strlen(sub)),
      pred(pred, strlen(pred)),
      type(type),
      obj(obj, strlen(obj))
{}

std::size_t Triple::all_keys_size() const {
    return (sub.size() + pred.size())   + (sizeof(std::string::size_type) * 2)
           #if SOP
           + (sub.size() + obj.size())  + (sizeof(std::string::size_type) * 2)
           #endif

           #if PSO
           + (pred.size() + sub.size()) + (sizeof(std::string::size_type) * 2)
           #endif

           #if POS
           + (pred.size() + obj.size()) + (sizeof(std::string::size_type) * 2)
           #endif

           #if OSP
           + (obj.size() + sub.size())  + (sizeof(std::string::size_type) * 2)
           #endif

           #if OPS
           + (obj.size() + pred.size()) + (sizeof(std::string::size_type) * 2)
           #endif
           ;
}

Blob Triple::get_sub() const {
    return ReferenceBlob((void *) sub.c_str(), sub.size());
}

Blob Triple::get_pred() const {
    return ReferenceBlob((void *) pred.c_str(), pred.size());
}

hxhim_object_type_t Triple::get_type() const {
    return type;
}

Blob Triple::get_obj() const {
    return ReferenceBlob((void *) obj.c_str(), obj.size());
}


std::size_t all_keys_size() {
    std::size_t total = 0;

    for(Triple const &triple : triples) {
        total += triple.all_keys_size();
    }

    return total;
}
