#include <cstring>

#include "common.hpp"

Triple::Triple(const char *sub,
               const char *pred,
               const char *obj)
    : sub(sub, strlen(sub)),
      pred(pred, strlen(pred)),
      obj(obj, strlen(obj))
{}

std::size_t Triple::all_keys_size() const {
    return (sub.size() + pred.size())   + (sizeof(std::string::size_type) * 2) + sizeof(hxhim_data_t)

           #if SOP
           + (sub.size() + obj.size())  + (sizeof(std::string::size_type) * 2) + sizeof(hxhim_data_t)
           #endif

           #if PSO
           + (pred.size() + sub.size()) + (sizeof(std::string::size_type) * 2) + sizeof(hxhim_data_t)
           #endif

           #if POS
           + (pred.size() + obj.size()) + (sizeof(std::string::size_type) * 2) + sizeof(hxhim_data_t)
           #endif

           #if OSP
           + (obj.size() + sub.size())  + (sizeof(std::string::size_type) * 2) + sizeof(hxhim_data_t)
           #endif

           #if OPS
           + (obj.size() + pred.size()) + (sizeof(std::string::size_type) * 2) + sizeof(hxhim_data_t)
           #endif
           ;
}

Blob Triple::get_sub() const {
    return ReferenceBlob((void *) sub.c_str(), sub.size(), hxhim_data_t::HXHIM_DATA_BYTE);
}

Blob Triple::get_pred() const {
    return ReferenceBlob((void *) pred.c_str(), pred.size(), hxhim_data_t::HXHIM_DATA_BYTE);
}

Blob Triple::get_obj() const {
    return ReferenceBlob((void *) obj.c_str(), obj.size(), hxhim_data_t::HXHIM_DATA_BYTE);
}

std::size_t all_keys_size() {
    std::size_t total = 0;

    for(Triple const &triple : triples) {
        total += triple.all_keys_size();
    }

    return total;
}
