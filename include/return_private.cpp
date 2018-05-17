#include "return_private.hpp"

hxhim_return_t *hxhim_return_init(hxhim::Return *ret) {
    if (!ret) {
        return nullptr;
    }

    hxhim_return_t *r = new hxhim_return_t();
    if (!r) {
        return nullptr;
    }

    r->ret = ret;
    return r;
}

hxhim_get_return_t *hxhim_get_return_init(hxhim::GetReturn *ret) {
    if (!ret) {
        return nullptr;
    }

    hxhim_get_return_t *r = new hxhim_get_return_t();
    if (!r) {
        return nullptr;
    }

    r->ret = ret;
    return r;
}
