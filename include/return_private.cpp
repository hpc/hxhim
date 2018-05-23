#include "return_private.hpp"

hxhim_return_t *hxhim_return_init(hxhim::Return *ret) {
    if (!ret) {
        return nullptr;
    }

    hxhim_return_t *r = new hxhim_return_t();
    if (!r) {
        return nullptr;
    }

    r->head = ret;
    r->curr = ret;
    return r;
}
