#include "hxhim_private.hpp"

hxhim_work_t *get_matching_work(hxhim_session_t *hx, const hxhim_work_t::Op op) {
    if (!hx || !hx->p || (op == hxhim_work_t::Op::NONE)) {
        return nullptr;
    }

    // if there is no work, add an empty operation
    // or if the most recent operation doesn't match the given operation, add a new operation
    if (!hx->p->queue.size() || (hx->p->queue.back()->op != op)) {
        hxhim_work_t *work = nullptr;
        switch (op) {
            case hxhim_work_t::Op::PUT:
                work = new hxhim_put_work_t();
                break;
            case hxhim_work_t::Op::GET:
                work = new hxhim_get_work_t();
                break;
            case hxhim_work_t::Op::DEL:
                work = new hxhim_del_work_t();
                break;
            case hxhim_work_t::Op::NONE:
            default:
                break;
        }

        if (!work) {
            return nullptr;
        }

        hx->p->queue.push_back(work);
    }

    // return the last operation
    return hx->p->queue.back();
}
