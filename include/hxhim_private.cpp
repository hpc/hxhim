#include "hxhim_private.hpp"

namespace hxhim {

/**
 * get_matching_work
 * Checks th end of the work queue to see if
 * the requested operation matches the last
 * item. If they match, use the existing
 * item to cache the new key(s)/value(s).
 * If not, create a new item and return it.
 *
 * @param hx         the HXHIM session
 * @param keys       the keys to bget
 * @param key_lesn   the length of the keys to bget
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
work_t *get_matching_work(hxhim_t *hx, const hxhim_work_op op) {
    if (!hx || !hx->p || (op == hxhim_work_op::HXHIM_NOP)) {
        return nullptr;
    }

    // only one new work item creation at a time
    std::lock_guard<std::mutex> lock(hx->p->queue_mutex);

    // if there is no work, add an empty operation
    // or if the most recent operation doesn't match the given operation, add a new operation
    if (!hx->p->queue.size() || (hx->p->queue.back()->op != op)) {
        work_t *work = nullptr;
        switch (op) {
            case hxhim_work_op::HXHIM_PUT:
                work = new put_work_t();
                break;
            case hxhim_work_op::HXHIM_GET:
                work = new get_work_t();
                break;
            case hxhim_work_op::HXHIM_DEL:
                work = new del_work_t();
                break;
            case hxhim_work_op::HXHIM_NOP:
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

}
