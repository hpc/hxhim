#include "datastore/datastore.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/private/options.hpp"
#include "transport/transports.hpp"

/**
 * bootstrap
 * Invalidates the MPI bootstrapping information
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::bootstrap(hxhim_t *hx) {
    hx->p->bootstrap.comm = MPI_COMM_NULL;
    hx->p->bootstrap.rank = -1;
    hx->p->bootstrap.size = -1;

    return HXHIM_SUCCESS;
}

/**
 * running
 * Sets the state to not running
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::running(hxhim_t *hx) {
    hx->p->running = false;
    return HXHIM_SUCCESS;
}

template <typename T>
void destroy_queue(const std::vector<std::list<T *> > &queue) {
    for(std::list <T *> const &rs : queue) {
        for(T *request : rs) {
            destruct(request);
        }
    }
}

/**
 * queues
 * Cleans up the queues
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::queues(hxhim_t *hx) {
    {
        #if ASYNC_PUTS
        std::lock_guard<std::mutex> lock(hx->p->queues.puts.mutex);
        #endif
        destroy_queue(hx->p->queues.puts.queue);
    }
    destroy_queue(hx->p->queues.gets);
    destroy_queue(hx->p->queues.getops);
    destroy_queue(hx->p->queues.deletes);
    destroy_queue(hx->p->queues.histograms);
    return HXHIM_SUCCESS;
}

/**
 * datastore
 * Cleans up the datastore
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::datastore(hxhim_t *hx) {
    return (datastore::destroy(hx) == DATASTORE_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}

/**
 * async_put
 * Stops the background thread and cleans up the variables used by it
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::async_put(hxhim_t *hx) {
    #if ASYNC_PUTS
    hx->p->queues.puts.start_processing.notify_all();

    // wait for running background thread to stop
    std::unique_lock<std::mutex> lock(hx->p->async_put.mutex);
    hx->p->async_put.done.wait(lock, [&]() -> bool { return hx->p->async_put.done_check; });

    if (hx->p->async_put.thread.joinable()) {
        hx->p->async_put.thread.join();
    }
    #endif

    // release unproceesed results from asynchronous PUTs
    destruct(hx->p->async_put.results);
    hx->p->async_put.results = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * hash
 * Cleans up the hash function
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::hash(hxhim_t *hx) {
    hx->p->hash.func = nullptr;
    hx->p->hash.args = nullptr;

    return HXHIM_SUCCESS;
}

/**
 * transport
 * Cleans up the transport
 *
 * @param hx   the HXHIM instance
 * @return HXHIM_SUCCESS on success or HXHIM_ERROR
 */
int hxhim::destroy::transport(hxhim_t *hx) {
    return (Transport::destroy(hx) == TRANSPORT_SUCCESS)?HXHIM_SUCCESS:HXHIM_ERROR;
}
