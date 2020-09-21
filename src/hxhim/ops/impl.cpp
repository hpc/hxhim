#include "hxhim/hxhim.hpp"
#include "hxhim/private/cache.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/Blob.hpp"
#include "utils/memory.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

/**
 * PutImpl
 * Add a PUT into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param puts           the queue to place the PUT in
 * @param subject        the subject to put
 * @param predicate      the prediate to put
 * @param object_type    the type of the object
 * @param object         the object to put
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::PutImpl(hxhim::Unsent<hxhim::PutData> &puts,
                   Blob subject,
                   Blob predicate,
                   enum hxhim_object_type_t object_type,
                   Blob object) {
    mlog(HXHIM_CLIENT_INFO, "Foreground PUT Start (%p, %p, %p)", subject.data(), predicate.data(), object.data());

    hxhim::PutData *put = construct<hxhim::PutData>();
    put->subject = subject;
    put->predicate = predicate;
    put->object_type = object_type;
    put->object = object;

    mlog(HXHIM_CLIENT_DBG, "Foreground PUT Insert SPO into queue");
    puts.insert(put);

    // background thread checks watermark after every single PUT is queued up
    puts.start_processing.notify_one();

    mlog(HXHIM_CLIENT_DBG, "Foreground PUT Completed");
    return HXHIM_SUCCESS;
}

/**
 * GetImpl
 * Add a GET into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param gets           the queue to place the GET in
 * @param subject        the subject to put
 * @param predicate      the prediate to put
 * @param object_type    the type of the object
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetImpl(hxhim::Unsent<hxhim::GetData> &gets,
                   Blob subject,
                   Blob predicate,
                   enum hxhim_object_type_t object_type) {
    mlog(HXHIM_CLIENT_DBG, "GET Start");

    hxhim::GetData *get = construct<hxhim::GetData>();
    get->subject = subject;
    get->predicate = predicate;
    get->object_type = object_type;

    mlog(HXHIM_CLIENT_DBG, "GET Insert into queue");
    gets.insert(get);

    mlog(HXHIM_CLIENT_DBG, "GET Completed");
    return HXHIM_SUCCESS;
}

/**
 * GetOpImpl
 * Add a GETOP into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param getops         the queue to place the GETOP in
 * @param subject        the subject to put
 * @param predicate      the prediate to put
 * @param object_type    the type of the object
 * @param num_records    the number of records to get
 * @param op             the operation to run
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::GetOpImpl(hxhim::Unsent<hxhim::GetOpData> &getops,
                     Blob subject,
                     Blob predicate,
                     enum hxhim_object_type_t object_type,
                     std::size_t num_records, enum hxhim_getop_t op) {
    mlog(HXHIM_CLIENT_DBG, "GETOP Start");

    hxhim::GetOpData *getop = construct<hxhim::GetOpData>();
    getop->subject = subject;
    getop->predicate = predicate;
    getop->object_type = object_type;
    getop->num_recs = num_records;
    getop->op = op;

    mlog(HXHIM_CLIENT_DBG, "GETOP Insert into queue");
    getops.insert(getop);

    mlog(HXHIM_CLIENT_DBG, "GETOP Completed");
    return HXHIM_SUCCESS;
}

/**
 * DeleteImpl
 * Add a DELETE into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param dels         the queue to place the DELETE in
 * @param subject      the subject to delete
 * @param prediate     the prediate to delete
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::DeleteImpl(hxhim::Unsent<hxhim::DeleteData> &dels,
                      Blob subject,
                      Blob predicate) {
    mlog(HXHIM_CLIENT_DBG, "DELETE Start");

    hxhim::DeleteData *del = construct<hxhim::DeleteData>();
    del->subject = subject;
    del->predicate = predicate;

    mlog(HXHIM_CLIENT_DBG, "DELETE Insert into queue");
    dels.insert(del);

    mlog(HXHIM_CLIENT_DBG, "Delete Completed");
    return HXHIM_SUCCESS;
}

/**
 * HistogramImpl
 * Add a HISTOGRAM into the work queue
 * hx and hx->p are not checked because they must have been
 * valid for this function to be called.
 *
 * @param hx      the HXHIM instance
 * @param hists   the queue to place the HISTOGRAM in
 * @param ds_id   the datastore id - value checked by caller
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim::HistogramImpl(hxhim_t *hx,
                         hxhim::Unsent<hxhim::HistogramData> &hists,
                         const int ds_id) {
    mlog(HXHIM_CLIENT_DBG, "HISTOGRAM Start");

    hxhim::HistogramData *hist = construct<hxhim::HistogramData>();

    // setting ds_* values here allows for shuffle to skip hashing this request
    hist->ds_id     = ds_id;
    hist->ds_rank   = hxhim::datastore::get_rank(hx, ds_id);
    hist->ds_offset = hxhim::datastore::get_offset(hx, ds_id);

    mlog(HXHIM_CLIENT_DBG, "HISTOGRAM Insert into queue");
    hists.insert(hist);

    mlog(HXHIM_CLIENT_DBG, "Histogram Completed");
    return HXHIM_SUCCESS;
}
