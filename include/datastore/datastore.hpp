#ifndef HXHIM_DATASTORE_HPP
#define HXHIM_DATASTORE_HPP

#include <mutex>

#include "datastore/constants.hpp"
#include "hxhim/hxhim.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Histogram.hpp"

namespace hxhim {
namespace datastore {

/**
 * Functions for converting between
 * datastore IDs and (rank, offset) pairs
 * get_id is the inverse of get_rank and get_offset
 *
 * Client : Server = 5 : 3
 * Datastores per range server: 4
 * MPI Rank:     |     0   |     1   |    2      | 3 | 4 |      5      |       6     |      7      | 8 | 9 |
 * Range Server: |     0   |     1   |    2      |   |   |      3      |       4     |      5      |   |   |
 * Datastore     | 0 1 2 3 | 4 5 6 7 | 8 9 10 11 |   |   | 12 13 14 15 | 16 17 18 19 | 20 21 22 23 |   |   |
 */
int get_rank(hxhim_t *hx, const int id);
int get_server(hxhim_t *hx, const int id);
int get_offset(hxhim_t *hx, const int id);
int get_id(hxhim_t *hx, const int rank, const std::size_t offset);

/**
 * base
 * The base class for all datastores used by HXHIM
 */
class Datastore {
    public:
        Datastore(hxhim_t *hx,
                  const int id,
                  Histogram::Histogram *hist); // Datastore takes ownership of hist
        virtual ~Datastore();

        bool Open(const std::string &new_name);
        void Close();

        int GetStats(const int dst_rank,
                     const bool get_put_times, long double *put_times,
                     const bool get_num_puts, std::size_t *num_puts,
                     const bool get_get_times, long double *get_times,
                     const bool get_num_gets, std::size_t *num_gets);

        // Transport::Response::Histogram *Histogram() const;

        Transport::Response::BPut    *operate(Transport::Request::BPut    *req);
        Transport::Response::BGet    *operate(Transport::Request::BGet    *req);
        Transport::Response::BGet2   *operate(Transport::Request::BGet2   *req);
        Transport::Response::BGetOp  *operate(Transport::Request::BGetOp  *req);
        Transport::Response::BDelete *operate(Transport::Request::BDelete *req);

        int Sync();

    protected:
        virtual bool OpenImpl(const std::string &new_name) = 0;
        virtual void CloseImpl() = 0;

        virtual Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req) = 0;
        virtual Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req) = 0;
        virtual Transport::Response::BGet2   *BGetImpl2  (Transport::Request::BGet2   *req) = 0;
        virtual Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req) = 0;
        virtual Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req) = 0;

        virtual int SyncImpl() = 0;

        int encode(const hxhim_type_t type, void *&ptr, std::size_t &len, bool &copied);
        int decode(const hxhim_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len);

        hxhim_t *hx;
        int id;
        Histogram::Histogram *hist;

        mutable std::mutex mutex;

        struct {
            std::size_t puts;
            long double put_times;
            std::size_t gets;
            long double get_times;
        } stats;
};

}
}

#endif
