#ifndef HXHIM_DATASTORE_HPP
#define HXHIM_DATASTORE_HPP

#include <list>

#include <mpi.h>
#include <mutex>

#include "datastore/constants.hpp"
#include "hxhim/struct.h"
#include "transport/Messages/Messages.hpp"
#include "utils/Histogram.hpp"
#include "utils/Stats.hpp"

namespace hxhim {
namespace datastore {

/**
 * Functions for converting between
 * datastore IDs and (rank, offset) pairs
 * get_id is the inverse of get_rank and get_offset
 *
 * Client : Server = 5 : 3
 * Datastores per range server: 4
 * MPI Rank:     |    0    |    1    |     2     |      3      |       4     |      5      |      6      |      7      |      8      |      9      |
 * Client:       |    0    |    1    |     2     |      3      |       4     |      5      |      6      |      7      |      8      |      9      |
 * Range Server: |    0    |    1    |     2     |             |             |      3      |      4      |      5      |             |             |
 * Datastore     | 0 1 2 3 | 4 5 6 7 | 8 9 10 11 |             |             | 12 13 14 15 | 16 17 18 19 | 20 21 22 23 |             |             |
 *
 * Client : Server = 3 : 5
 * Datastores per range server: 4
 * MPI Rank:     |    0    |    1    |     2     |      3      |       4     |      5      |      6      |      7      |      8      |      9      |
 * Client:       |    0    |    1    |     2     |             |             |      5      |      6      |      7      |             |             |
 * Range Server: |    0    |    1    |     2     |      3      |       4     |      5      |      6      |      7      |      8      |      9      |
 * Datastore     | 0 1 2 3 | 4 5 6 7 | 8 9 10 11 | 12 13 14 15 | 16 17 18 19 | 20 21 22 23 | 24 25 26 27 | 28 29 30 31 | 32 33 34 35 | 36 37 38 39 |
 */
int get_rank(hxhim_t *hx, const int id);
int get_server(hxhim_t *hx, const int id);
int get_offset(hxhim_t *hx, const int id);
int get_id(hxhim_t *hx, const int rank, const std::size_t offset);

/**
 * Base configuration type
 */
struct Config {
    Config(const hxhim::datastore::Type type)
        : type(type)
    {}

    virtual ~Config() {}

    const hxhim::datastore::Type type;
};

/**
 * Datatstores
 * The base class for all datastores used by HXHIM
 */
class Datastore {
    public:
        Datastore(const int rank,
                  const int id,
                  Histogram::Histogram *hist); // Datastore takes ownership of hist
        virtual ~Datastore();

        bool Open(const std::string &new_name);
        void Close();
        int ID() const;

        Transport::Response::BPut    *operate(Transport::Request::BPut    *req);
        Transport::Response::BGet    *operate(Transport::Request::BGet    *req);
        Transport::Response::BGetOp  *operate(Transport::Request::BGetOp  *req);
        Transport::Response::BDelete *operate(Transport::Request::BDelete *req);

        int GetHistogram(Histogram::Histogram **h) const;
        int GetStats(long double *put_times,
                     std::size_t *num_puts,
                     long double *get_times,
                     std::size_t *num_gets);

        int Sync();

    protected:
        virtual bool OpenImpl(const std::string &new_name) = 0;
        virtual void CloseImpl() = 0;

        virtual Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req) = 0;
        virtual Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req) = 0;
        virtual Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req) = 0;
        virtual Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req) = 0;

        virtual int SyncImpl() = 0;

        int encode(const hxhim_type_t type, void *&ptr, std::size_t &len, bool &copied);
        int decode(const hxhim_type_t type, void *src, const std::size_t &src_len, void **dst, std::size_t *dst_len);

        int rank;      // MPI rank of HXHIM instance
        int id;
        Histogram::Histogram *hist;

        mutable std::mutex mutex;

    public:
        struct Stats {
            struct Event {
                Event();

                struct Chronostamp time;
                std::size_t count;    // how many operations were performed during this event
                std::size_t size;     // how much data was involved
            };

            std::list<Event> puts;
            std::list<Event> gets;
            std::list<Event> getops;  // each individual op, not the entire packet
            std::list<Event> deletes;
        };

    protected:
        Stats stats;
};

}
}

#endif
