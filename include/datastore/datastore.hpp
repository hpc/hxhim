#ifndef HXHIM_DATASTORE_HPP
#define HXHIM_DATASTORE_HPP

#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>

#include "datastore/constants.hpp"
#include "datastore/transform.hpp"
#include "hxhim/Blob.hpp"
#include "transport/Messages/Messages.hpp"
#include "utils/Histogram.hpp"
#include "utils/Stats.hpp"

namespace datastore {

/**
 * Base configuration type
 */
struct Config {
    Config(const datastore::Type type)
        : type(type)
    {}

    virtual ~Config() {}

    const datastore::Type type;
};

// mapping from predicate name to id
typedef std::set<std::string> HistNames_t;

/**
 * Datatstores
 * The base class for all datastores used by HXHIM
 */
class Datastore {
    public:
        Datastore(const int rank,
                  const int id,
                  Transform::Callbacks *callbacks);   // Datastore takes ownership of callbacks

        virtual ~Datastore();

        bool Open(const std::string &new_name);
        void Close();
        int ID() const;

        Transport::Response::BPut       *operate(Transport::Request::BPut       *req);
        Transport::Response::BGet       *operate(Transport::Request::BGet       *req);
        Transport::Response::BGetOp     *operate(Transport::Request::BGetOp     *req);
        Transport::Response::BDelete    *operate(Transport::Request::BDelete    *req);
        Transport::Response::BHistogram *operate(Transport::Request::BHistogram *req);

        typedef std::shared_ptr<::Histogram::Histogram> Histogram;
        typedef std::map<std::string, Histogram> Histograms;
        int AddHistogram(const std::string &name, Histogram new_histogram);
        int GetHistogram(const std::string &name, Histogram *h) const;
        int GetHistogram(const std::string &name, ::Histogram::Histogram **h) const;
        int GetHistograms(const Histograms **histograms) const;
        int GetStats(uint64_t    *put_times,
                     std::size_t *num_puts,
                     uint64_t    *get_times,
                     std::size_t *num_gets);

        int Sync();

    protected:
        // child classes should implement these functions
        virtual bool OpenImpl(const std::string &new_name) = 0;
        virtual void CloseImpl() = 0;

        virtual Transport::Response::BPut    *BPutImpl   (Transport::Request::BPut    *req) = 0;
        virtual Transport::Response::BGet    *BGetImpl   (Transport::Request::BGet    *req) = 0;
        virtual Transport::Response::BGetOp  *BGetOpImpl (Transport::Request::BGetOp  *req) = 0;
        virtual Transport::Response::BDelete *BDeleteImpl(Transport::Request::BDelete *req) = 0;

        virtual int SyncImpl() = 0;

        static int encode(Transform::Callbacks *callbacks,
                   const Blob &src,
                   void **dst, std::size_t *dst_size);
        static int decode(Transform::Callbacks *callbacks,
                   const Blob &src,
                   void **dst, std::size_t *dst_size);

    protected:
        int rank;      // MPI rank of HXHIM instance
        int id;
        Transform::Callbacks *callbacks;
        Histograms hists;
        mutable std::mutex mutex;

    public:
        // child classes should update stats, since events might
        // not be the same between different implementations
        struct Stats {
            struct Event {
                Event();

                ::Stats::Chronostamp time;
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

    public:
        template <typename Key_t, typename Value_t>
        static void BGetOp_copy_response(Transform::Callbacks *callbacks,
                                         const Key_t &key,
                                         const Value_t &value,
                                         Transport::Request::BGetOp *req,
                                         Transport::Response::BGetOp *res,
                                         const std::size_t i,
                                         const std::size_t j,
                                         datastore::Datastore::Stats::Event &event);

        void BGetOp_error_response(Transport::Response::BGetOp *res,
                                   const std::size_t i,
                                   Blob &subject, Blob &predicate,
                                   Stats::Event &event);

};

}

#include "datastore/datastore.tpp"

#endif
