#ifndef HXHIM_DATASTORE_HPP
#define HXHIM_DATASTORE_HPP

#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>

#include "datastore/constants.hpp"
#include "datastore/transform.hpp"
#include "message/Messages.hpp"
#include "utils/Blob.hpp"
#include "utils/Histogram.hpp"
#include "utils/Stats.hpp"

namespace Datastore {

/**
 * Base configuration type
 */
struct Config {
    Config(const Type type)
        : type(type)
    {}

    virtual ~Config() {}

    const Type type;
};

// mapping from predicate name to id
typedef std::set<std::string> HistNames_t;

/**
 * Datatstores
 * The base class for all datastores used by HXHIM
 */
class Datastore {
    public:
        // subject of the key used to store histograms
        static const std::string HISTOGRAM_SUBJECT;

        Datastore(const int rank,
                  const int id,
                  Transform::Callbacks *callbacks);     // Datastore takes ownership of callbacks
        virtual ~Datastore();

        // owner must explicitly call Open to open the underlying datastore
        bool Open(const std::string &new_name, const HistNames_t *histogram_names = nullptr);
        // destructor calls Close
        void Close(const bool write_histograms = false);

        bool Usable() const;

        // write histogram, close, open new datastore
        bool Change(const std::string &new_name,
                    const bool write_histograms = true,
                    const HistNames_t *find_histogram_names = nullptr);

        int ID() const;

        Message::Response::BPut       *operate(Message::Request::BPut       *req);
        Message::Response::BGet       *operate(Message::Request::BGet       *req);
        Message::Response::BGetOp     *operate(Message::Request::BGetOp     *req);
        Message::Response::BDelete    *operate(Message::Request::BDelete    *req);
        Message::Response::BHistogram *operate(Message::Request::BHistogram *req);

        typedef std::shared_ptr<::Histogram::Histogram> Histogram;
        typedef std::map<std::string, Histogram> Histograms;
        int WriteHistograms();
        std::size_t ReadHistograms(const HistNames_t &names);
        int AddHistogram(const std::string &name,
                         const ::Histogram::Config &config);     // overwrites existing histogram
        int AddHistogram(const std::string &name,
                         ::Histogram::Histogram *new_histogram); // overwrites existing histogram
        int GetHistogram(const std::string &name, Histogram *h) const;
        int GetHistogram(const std::string &name, ::Histogram::Histogram **h) const;
        int GetHistograms(const Histograms **histograms) const;
        int GetStats(uint64_t    *put_times,
                     std::size_t *num_puts,
                     uint64_t    *get_times,
                     std::size_t *num_gets);

        int Sync(const bool write_histograms);

    private:
        // child classes should implement these functions

        // only open;  no processing
        virtual bool OpenImpl(const std::string &new_name) = 0;

        // only close; no processing
        virtual void CloseImpl() = 0;

        // check whether or not the underyling datastore is valid
        virtual bool UsableImpl() const = 0;

        virtual Message::Response::BPut    *BPutImpl   (Message::Request::BPut    *req) = 0;
        virtual Message::Response::BGet    *BGetImpl   (Message::Request::BGet    *req) = 0;
        virtual Message::Response::BGetOp  *BGetOpImpl (Message::Request::BGetOp  *req) = 0;
        virtual Message::Response::BDelete *BDeleteImpl(Message::Request::BDelete *req) = 0;

        virtual int WriteHistogramsImpl() = 0;                                // store histograms in datastore
        virtual std::size_t ReadHistogramsImpl(const HistNames_t &names) = 0; // retrieve histograms from datastore
        virtual int SyncImpl() = 0;

    protected:
        static int encode(Transform::Callbacks *callbacks,
                          const Blob &src,
                          void **dst, std::size_t *dst_size);
        static int decode(Transform::Callbacks *callbacks,
                          const Blob &src,
                          void **dst, std::size_t *dst_size);

        // only used for objects
        static Blob append_type(void *ptr, std::size_t size, hxhim_data_t type);
        static hxhim_data_t remove_type(void *ptr, std::size_t &size);

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

        template <typename Key_t, typename Value_t>
        static void BGetOp_copy_response(Transform::Callbacks *callbacks,
                                         const Key_t &key,
                                         const Value_t &value,
                                         Message::Request::BGetOp *req,
                                         Message::Response::BGetOp *res,
                                         const std::size_t i,
                                         const std::size_t j,
                                         Datastore::Datastore::Stats::Event &event);

        void BGetOp_error_response(Message::Response::BGetOp *res,
                                   const std::size_t i,
                                   Blob &subject, Blob &predicate,
                                   Stats::Event &event);

};

}

#include "datastore/datastore.tpp"

#endif
