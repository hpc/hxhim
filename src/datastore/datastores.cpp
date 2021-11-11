#include <sstream>
#include <sys/stat.h>

#include "datastore/datastores.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/mkdir_p.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

std::string Datastore::generate_name(const std::string &prefix,
                                     const std::string &basename,
                                     const std::size_t id,
                                     const std::string &postfix,
                                     const char path_sep) {
    std::stringstream s;
    s << prefix << path_sep << basename << "-" << id << postfix;
    return s.str();
}

Datastore::Datastore *
Datastore::Init(hxhim_t *hx,
                const int id,
                Config *config,
                Transform::Callbacks *callbacks,
                const Histogram::Config &hist_config,
                const std::string *exact_name,     // full path + name of datastore; ignores config
                const bool do_open,                // opening of underlying implementation not guaranteed
                const bool read_histograms,
                const bool write_histograms) {
    if ((hx->p->bootstrap.rank < 0) || (id < 0)) {
        return nullptr;
    }

    hx->p->histograms.read  = read_histograms;
    hx->p->histograms.write = write_histograms;

    decltype(hx->p->range_server.datastores) *rs_ds = &hx->p->range_server.datastores;

    Datastore *ds = nullptr;
    switch (config->type) {
        case IN_MEMORY:
            ds = new InMemory(hx->p->bootstrap.rank,
                              id, callbacks);
            mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore %d", id);
            break;

        #if HXHIM_HAVE_LEVELDB
        case LEVELDB:
            {
                LevelDB::Config *leveldb_config = static_cast<LevelDB::Config *>(config);
                if (exact_name) {
                    ds = new LevelDB(hx->p->bootstrap.rank,
                                     id, callbacks,
                                     false);
                }
                else {
                    // generate the prefix path
                    mkdir_p(rs_ds->prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

                    ds = new LevelDB(hx->p->bootstrap.rank,
                                     id, callbacks,
                                     leveldb_config->create_if_missing);
                }
                mlog(HXHIM_CLIENT_INFO, "Initialized LevelDB in datastore %d", id);
            }
            break;
        #endif

        #if HXHIM_HAVE_ROCKSDB
        case ROCKSDB:
            {
                RocksDB::Config *rocksdb_config = static_cast<RocksDB::Config *>(config);
                if (exact_name) {
                    ds = new RocksDB(hx->p->bootstrap.rank,
                                     id, callbacks,
                                     false);
                }
                else {
                    // generate the prefix path
                    mkdir_p(rs_ds->prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

                    ds = new RocksDB(hx->p->bootstrap.rank,
                                     id, callbacks,
                                     rocksdb_config->create_if_missing);
                }
                mlog(HXHIM_CLIENT_INFO, "Initialized RocksDB in datastore %d", id);
            }
            break;
        #endif

        default:
            break;
    }

    if (!ds) {
        return nullptr;
    }

    // need to explicitly open the datastore
    if (do_open) {
        bool open_rc = false;

        if (exact_name) {
            open_rc = ds->Open(*exact_name);
        }
        else {
            const std::string name = generate_name(rs_ds->prefix,
                                                   rs_ds->basename,
                                                   id,
                                                   rs_ds->postfix);
            open_rc = ds->Open(name);
        }

        if (!open_rc) {
            destruct(ds);
            return nullptr;
        }

        // find histograms in the datastore and create them
        if (read_histograms) {
            ds->ReadHistograms(hx->p->histograms.names);
        }

        const Datastore::Datastore::Histograms *existing = nullptr;
        ds->GetHistograms(&existing);

        // construct histograms that don't exist
        for(std::string const &name : hx->p->histograms.names) {
            if (!read_histograms                          ||   // overwrite existing histogram
                (existing->find(name) == existing->end())) {   // histogram doesn't exist
                ds->AddHistogram(name, hist_config);
            }
        }
    }

    return ds;
}

int Datastore::destroy(hxhim_t *hx) {
    if (hx && hx->p) {
        for(Datastore *&ds : hx->p->range_server.datastores.ds) {
            if (ds) {
                ds->Close(hx->p->histograms.write);
                delete ds;
                ds = nullptr;
            }
        }

        destruct(hx->p->range_server.datastores.config);
        hx->p->range_server.datastores.config = nullptr;
    }

    return DATASTORE_SUCCESS;
}
