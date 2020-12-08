#include <sstream>
#include <sys/stat.h>

#include "datastore/datastores.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/mkdir_p.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

static std::string generate_name(const std::string &prefix, const std::string &basename, const int id, const std::string &postfix) {
    std::stringstream s;
    s << prefix << "/" << basename << "-" << id << postfix;
    return s.str();
}

int datastore::Init(hxhim_t *hx,
                    const int id,
                    Config *config,
                    Transform::Callbacks *callbacks,
                    const Histogram::Config &hist_config,
                    const std::string *exact_name,     // full path + name of datastore; ignores config
                    const bool do_open,                // opening of underlying implementation not guaranteed
                    const bool read_histograms,
                    const bool write_histograms) {
    if ((hx->p->bootstrap.rank < 0) || (id < 0)) {
        return DATASTORE_ERROR;
    }

    // clear out old datastore
    datastore::destroy(hx);

    hx->p->histograms.read  = read_histograms;
    hx->p->histograms.write = write_histograms;

    std::string name = "";

    datastore::Datastore *ds = nullptr;
    switch (config->type) {
        case IN_MEMORY:
            ds = new InMemory(hx->p->bootstrap.rank,
                              id, callbacks);
            hx->p->range_server.prefix = "";
            hx->p->range_server.postfix = "";

            mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore %d", id);
            break;

        #if HXHIM_HAVE_LEVELDB
        case LEVELDB:
            {
                leveldb::Config *leveldb_config = static_cast<leveldb::Config *>(config);
                hx->p->range_server.prefix = leveldb_config->prefix;
                hx->p->range_server.postfix = leveldb_config->postfix;

                if (exact_name) {
                    ds = new leveldb(hx->p->bootstrap.rank,
                                     id, callbacks,
                                     false);
                }
                else {
                    // generate the prefix path
                    mkdir_p(hx->p->range_server.prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

                    name = generate_name(hx->p->range_server.prefix,
                                         hx->p->hash.name, id,
                                         hx->p->range_server.postfix);
                    ds = new leveldb(hx->p->bootstrap.rank,
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
                rocksdb::Config *rocksdb_config = static_cast<rocksdb::Config *>(config);
                hx->p->range_server.prefix = rocksdb_config->prefix;
                hx->p->range_server.postfix = rocksdb_config->postfix;

                if (exact_name) {
                    ds = new rocksdb(hx->p->bootstrap.rank,
                                     id, callbacks,
                                     false);
                }
                else {
                    // generate the prefix path
                    mkdir_p(hx->p->range_server.prefix, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

                    name = generate_name(hx->p->range_server.prefix,
                                         hx->p->hash.name, id,
                                         hx->p->range_server.postfix);
                    ds = new rocksdb(hx->p->bootstrap.rank,
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

    // ds must exist
    if (!(hx->p->range_server.datastore = ds)) {
        return DATASTORE_ERROR;
    }

    // need to explicitly open the datastore
    if (do_open) {
        bool open_rc = false;

        if (exact_name) {
            open_rc = ds->Open(*exact_name);
        }
        else {
            open_rc = ds->Open(name);
        }

        if (!open_rc) {
            destruct(ds);
            hx->p->range_server.datastore = nullptr;
            return DATASTORE_ERROR;
        }

        // find histograms in the datastore and create them
        if (read_histograms) {
            ds->ReadHistograms(hx->p->histograms.names);
        }

        const datastore::Datastore::Histograms *existing = nullptr;
        ds->GetHistograms(&existing);

        // construct histograms that don't exist
        for(std::string const &name : hx->p->histograms.names) {
            if (!read_histograms                          ||   // overwrite existing histogram
                (existing->find(name) == existing->end())) {   // histogram doesnt exist
                ds->AddHistogram(name, hist_config);
            }
        }
    }

    return DATASTORE_SUCCESS;
}

int datastore::destroy(hxhim_t *hx) {
    datastore::Datastore *&ds = hx->p->range_server.datastore;
    if (ds) {
        ds->Close(hx->p->histograms.write);
        delete ds;
        ds = nullptr;
    }

    return DATASTORE_SUCCESS;
}
