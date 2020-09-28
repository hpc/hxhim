#include "datastore/datastores.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

int hxhim::datastore::Init(hxhim_t *hx,
                           Config *config,
                           const Histogram::Config &hist_config,
                           const std::string *exact_name) {
    int rank = -1;
    nocheck::GetMPI(hx, nullptr, &rank, nullptr);

    const int id = get_id(hx, rank);

    Histogram::Histogram *hist = new Histogram::Histogram(hist_config);

    switch (config->type) {
        case IN_MEMORY:
            hx->p->datastore = new hxhim::datastore::InMemory(rank,
                                                              id,
                                                              hist,
                                                              hx->p->hash.name);
            mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore %d", id);
            break;

        #if HXHIM_HAVE_LEVELDB
        case LEVELDB:
            {
                leveldb::Config *leveldb_config = static_cast<leveldb::Config *>(config);
                if (exact_name) {
                    hx->p->datastore = new leveldb(rank,
                                                   hist,
                                                   *exact_name,
                                                   false);
                }
                else {
                    hx->p->datastore = new leveldb(rank,
                                                   id,
                                                   hist,
                                                   leveldb_config->prefix,
                                                   hx->p->hash.name,
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
                if (exact_name) {
                    hx->p->datastore = new rocksdb(rank,
                                                   hist,
                                                   *exact_name,
                                                   false);
                }
                else {
                    hx->p->datastore = new rocksdb(rank,
                                                   id,
                                                   hist,
                                                   rocksdb_config->prefix,
                                                   hx->p->hash.name,
                                                   rocksdb_config->create_if_missing);
                }
                mlog(HXHIM_CLIENT_INFO, "Initialized RocksDB in datastore %d", id);
            }
            break;
        #endif

        default:
            break;
    }

    return DATASTORE_SUCCESS;
}

int hxhim::datastore::destroy(hxhim_t *hx) {
    hxhim::datastore::Datastore *&ds = hx->p->datastore;
    if (ds) {
        ds->Close();
        delete ds;
        ds = nullptr;
    }

    return DATASTORE_SUCCESS;
}
