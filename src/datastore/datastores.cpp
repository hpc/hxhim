#include "datastore/datastores.hpp"
#include "hxhim/private/accessors.hpp"
#include "hxhim/private/hxhim.hpp"
#include "hxhim/RangeServer.hpp"
#include "utils/mlog2.h"
#include "utils/mlogfacs2.h"

int datastore::Init(hxhim_t *hx,
                    Config *config,
                    Transform::Callbacks *callbacks,
                    const Histogram::Config &hist_config,
                    const std::string *exact_name) {
    int rank = -1;
    int size = -1;
    hxhim::nocheck::GetMPI(hx, nullptr, &rank, &size);

    std::size_t client = 0;
    std::size_t server = 0;
    hxhim::nocheck::GetRangeServerClientToServerRatio(hx, &client, &server);

    const int id = hxhim::RangeServer::get_id(rank, size, client, server);
    if (id < 0) {
        return DATASTORE_ERROR;
    }

    switch (config->type) {
        case IN_MEMORY:
            hx->p->datastore = new InMemory(rank,
                                            id,
                                            callbacks,
                                            hx->p->hash.name);
            mlog(HXHIM_CLIENT_INFO, "Initialized In-Memory in datastore %d", id);
            break;

        #if HXHIM_HAVE_LEVELDB
        case LEVELDB:
            {
                leveldb::Config *leveldb_config = static_cast<leveldb::Config *>(config);
                if (exact_name) {
                    hx->p->datastore = new leveldb(rank,
                                                   callbacks,
                                                   *exact_name,
                                                   false);
                }
                else {
                    hx->p->datastore = new leveldb(rank,
                                                   id,
                                                   callbacks,
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
                                                   callbacks,
                                                   *exact_name,
                                                   false);
                }
                else {
                    hx->p->datastore = new rocksdb(rank,
                                                   id,
                                                   callbacks,
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

    if (hx->p->datastore) {
        for(std::string const &name : hx->p->hist_names) {
            hx->p->datastore->AddHistogram(name,
                                           std::shared_ptr<Histogram::Histogram>(construct<Histogram::Histogram>(hist_config, name), Histogram::deleter));
        }
    }

    return DATASTORE_SUCCESS;
}

int datastore::destroy(hxhim_t *hx) {
    datastore::Datastore *&ds = hx->p->datastore;
    if (ds) {
        ds->Close();
        delete ds;
        ds = nullptr;
    }

    return DATASTORE_SUCCESS;
}
