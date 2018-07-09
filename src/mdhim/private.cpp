#include <cstdlib>
#include <map>
#include <unistd.h>
#include <sys/types.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "mdhim/indexes.h"
#include "mdhim/local_client.h"
#include "mdhim/mdhim_options_private.h"
#include "mdhim/partitioner.h"
#include "mdhim/private.h"
#include "transport/MPI.hpp"
#include "transport/Thallium.hpp"

/**
 * mdhim_private_init_db
 *
 * @param md        the mdhim context to initialize
 * @param db        the database options to initialize with
 * @param MDHIM_SUCCESS or MDHIM_ERROR
 */
static int mdhim_private_init_db(mdhim_t *md, mdhim_db_options_t *db) {
    if (!md || !md->p || !db) {
        return MDHIM_ERROR;
    }

    if (db->type == LEVELDB) {
        return MDHIM_SUCCESS;
    }

    mlog(MDHIM_CLIENT_CRIT, "Invalid data store type specified");
    return MDHIM_DB_ERROR;
}

/**
 * mdhim_private_init_transport_mpi
 *
 * @param md        the mdhim context to initialize
 * @param opts      the data needed to set up MPI as the underlying transport
 * @param MDHIM_SUCCESS or MDHIM_ERROR
 */
static int mdhim_private_init_transport_mpi(mdhim_t *md, MPIOptions *opts, const std::set<int> &endpointgroup) {
    if (!md || !md->p || !md->p->transport || !opts) {
        return MDHIM_ERROR;
    }

    // Do not allow MPI_COMM_NULL
    if (opts->comm_ == MPI_COMM_NULL) {
        return MDHIM_ERROR;
    }

    // Get the memory pool used for storing messages
    FixedBufferPool *fbp = Memory::Pool(opts->alloc_size_, opts->regions_);
    if (!fbp) {
        return MDHIM_ERROR;
    }

    // give the range server access to the memory buffer
    MPIRangeServer::init(md, fbp, opts->listeners_);

    MPIEndpointGroup *eg = new MPIEndpointGroup(opts->comm_, md->lock, fbp);
    if (!eg) {
        return MDHIM_ERROR;
    }

    // create mapping between unique IDs and ranks
    for(int i = 0; i < md->size; i++) {
        // MPI ranks map 1:1 with the boostrap MPI rank
        md->p->transport->AddEndpoint(i, new MPIEndpoint(opts->comm_, i, fbp, md->p->shutdown));

        // if the rank was specified as part of the endpoint group, add the rank to the endpoint group
        if (endpointgroup.find(i) != endpointgroup.end()) {
            eg->AddID(i, i);
        }
    }

    // remove loopback endpoint
    md->p->transport->RemoveEndpoint(md->rank);

    md->p->transport->SetEndpointGroup(eg);
    md->p->send_client_response = MPIRangeServer::send_client_response;
    md->p->range_server_destroy = MPIRangeServer::destroy;

    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_init_transport_thallium
 *
 * @param md        the mdhim context to initialize
 * @param opts      the data needed to set up thallium as the underlying transport
 * @param MDHIM_SUCCESS or MDHIM_ERROR
 */
static int mdhim_private_init_transport_thallium(mdhim_t *md, ThalliumOptions *opts, const std::set<int> &endpointgroup) {
    if (!md || !md->p || !md->p->transport || !opts) {
        return MDHIM_ERROR;
    }

    // create the engine (only 1 instance per process)
    Thallium::Engine_t engine(new thallium::engine(opts->protocol_, THALLIUM_SERVER_MODE, true, -1),
                              [=](thallium::engine *engine) {
                                  engine->finalize();
                                  delete engine;
                              });

    // create client to range server RPC
    Thallium::RPC_t rpc(new thallium::remote_procedure(engine->define(ThalliumRangeServer::CLIENT_TO_RANGE_SERVER_NAME,
                                                                      ThalliumRangeServer::receive_rangesrv_work)));

    // give the range server access to the mdhim_t data
    ThalliumRangeServer::init(md->p);

    // wait for every engine to start up
    MPI_Barrier(md->comm);

    // get a mapping of unique IDs to thallium addresses
    std::map<int, std::string> addrs;
    if (Thallium::get_addrs(md->comm, engine, addrs) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    // remove the loopback endpoint
    addrs.erase(md->rank);

    ThalliumEndpointGroup *eg = new ThalliumEndpointGroup(rpc);
    if (!eg) {
        return MDHIM_ERROR;
    }

    // create mapping between unique IDs and ranks
    for(std::pair<const int, std::string> const &addr : addrs) {
        Thallium::Endpoint_t server(new thallium::endpoint(engine->lookup(addr.second)));

        // add the remote thallium endpoint to the tranport
        ThalliumEndpoint* ep = new ThalliumEndpoint(engine, rpc, server);
        md->p->transport->AddEndpoint(addr.first, ep);

        // if the rank was specified as part of the endpoint group, add the thallium endpoint to the endpoint group
        if (endpointgroup.find(addr.first) != endpointgroup.end()) {
            eg->AddID(addr.first, server);
        }
    }

    md->p->transport->SetEndpointGroup(eg);
    md->p->send_client_response = ThalliumRangeServer::send_client_response;
    md->p->range_server_destroy = ThalliumRangeServer::destroy;

    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_init_transport
 *
 * @param md        the mdhim context to initialize
 * @param transport the transport options to initialize with
 * @param MDHIM_SUCCESS or MDHIM_ERROR
 */
static int mdhim_private_init_transport(mdhim_t *md, mdhim_transport_options_t *transport) {
    if (!md || !md->p || !transport) {
        return MDHIM_ERROR;
    }

    if (!(md->p->transport = new Transport())) {
        return MDHIM_ERROR;
    }

    int ret = MDHIM_ERROR;
    if (transport->transport_specific->type_ == MDHIM_TRANSPORT_MPI) {
        ret = mdhim_private_init_transport_mpi(md, dynamic_cast<MPIOptions *>(transport->transport_specific), transport->endpointgroup);
    }
    else if (transport->transport_specific->type_ == MDHIM_TRANSPORT_THALLIUM) {
        ret = mdhim_private_init_transport_thallium(md, dynamic_cast<ThalliumOptions *>(transport->transport_specific), transport->endpointgroup);
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid transport type specified");
    }

    if (ret == MDHIM_ERROR) {
        delete md->p->transport;
        md->p->transport = nullptr;
    }

    return ret;
}

/**
 * mdhim_private_init_index
 * Initializes index values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int mdhim_private_init_index(mdhim_t *md) {
    if (!md || !md->p || !md->p->db_opts){
        return MDHIM_ERROR;
    }

    //Initialize the indexes and create the primary index
    md->p->indexes = nullptr;
    md->p->indexes_by_name = nullptr;
    md->p->indexes_lock = PTHREAD_RWLOCK_INITIALIZER;

    //Create the default remote primary index
    md->p->primary_index = create_global_index(md, md->p->db_opts->rserver_factor, md->p->db_opts->max_recs_per_slice,
                                               md->p->db_opts->type, md->p->db_opts->key_type, nullptr);

    if (!md->p->primary_index) {
        return MDHIM_ERROR;
    }

    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_init
 * A simple wrapper function to call other functions to initialize mdhim_t variables
 *
 * @param md        the mdhim context to initialize
 * @param db        the database options to initialize with
 * @param transport the transrpot options to initialize with
 * @param MDHIM_SUCCESS or MDHIM_ERROR
 */
int mdhim_private_init(mdhim_t* md, mdhim_db_options_t *db, mdhim_transport_options_t *transport) {
    if (!md || !db || !transport) {
        return MDHIM_ERROR;
    }

    if (!(md->p = new mdhim_private_t())) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Private Initialization Failed");
        return MDHIM_ERROR;
    }

    //Flag that won't be used until shutdown
    md->p->shutdown = 0;

    //Required for index initialization
    md->p->db_opts = db;
    md->p->mdhim_rs = nullptr;

    //Initialize index members of context
    if (mdhim_private_init_index(md) != MDHIM_SUCCESS){
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Index Initialization Failed");
        return MDHIM_ERROR;
    }

    // initialize the database
    if (mdhim_private_init_db(md, db) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Database Initialization Failed");
        return MDHIM_ERROR;
    }

    // initialize the transport
    if (mdhim_private_init_transport(md, transport) != MDHIM_SUCCESS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM - Error Transport Initialization Failed");
        return MDHIM_ERROR;
    }

    md->p->receive_msg_mutex = PTHREAD_MUTEX_INITIALIZER;
    md->p->receive_msg_ready_cv = PTHREAD_COND_INITIALIZER;
    md->p->receive_msg = nullptr;

    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_destroy_index
 * Cleans up index values in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int mdhim_private_destroy_index(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    if (pthread_rwlock_destroy(&md->p->indexes_lock) != 0) {
        return MDHIM_ERROR;
    }

    indexes_release(md);

    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_destroy_transport
 * Cleans up the transport in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int mdhim_private_destroy_transport(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    delete md->p->transport;
    md->p->transport = nullptr;
    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_destroy_db_options
 * Cleans up the db options in mdhim_t
 *
 * @param md MDHIM context
 * @return MDHIM status value
 */
static int mdhim_private_destroy_db_options(mdhim_t *md) {
    if (!md || !md->p){
        return MDHIM_ERROR;
    }

    // do not delete db_opts - it is not owned by mdhim_t
    md->p->db_opts = nullptr;
    return MDHIM_SUCCESS;
}

/**
 * mdhim_private_destroy
 * Cleans up the private mdhim_t variables.
 * Only md->p is cleaned up. md itself is not deleted.
 *
 * @param md        the mdhim context to destroy
 * @param MDHIM_SUCCESS or MDHIM_ERROR
 */
int mdhim_private_destroy(mdhim_t *md) {
    if (!md || !md->p) {
        return MDHIM_ERROR;
    }

    //Force shutdown if not already started
    md->p->shutdown = 1;

    //Stop range server if I'm a range server
    if (md->p->mdhim_rs) {
        range_server_stop(md);
    }

    // Clean up transport specific range server data
    if (md->p->range_server_destroy) {
        md->p->range_server_destroy();
    }

    //Free up memory used by message buffer
    ::operator delete(md->p->receive_msg);
    md->p->receive_msg = nullptr;

    //Free up memory used by the transport
    mdhim_private_destroy_transport(md);

    //Free up memory used by indexes
    mdhim_private_destroy_index(md);

    //Free up memory used by the db options
    mdhim_private_destroy_db_options(md);

    // delete the private variable
    delete md->p;
    md->p = nullptr;

    return MDHIM_SUCCESS;
}

/**
 * _decompose_db
 * Decompose a database id into a rank and index
 *
 * @param md     the mdhim context
 * @param db     the database id
 * @param rank   (optional) the rank the database is located at
 * @param rs_idx (optional) the index the database is at inside the rank
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int _decompose_db(index_t *index, const int db, int *rank, int *rs_idx) {
    if (!index || (db < 0)) {
        return MDHIM_ERROR;
    }

    if (rank) {
        *rank = index->range_server_factor * db / index->dbs_per_server;
    }

    if (rs_idx) {
        *rs_idx = db % index->dbs_per_server;
    }

    return MDHIM_SUCCESS;
}

/**
 * _compose_db
 * Converts a rank and index back into a database
 *
 * @param md     the mdhim context
 * @param db     the database id
 * @param rank   the rank the database is located at
 * @param rs_idx the index the database is at inside the rank
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int _compose_db(index_t *index, int *db, const int rank, const int rs_idx) {
    if (!index || !db) {
        return MDHIM_ERROR;
    }

    *db = index->dbs_per_server * rank / index->range_server_factor + rs_idx;
    return MDHIM_SUCCESS;
}

/**
 * _put_record
 * Puts a record into MDHIM
 *
 * @param md          main MDHIM struct
 * @param index       the index to put the key to
 * @param key         pointer to key to put
 * @param key_len     the length of the key
 * @param value       pointer to value to put
 * @param value_len   the length of the value
 * @return TransportRecvMessage * or nullptr on error
 */
TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                  void *key, std::size_t key_len,
                                  void *value, std::size_t value_len) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len ||
        !value || !value_len) {
        return nullptr;
    }

    rangesrv_list *rl = nullptr;
    index_t *lookup_index = nullptr;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    //Get the range server this key will be sent to
    void *lookup = (index->type == LOCAL_INDEX)?value:key;
    std::size_t lookup_len = (index->type == LOCAL_INDEX)?value_len:key_len;
    if (!(rl = get_range_servers(md->size, lookup_index, lookup, lookup_len))) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
             "Error while determining range server in _put_record",
             md->rank);
        return nullptr;
    }

    TransportRecvMessage *rm = nullptr;
    while (rl) {
        TransportPutMessage *pm = new TransportPutMessage();
        if (!pm) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while allocating memory in _put_record",
                 md->rank);
            return nullptr;
        }

        //Initialize the put message
        pm->key = key;
        pm->key_len = key_len;
        pm->value = value;
        pm->value_len = value_len;
        pm->src = md->rank;
        pm->index = index->id;
        pm->index_type = index->type;

        if (_decompose_db(index, rl->ri->database, &pm->dst, &pm->rs_idx) != MDHIM_SUCCESS) {
            delete pm;
            return nullptr;
        }

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(index) && md->rank == pm->dst) {
            rm = local_client_put(md, pm);
        } else {
            //Send the message through the network as this message is for another rank
            rm = md->p->transport->Put(pm);
            delete pm;
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return rm;
}

TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                  void *key, std::size_t key_len,
                                  void *value, std::size_t value_len,
                                  const int database) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len ||
        !value || !value_len) {
        return nullptr;
    }

    TransportRecvMessage *rm = nullptr;
    TransportPutMessage *pm = new TransportPutMessage();
    if (!pm) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
             "Error while allocating memory in _put_record",
             md->rank);
        return nullptr;
    }

    //Initialize the put message
    pm->key = key;
    pm->key_len = key_len;
    pm->value = value;
    pm->value_len = value_len;
    pm->src = md->rank;
    pm->index = index->id;
    pm->index_type = index->type;

    if (_decompose_db(index, database, &pm->dst, &pm->rs_idx) != MDHIM_SUCCESS) {
        delete pm;
        return nullptr;
    }

    //If I'm a range server and I'm the one this key goes to, send the message locally
    if (im_range_server(index) && md->rank == pm->dst) {
        rm = local_client_put(md, pm);
    } else {
        //Send the message through the network as this message is for another rank
        rm = md->p->transport->Put(pm);
        delete pm;
    }

    return rm;
}

/**
 * _get_record
 * Gets a record into MDHIM
 *
 * @param md          main MDHIM struct
 * @param index       the index to get the key from
 * @param key         pointer to key to get
 * @param key_len     the length of the key
 * @return TransportGetRecvMessage * or nullptr on error
 */
TransportGetRecvMessage *_get_record(mdhim_t *md, index_t *index,
                                     void *key, std::size_t key_len,
                                     enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len) {
        return nullptr;
    }

    rangesrv_list *rl = nullptr;
    //Get the range server this key will be sent to
    if ((op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) &&
        index->type != LOCAL_INDEX &&
        (rl = get_range_servers(md->size, index, key, key_len)) ==
        nullptr) {
        return nullptr;
    } else if ((index->type == LOCAL_INDEX ||
                (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
               (rl = get_range_servers_from_stats(md->rank, index, key, key_len, op)) ==
               nullptr) {
        return nullptr;
    }

    TransportGetRecvMessage *grm = nullptr;
    while (rl) {
        TransportGetMessage *gm = new TransportGetMessage();
        gm->key = key;
        gm->key_len = key_len;
        gm->num_keys = 1;
        gm->src = md->rank;
        gm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
        gm->index = index->id;
        gm->index_type = index->type;

        if (_decompose_db(index, rl->ri->database, &gm->dst, &gm->rs_idx) != MDHIM_SUCCESS) {
            delete gm;
            return nullptr;
        }

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(index) && md->rank == gm->dst) {
            grm = local_client_get(md, gm);
        } else {
            //Send the message through the network as this message is for another rank
            grm = md->p->transport->Get(gm);
            delete gm;
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return grm;
}

TransportGetRecvMessage *_get_record(mdhim_t *md, index_t *index,
                                     void *key, std::size_t key_len,
                                     const int database,
                                     enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len) {
        return nullptr;
    }

    TransportGetRecvMessage *grm = nullptr;
    TransportGetMessage *gm = new TransportGetMessage();
    gm->key = key;
    gm->key_len = key_len;
    gm->num_keys = 1;
    gm->src = md->rank;
    gm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
    gm->index = index->id;
    gm->index_type = index->type;

    if (_decompose_db(index, database, &gm->dst, &gm->rs_idx) != MDHIM_SUCCESS) {
        delete gm;
        return nullptr;
    }

    //If I'm a range server and I'm the one this key goes to, send the message locally
    if (im_range_server(index) && md->rank == gm->dst) {
        grm = local_client_get(md, gm);
    } else {
        //Send the message through the network as this message is for another rank
        grm = md->p->transport->Get(gm);
        delete gm;
    }

    return grm;
}

/* adds new to the list pointed to by head */
static void _concat_brm(TransportBRecvMessage **head, TransportBRecvMessage *addition) {
    if (!head || !addition) {
        return;
    }

    if (!*head) {
        *head = addition;
    }

    TransportBRecvMessage *brmp = *head;
    while (brmp->next) {
        brmp = brmp->next;
    }

    brmp->next = addition;
}

/**
 * _bput_records
 * BPuts records into MDHIM
 *
 * @param md          main MDHIM struct
 * @param index       the index to put the key to
 * @param keys        array of pointers to keys to put
 * @param key_lens    array of the key lengths
 * @param values      array of pointers to values to put
 * @param value_lens  array of value lengths
 * @param num_keys    the number of key value pairs
 * @return TransportRecvMessage * or nullptr on error
 */
TransportBRecvMessage *_bput_records(mdhim_t *md, index_t *index,
                                     void **keys, std::size_t *key_lens,
                                     void **values, std::size_t *value_lens,
                                     std::size_t num_keys) {
    index_t *lookup_index = nullptr;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    //The message to be sent to ourselves if necessary
    TransportBPutMessage *lbpm = nullptr;

    //Create an array of bulk put messages that holds one bulk message per range server
    TransportBPutMessage **bpm_list = new TransportBPutMessage*[lookup_index->num_rangesrvs]();

    /* Go through each of the records to find the range server(s) the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys; i++) {
        //Get the range server this key will be sent to
        rangesrv_list *rl = nullptr;
        if (index->type == LOCAL_INDEX) {
            if ((rl = get_range_servers(md->size, lookup_index, values[i], value_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                     "Error while determining range server in mdhimBPut",
                     md->rank);
                continue;
            }
        } else {
            if ((rl = get_range_servers(md->size, lookup_index, keys[i], key_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                     "Error while determining range server in mdhimBPut",
                     md->rank);
                continue;
            }
        }

        //There could be more than one range server returned in the case of the local index
        while (rl) {
            int dst_rank, rs_idx;
            if (_decompose_db(index, rl->ri->database, &dst_rank, &rs_idx) != MDHIM_SUCCESS) {
                return nullptr;
            }

            // Get the message that will be sent
            TransportBPutMessage *bpm = nullptr;
            if (md->rank != dst_rank) {
                //Set the message in the list for this range server
                bpm = bpm_list[dst_rank];
            } else {
                //Set the local message
                bpm = lbpm;
            }

            //If the message doesn't exist, create one
            if (!bpm) {
                bpm = new TransportBPutMessage();
                bpm->num_keys = 0;
                bpm->src = md->rank;
                bpm->dst = dst_rank;
                bpm->index = index->id;
                bpm->index_type = index->type;

                bpm->rs_idx = new int[num_keys]();
                bpm->keys = new void *[num_keys]();
                bpm->key_lens = new std::size_t[num_keys]();
                bpm->values = new void *[num_keys]();
                bpm->value_lens = new std::size_t[num_keys]();

                if (md->rank != bpm->dst) {
                    bpm_list[dst_rank] = bpm;
                } else {
                    lbpm = bpm;
                }
            }

            //Add the key, lengths, and data to the message
            bpm->keys[bpm->num_keys] = keys[i];
            bpm->key_lens[bpm->num_keys] = key_lens[i];
            bpm->values[bpm->num_keys] = values[i];
            bpm->value_lens[bpm->num_keys] = value_lens[i];
            bpm->rs_idx[bpm->num_keys] = rs_idx;
            bpm->num_keys++;

            rangesrv_list *rlp = rl;
            rl = rl->next;
            free(rlp);
        }
    }

    //Make a list out of the received messages to return
    TransportBRecvMessage *brm_head = md->p->transport->BPut(index->num_rangesrvs, bpm_list);
    if (lbpm) {
        TransportBRecvMessage *brm = local_client_bput(md, lbpm);
        if (brm) {
            _concat_brm(&brm, brm_head);
            brm_head = brm;
        }
    }

    //Free up messages sent
    for (std::size_t i = 0; i < index->num_rangesrvs; i++) {
        delete bpm_list[i];
    }

    // free(bpm_list);
    delete [] bpm_list;

    //Return the head of the list
    return brm_head;
}

TransportBRecvMessage *_bput_records(mdhim_t *md, index_t *index,
                                     void **keys, std::size_t *key_lens,
                                     void **values, std::size_t *value_lens,
                                     const int *databases,
                                     std::size_t num_keys) {
    //The message to be sent to ourselves if necessary
    TransportBPutMessage *lbpm = nullptr;

    //Create an array of bulk put messages that holds one bulk message per range server
    TransportBPutMessage **bpm_list = new TransportBPutMessage*[index->num_rangesrvs]();

    /* Go through each of the records to find the range server(s) the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys; i++) {
        int dst_rank, rs_idx;
        if (_decompose_db(index, databases[i], &dst_rank, &rs_idx) != MDHIM_SUCCESS) {
            return nullptr;
        }

        // Get the message that will be sent
        TransportBPutMessage *bpm = nullptr;
        if (md->rank != dst_rank) {
            //Set the message in the list for this range server
            bpm = bpm_list[dst_rank];
        } else {
            //Set the local message
            bpm = lbpm;
        }

        //If the message doesn't exist, create one
        if (!bpm) {
            bpm = new TransportBPutMessage();
            bpm->num_keys = 0;
            bpm->src = md->rank;
            bpm->dst = dst_rank;
            bpm->index = index->id;
            bpm->index_type = index->type;

            bpm->rs_idx = new int[num_keys]();
            bpm->keys = new void *[num_keys]();
            bpm->key_lens = new std::size_t[num_keys]();
            bpm->values = new void *[num_keys]();
            bpm->value_lens = new std::size_t[num_keys]();

            if (md->rank != bpm->dst) {
                bpm_list[dst_rank] = bpm;
            } else {
                lbpm = bpm;
            }
        }

        //Add the key, lengths, and data to the message
        bpm->keys[bpm->num_keys] = keys[i];
        bpm->key_lens[bpm->num_keys] = key_lens[i];
        bpm->values[bpm->num_keys] = values[i];
        bpm->value_lens[bpm->num_keys] = value_lens[i];
        bpm->rs_idx[bpm->num_keys] = rs_idx;
        bpm->num_keys++;
    }

    //Make a list out of the received messages to return
    TransportBRecvMessage *brm_head = md->p->transport->BPut(index->num_rangesrvs, bpm_list);
    if (lbpm) {
        TransportBRecvMessage *brm = local_client_bput(md, lbpm);
        if (brm) {
            _concat_brm(&brm, brm_head);
            brm_head = brm;
        }
    }

    //Free up messages sent
    for (std::size_t i = 0; i < index->num_rangesrvs; i++) {
        delete bpm_list[i];
    }

    // free(bpm_list);
    delete [] bpm_list;

    //Return the head of the list
    return brm_head;
}

/**
 * _bget_records
 * BGets records from MDHIM
 *
 * @param md          main MDHIM struct
 * @param index       the index to get the key from
 * @param keys        array of pointers to keys to put
 * @param key_lens    array of the key lengths
 * @param num_keys    the number of key value pairs
 * @param num_records the number of records to get back
 * @param op          the comparison to use for key matching
 * @return TransportBGetRecvMessage * or nullptr on error
 */
TransportBGetRecvMessage *_bget_records(mdhim_t *md, index_t *index,
                                        void **keys, std::size_t *key_lens,
                                        std::size_t num_keys, std::size_t num_records,
                                        enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !index ||
        !keys || !key_lens) {
        return nullptr;
    }

    //Create an array of bulk get messages that holds one bulk message per range server
    TransportBGetMessage **bgm_list = new TransportBGetMessage*[index->num_rangesrvs]();
    TransportBGetMessage *lbgm = nullptr; //The message to be sent to ourselves if necessary
    rangesrv_list *rl = nullptr;

    /* Go through each of the records to find the range server the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys; i++) {
        //Get the range server this key will be sent to
        if ((op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) &&
            index->type != LOCAL_INDEX &&
            (rl = get_range_servers(md->size, index, keys[i], key_lens[i])) ==
            nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Error while determining range server in mdhimBget",
                 md->rank);
            delete [] bgm_list;
            return nullptr;
        } else if ((index->type == LOCAL_INDEX ||
                    (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
                   (rl = get_range_servers_from_stats(md->rank, index, keys[i], key_lens[i], op)) ==
                   nullptr) {

            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Error while determining range server in mdhimBget",
                 md->rank);
            delete [] bgm_list;
            return nullptr;
        }
        // else if (op == TransportGetMessageOp::GET_FIRST) {
        //     rangesrv_info_t *ret_rp = (rangesrv_info_t *) calloc(1, sizeof(rangesrv_info_t));
        //     ret_rp->database = 0; // this might not be correct
        //     rl = (rangesrv_list *) malloc(sizeof(rangesrv_list));
        //     _add_to_rangesrv_list(&rl, ret_rp);
        // }
        // else if (op == TransportGetMessageOp::GET_LAST) {
        //     rangesrv_info_t *ret_rp = (rangesrv_info_t *) calloc(1, sizeof(rangesrv_info_t));
        //     ret_rp->database = get_num_databases(md->size, index->range_server_factor, index->dbs_per_server) - 1;
        //     rl = (rangesrv_list *) malloc(sizeof(rangesrv_list));
        //     _add_to_rangesrv_list(&rl, ret_rp);
        // }

        while (rl) {
            int dst_rank, rs_idx;
            if (_decompose_db(index, rl->ri->database, &dst_rank, &rs_idx) != MDHIM_SUCCESS) {
                return nullptr;
            }

            TransportBGetMessage *bgm = nullptr;
            if (md->rank != dst_rank) {
                //Set the message in the list for this range server
                bgm = bgm_list[dst_rank];
            } else {
                bgm = lbgm;
            }

            //If the message doesn't exist, create one
            if (!bgm) {
                bgm = new TransportBGetMessage();
                bgm->num_keys = 0;
                bgm->num_recs = num_records;
                bgm->src = md->rank;
                bgm->dst = dst_rank;
                bgm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
                bgm->index = index->id;
                bgm->index_type = index->type;
                bgm->rs_idx = new int[num_keys]();
                bgm->keys = new void *[num_keys]();
                bgm->key_lens = new std::size_t[num_keys]();

                if (md->rank != dst_rank) {
                    bgm_list[dst_rank] = bgm;
                } else {
                    lbgm = bgm;
                }
            }

            //Add the key, lengths, and data to the message
            bgm->keys[bgm->num_keys] = keys[i];
            bgm->key_lens[bgm->num_keys] = key_lens[i];
            bgm->rs_idx[bgm->num_keys] = rs_idx;
            bgm->num_keys++;

            rangesrv_list *rlp = rl;
            rl = rl->next;
            free(rlp);
        }
    }

    //Make a list out of the received messages to return
    TransportBGetRecvMessage *bgrm_head = md->p->transport->BGet(index->num_rangesrvs, bgm_list);
    if (lbgm) {
        TransportBGetRecvMessage *lbgrm = local_client_bget(md, lbgm);
        lbgrm->next = bgrm_head;
        bgrm_head = lbgrm;
    }

    delete [] bgm_list;

    return bgrm_head;
}

TransportBGetRecvMessage *_bget_records(mdhim_t *md, index_t *index,
                                        void **keys, std::size_t *key_lens,
                                        const int *databases,
                                        std::size_t num_keys, std::size_t num_records,
                                        enum TransportGetMessageOp op) {
    if (!md || !md->p ||
        !index ||
        !keys || !key_lens) {
        return nullptr;
    }

    //Create an array of bulk get messages that holds one bulk message per range server
    TransportBGetMessage **bgm_list = new TransportBGetMessage*[index->num_rangesrvs]();
    TransportBGetMessage *lbgm = nullptr; //The message to be sent to ourselves if necessary

    /* Go through each of the records to find the range server the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys; i++) {
        int dst_rank, rs_idx;
        if (_decompose_db(index, databases[i], &dst_rank, &rs_idx) != MDHIM_SUCCESS) {
            return nullptr;
        }

        TransportBGetMessage *bgm = nullptr;
        if (md->rank != dst_rank) {
            //Set the message in the list for this range server
            bgm = bgm_list[dst_rank];
        } else {
            bgm = lbgm;
        }

        //If the message doesn't exist, create one
        if (!bgm) {
            bgm = new TransportBGetMessage();
            bgm->num_keys = 0;
            bgm->num_recs = num_records;
            bgm->src = md->rank;
            bgm->dst = dst_rank;
            bgm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
            bgm->index = index->id;
            bgm->index_type = index->type;
            bgm->rs_idx = new int[num_keys]();
            bgm->keys = new void *[num_keys]();
            bgm->key_lens = new std::size_t[num_keys]();

            if (md->rank != dst_rank) {
                bgm_list[dst_rank] = bgm;
            } else {
                lbgm = bgm;
            }
        }

        //Add the key, lengths, and data to the message
        bgm->keys[bgm->num_keys] = keys[i];
        bgm->key_lens[bgm->num_keys] = key_lens[i];
        bgm->rs_idx[bgm->num_keys] = rs_idx;
        bgm->num_keys++;
    }

    //Make a list out of the received messages to return
    TransportBGetRecvMessage *bgrm_head = md->p->transport->BGet(index->num_rangesrvs, bgm_list);
    if (lbgm) {
        TransportBGetRecvMessage *lbgrm = local_client_bget(md, lbgm);
        lbgrm->next = bgrm_head;
        bgrm_head = lbgrm;
    }

    delete [] bgm_list;

    return bgrm_head;
}

/**
 * _del_record
 * Deletes a record from MDHIM
 *
 * @param md main MDHIM struct
 * @param index       the index to delete the key from
 * @param key         pointer to array of keys to delete
 * @param key_len     array with lengths of each key in keys
 * @return TransportRecvMessage * or nullptr on error
 */
TransportRecvMessage *_del_record(mdhim_t *md, index_t *index,
                                  void *key, std::size_t key_len) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len) {
        return nullptr;
    }

    index_t *lookup_index = nullptr;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    rangesrv_list *rl = nullptr;

    //Get the range server this key will be sent to
    if (index->type == LOCAL_INDEX) {
        if (!(rl = get_range_servers_from_stats(md->rank, index, key, key_len, TransportGetMessageOp::GET_EQ))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBDel",
                 md->rank);
            return nullptr;
        }
    } else {
        //Get the range server this key will be sent to
        if (!(rl = get_range_servers(md->size, index, key, key_len))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in _del_record",
                 md->rank);
            return nullptr;
        }
    }

    TransportRecvMessage *rm = nullptr;
    while (rl) {
        TransportDeleteMessage *dm = new TransportDeleteMessage();
        if (!dm) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while allocating memory in _del_record",
                 md->rank);
            return nullptr;
        }

        dm->key = key;
        dm->key_len = key_len;
        dm->src = md->rank;
        dm->index = index->id;
        dm->index_type = index->type;

        if (_decompose_db(index, rl->ri->database, &dm->dst, &dm->rs_idx) != MDHIM_SUCCESS) {
            delete dm;
            return nullptr;
        }

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(index) && md->rank == dm->dst) {
            rm = local_client_delete(md, dm);
        } else {
            //Send the message through the network as this message is for another rank
            rm = md->p->transport->Delete(dm);
            delete dm;
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return rm;
}

TransportRecvMessage *_del_record(mdhim_t *md, index_t *index,
                                  void *key, std::size_t key_len,
                                  const int database) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len) {
        return nullptr;
    }

    TransportRecvMessage *rm = nullptr;
    TransportDeleteMessage *dm = new TransportDeleteMessage();
    if (!dm) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
             "Error while allocating memory in _del_record",
             md->rank);
        return nullptr;
    }

    dm->key = key;
    dm->key_len = key_len;
    dm->src = md->rank;
    dm->index = index->id;
    dm->index_type = index->type;

    if (_decompose_db(index, database, &dm->dst, &dm->rs_idx) != MDHIM_SUCCESS) {
        delete dm;
        return nullptr;
    }

    //If I'm a range server and I'm the one this key goes to, send the message locally
    if (im_range_server(index) && md->rank == dm->dst) {
        rm = local_client_delete(md, dm);
    } else {
        //Send the message through the network as this message is for another rank
        rm = md->p->transport->Delete(dm);
        delete dm;
    }

    return rm;
}

/**
 * _bdel_records
 * Deletes multiple records from MDHIM
 *
 * @param md           main MDHIM struct
 * @param index        the index to delete the key from
 * @param keys         pointer to array of keys to delete
 * @param key_lens     array with lengths of each key in keys
 * @param num_keys     the number of keys to delete (i.e., the number of keys in keys array)
 * @return TransportBRecvMessage * or nullptr on error
 */
TransportBRecvMessage *_bdel_records(mdhim_t *md, index_t *index,
                                     void **keys, std::size_t *key_lens,
                                     std::size_t num_keys) {
    if (!md || !md->p ||
        !index ||
        !keys || !key_lens) {
        return nullptr;
    }

    //The message to be sent to ourselves if necessary
    TransportBDeleteMessage *lbdm = nullptr;

    //Create an array of bulk del messages that holds one bulk message per range server
    TransportBDeleteMessage **bdm_list = new TransportBDeleteMessage *[index->num_rangesrvs]();

    rangesrv_list *rl = nullptr;

    /* Go through each of the records to find the range server the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys; i++) {
        //Get the range server this key will be sent to
        if (index->type != LOCAL_INDEX &&
            !(rl = get_range_servers(md->size, index, keys[i], key_lens[i]))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBdel",
                 md->rank);
            continue;
        } else if (index->type == LOCAL_INDEX &&
                   !(rl = get_range_servers_from_stats(md->rank, index, keys[i], key_lens[i], TransportGetMessageOp::GET_EQ))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBdel",
                 md->rank);
            continue;
        }

        while (rl) {
            int dst_rank, rs_idx;
            if (_decompose_db(index, rl->ri->database, &dst_rank, &rs_idx) != MDHIM_SUCCESS) {
                return nullptr;
            }

            TransportBDeleteMessage *bdm = nullptr;
            if (md->rank != dst_rank) {
                //Set the message in the list for this range server
                bdm = bdm_list[dst_rank - 1];
            } else {
                //Set the local message
                bdm = lbdm;
            }

            //If the message doesn't exist, create one
            if (!bdm) {
                bdm = new TransportBDeleteMessage();
                bdm->num_keys = 0;
                bdm->src = md->rank;
                bdm->dst = dst_rank;
                bdm->index = index->id;
                bdm->index_type = index->type;
                bdm->rs_idx = new int[num_keys]();
                bdm->keys = new void *[num_keys]();
                bdm->key_lens = new std::size_t[num_keys]();

                if (md->rank != dst_rank) {
                    bdm_list[dst_rank - 1] = bdm;
                } else {
                    lbdm = bdm;
                }
            }

            //Add the key, lengths, and data to the message
            bdm->keys[bdm->num_keys] = keys[i];
            bdm->key_lens[bdm->num_keys] = key_lens[i];
            bdm->rs_idx[bdm->num_keys] = rs_idx;
            bdm->num_keys++;

            rangesrv_list *rlp = rl;
            rl = rl->next;
            free(rlp);
        }
    }

    //Make a list out of the received messages to return
    TransportBRecvMessage *brm_head = md->p->transport->BDelete(index->num_rangesrvs, bdm_list);
    if (lbdm) {
        TransportBRecvMessage *brm = local_client_bdelete(md, lbdm);
        if (brm) {
            brm->next = brm_head;
            brm_head = brm;
        }
    }

    for (std::size_t i = 0; i < index->num_rangesrvs; i++) {
        delete bdm_list[i];
    }

    delete [] bdm_list;

    //Return the head of the list
    return brm_head;
}

TransportBRecvMessage *_bdel_records(mdhim_t *md, index_t *index,
                                     void **keys, std::size_t *key_lens,
                                     const int *databases,
                                     std::size_t num_keys) {
    if (!md || !md->p ||
        !index ||
        !keys || !key_lens) {
        return nullptr;
    }

    //The message to be sent to ourselves if necessary
    TransportBDeleteMessage *lbdm = nullptr;

    //Create an array of bulk del messages that holds one bulk message per range server
    TransportBDeleteMessage **bdm_list = new TransportBDeleteMessage *[index->num_rangesrvs]();

    /* Go through each of the records to find the range server the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys; i++) {
        int dst_rank, rs_idx;
        if (_decompose_db(index, databases[i], &dst_rank, &rs_idx) != MDHIM_SUCCESS) {
            return nullptr;
        }

        TransportBDeleteMessage *bdm = nullptr;
        if (md->rank != dst_rank) {
            //Set the message in the list for this range server
            bdm = bdm_list[dst_rank - 1];
        } else {
            //Set the local message
            bdm = lbdm;
        }

        //If the message doesn't exist, create one
        if (!bdm) {
            bdm = new TransportBDeleteMessage();
            bdm->num_keys = 0;
            bdm->src = md->rank;
            bdm->dst = dst_rank;
            bdm->index = index->id;
            bdm->index_type = index->type;
            bdm->rs_idx = new int[num_keys]();
            bdm->keys = new void *[num_keys]();
            bdm->key_lens = new std::size_t[num_keys]();

            if (md->rank != dst_rank) {
                bdm_list[dst_rank - 1] = bdm;
            } else {
                lbdm = bdm;
            }
        }

        //Add the key, lengths, and data to the message
        bdm->keys[bdm->num_keys] = keys[i];
        bdm->key_lens[bdm->num_keys] = key_lens[i];
        bdm->rs_idx[bdm->num_keys] = rs_idx;
        bdm->num_keys++;
    }

    //Make a list out of the received messages to return
    TransportBRecvMessage *brm_head = md->p->transport->BDelete(index->num_rangesrvs, bdm_list);
    if (lbdm) {
        TransportBRecvMessage *brm = local_client_bdelete(md, lbdm);
        if (brm) {
            brm->next = brm_head;
            brm_head = brm;
        }
    }

    for (std::size_t i = 0; i < index->num_rangesrvs; i++) {
        delete bdm_list[i];
    }

    delete [] bdm_list;

    //Return the head of the list
    return brm_head;
}

int _which_db(mdhim_t *md, void *key, std::size_t key_len)
{
    rangesrv_list *rl = get_range_servers(md->size, md->p->primary_index, key, key_len);
    const int db = rl?rl->ri->database:MDHIM_ERROR;
    free(rl);
    return db;
}