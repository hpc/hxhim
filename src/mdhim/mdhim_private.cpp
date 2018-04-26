#include <cstdlib>
#include <map>
#include <unistd.h>
#include <sys/types.h>

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

#include "indexes.h"
#include "local_client.h"
#include "mdhim_options_private.h"
#include "mdhim_private.h"
#include "partitioner.h"
#include "transport_mpi.hpp"
#include "transport_thallium.hpp"

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
    if (!opts) {
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

    // give the range server access to the memory buffer sizes
    MPIRangeServer::init(md, fbp);

    MPIEndpointGroup *eg = new MPIEndpointGroup(opts->comm_, md->lock, fbp);

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
    if (!opts) {
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
    if (pthread_rwlock_init(&md->p->indexes_lock, nullptr) != 0) {
        return MDHIM_ERROR;
    }

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

    //Initialize the partitioner
    partitioner_init();

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

    //Free up memory used by the partitioner
    partitioner_release();

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

    index_t *put_index = index;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    //Get the range server this key will be sent to
    if (put_index->type == LOCAL_INDEX) {
        if (!(rl = get_range_servers(md, lookup_index, value, value_len))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBPut",
                 md->rank);
            return nullptr;
        }
    } else {
        //Get the range server this key will be sent to
        if (!(rl = get_range_servers(md, lookup_index, key, key_len))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in _put_record",
                 md->rank);
            return nullptr;
        }
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
        pm->mtype = TransportMessageType::PUT;
        pm->key = key;
        pm->key_len = key_len;
        pm->value = value;
        pm->value_len = value_len;
        pm->src = md->rank;
        pm->dst = rl->ri->rank;
        pm->index = put_index->id;
        pm->index_type = put_index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(put_index) && md->rank == pm->dst) {
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

    //Create an array of bulk get messages that holds one bulk message per range server
    rangesrv_list *rl = nullptr;
    //Get the range server this key will be sent to
    if ((op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) &&
        index->type != LOCAL_INDEX &&
        (rl = get_range_servers(md, index, key, key_len)) ==
        nullptr) {
        return nullptr;
    } else if ((index->type == LOCAL_INDEX ||
                (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
               (rl = get_range_servers_from_stats(md, index, key, key_len, op)) ==
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
        gm->dst = rl->ri->rank;
        gm->mtype = TransportMessageType::GET;
        gm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
        gm->index = index->id;
        gm->index_type = index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(index) &&  md->rank == gm->dst) {
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

/* Creates a linked list of mdhim_rm_t messages */
static TransportBRecvMessage *_create_brm(TransportRecvMessage *rm) {
    if (!rm) {
        return nullptr;
    }

    TransportBRecvMessage *brm = new TransportBRecvMessage();
    brm->error = rm->error;
    brm->mtype = rm->mtype;
    brm->index = rm->index;
    brm->index_type = rm->index_type;
    brm->src = rm->src;
    brm->dst = rm->dst;

    return brm;
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
    index_t *put_index = index;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }

    } else {
        lookup_index = index;
    }

    //Check to see that we were given a sane amount of records
    if (num_keys > MAX_BULK_OPS) {
        mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
             "To many bulk operations requested in mdhimBGetOp",
             md->rank);
        return nullptr;
    }

    //The message to be sent to ourselves if necessary
    TransportBPutMessage *lbpm = nullptr;

    //Create an array of bulk put messages that holds one bulk message per range server
    TransportBPutMessage **bpm_list = new TransportBPutMessage*[lookup_index->num_rangesrvs]();

    /* Go through each of the records to find the range server(s) the record belongs to.
       If there is not a bulk message in the array for the range server the key belongs to,
       then it is created.  Otherwise, the data is added to the existing message in the array.*/
    for (std::size_t i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
        //Get the range server this key will be sent to
        rangesrv_list *rl = nullptr;
        if (put_index->type == LOCAL_INDEX) {
            if ((rl = get_range_servers(md, lookup_index, values[i], value_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                     "Error while determining range server in mdhimBPut",
                     md->rank);
                continue;
            }
        } else {
            if ((rl = get_range_servers(md, lookup_index, keys[i], key_lens[i])) ==
                nullptr) {
                mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                     "Error while determining range server in mdhimBPut",
                     md->rank);
                continue;
            }
        }

        //There could be more than one range server returned in the case of the local index
        while (rl) {
            TransportBPutMessage *bpm = nullptr;
            if (rl->ri->rank != md->rank) {
                //Set the message in the list for this range server
                bpm = bpm_list[rl->ri->rangesrv_num - 1];
            } else {
                //Set the local message
                bpm = lbpm;
            }

            //If the message doesn't exist, create one
            if (!bpm) {
                bpm = new TransportBPutMessage();
                bpm->keys = new void *[MAX_BULK_OPS]();
                bpm->key_lens = new std::size_t[MAX_BULK_OPS]();
                bpm->values = new void *[MAX_BULK_OPS]();
                bpm->value_lens = new std::size_t[MAX_BULK_OPS]();
                bpm->num_keys = 0;
                bpm->src = md->rank;
                bpm->dst = rl->ri->rank;
                bpm->mtype = TransportMessageType::BPUT;
                bpm->index = put_index->id;
                bpm->index_type = put_index->type;
                if (rl->ri->rank != md->rank) {
                    bpm_list[rl->ri->rangesrv_num - 1] = bpm;
                } else {
                    lbpm = bpm;
                }
            }

            //Add the key, lengths, and data to the message
            bpm->keys[bpm->num_keys] = keys[i];
            bpm->key_lens[bpm->num_keys] = key_lens[i];
            bpm->values[bpm->num_keys] = values[i];
            bpm->value_lens[bpm->num_keys] = value_lens[i];
            bpm->num_keys++;
            rangesrv_list *rlp = rl;
            rl = rl->next;
            free(rlp);
        }
    }

    //Make a list out of the received messages to return
    TransportBRecvMessage *brm_head = md->p->transport->BPut(put_index->num_rangesrvs, bpm_list);
    if (lbpm) {
        TransportRecvMessage *rm = local_client_bput(md, lbpm);
        if (rm) {
            TransportBRecvMessage *brm = _create_brm(rm);
            _concat_brm(&brm, brm_head);
            brm_head = brm;
            delete rm;
        }
    }

    //Free up messages sent
    for (std::size_t i = 0; i < lookup_index->num_rangesrvs; i++) {
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
 * @param num_records ??
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
    for (std::size_t i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
        //Get the range server this key will be sent to
        if ((op == TransportGetMessageOp::GET_EQ || op == TransportGetMessageOp::GET_PRIMARY_EQ) &&
            index->type != LOCAL_INDEX &&
            (rl = get_range_servers(md, index, keys[i], key_lens[i])) ==
            nullptr) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Error while determining range server in mdhimBget",
                 md->rank);
            delete [] bgm_list;
            return nullptr;
        } else if ((index->type == LOCAL_INDEX ||
                    (op != TransportGetMessageOp::GET_EQ && op != TransportGetMessageOp::GET_PRIMARY_EQ)) &&
                   (rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], op)) ==
                   nullptr) {

            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %d - "
                 "Error while determining range server in mdhimBget",
                 md->rank);
            delete [] bgm_list;
            return nullptr;
        }

        while (rl) {
            TransportBGetMessage *bgm = nullptr;
            if (rl->ri->rank != md->rank) {
                //Set the message in the list for this range server
                bgm = bgm_list[rl->ri->rangesrv_num - 1];
            } else {
                bgm = lbgm;
            }

            //If the message doesn't exist, create one
            if (!bgm) {
                bgm = new TransportBGetMessage();
                bgm->keys = new void *[num_keys]();
                bgm->key_lens = new std::size_t[num_keys]();
                bgm->num_keys = 0;
                bgm->num_recs = num_records;
                bgm->src = md->rank;
                bgm->dst = rl->ri->rank;
                bgm->mtype = TransportMessageType::BGET;
                bgm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
                bgm->index = index->id;
                bgm->index_type = index->type;
                if (rl->ri->rank != md->rank) {
                    bgm_list[rl->ri->rangesrv_num - 1] = bgm;
                } else {
                    lbgm = bgm;
                }
            }

            //Add the key, lengths, and data to the message
            bgm->keys[bgm->num_keys] = keys[i];
            bgm->key_lens[bgm->num_keys] = key_lens[i];
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

    rangesrv_list *rl = nullptr;
    index_t *lookup_index = nullptr;

    index_t *del_index = index;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return nullptr;
        }
    } else {
        lookup_index = index;
    }

    //Get the range server this key will be sent to
    if (del_index->type == LOCAL_INDEX) {
        if (!(rl = get_range_servers_from_stats(md, lookup_index, key, key_len, TransportGetMessageOp::GET_EQ))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBDel",
                 md->rank);
            return nullptr;
        }
    } else {
        //Get the range server this key will be sent to
        if (!(rl = get_range_servers(md, lookup_index, key, key_len))) {
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

        //Initialize the del message
        dm->mtype = TransportMessageType::DELETE;
        dm->key = key;
        dm->key_len = key_len;
        dm->src = md->rank;
        dm->dst = rl->ri->rank;
        dm->index = del_index->id;
        dm->index_type = del_index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(del_index) && md->rank == dm->dst) {
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
    for (std::size_t i = 0; i < num_keys && i < MAX_BULK_OPS; i++) {
        //Get the range server this key will be sent to
        if (index->type != LOCAL_INDEX &&
            !(rl = get_range_servers(md, index, keys[i], key_lens[i]))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBdel",
                 md->rank);
            continue;
        } else if (index->type == LOCAL_INDEX &&
                   !(rl = get_range_servers_from_stats(md, index, keys[i], key_lens[i], TransportGetMessageOp::GET_EQ))) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBdel",
                 md->rank);
            continue;
        }

        TransportBDeleteMessage *bdm = nullptr;
        if (rl->ri->rank != md->rank) {
            //Set the message in the list for this range server
            bdm = bdm_list[rl->ri->rangesrv_num - 1];
        } else {
            //Set the local message
            bdm = lbdm;
        }

        //If the message doesn't exist, create one
        if (!bdm) {
            bdm = new TransportBDeleteMessage();
            bdm->keys = new void *[MAX_BULK_OPS]();
            bdm->key_lens = new std::size_t[MAX_BULK_OPS]();
            bdm->num_keys = 0;
            bdm->src = md->rank;
            bdm->dst = rl->ri->rank;
            bdm->mtype = TransportMessageType::BDELETE;
            bdm->index = index->id;
            bdm->index_type = index->type;
            if (rl->ri->rank != md->rank) {
                bdm_list[rl->ri->rangesrv_num - 1] = bdm;
            } else {
                lbdm = bdm;
            }
        }

        //Add the key, lengths, and data to the message
        bdm->keys[bdm->num_keys] = keys[i];
        bdm->key_lens[bdm->num_keys] = key_lens[i];
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

int _which_server(mdhim_t *md, void *key, std::size_t key_len)
{
    rangesrv_list *rl = get_range_servers(md, md->p->primary_index, key, key_len);
    int server = rl?rl->ri->rank:MDHIM_ERROR;
    free(rl);
    /* what is the difference between 'rank' and 'rangeserv_num' ? */
    return server;
}
