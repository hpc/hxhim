#include <cstdlib>
#include <map>
#include <memory>
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
#include "MemoryManagers.hpp"
#include "MPIInstance.hpp"

// get all thallium lookup addresses
int get_addrs(thallium::engine *engine, const MPI_Comm comm, std::map<int, std::string> &addrs) {
    if (!engine) {
        return MDHIM_ERROR;
    }

    int rank;
    if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    int size;
    if (MPI_Comm_size(comm, &size) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }

    // get local engine's address
    const std::string self = (std::string) engine->self();

    // get maximum size of all addresses
    const int self_len = self.size();
    int max_len = 0;
    if (MPI_Allreduce(&self_len, &max_len, 1, MPI_INT, MPI_MAX, comm) != MPI_SUCCESS) {
        return MDHIM_ERROR;
    }
    max_len++; // NULL terminate

    // get addresses
    char *buf = Memory::FBP_MEDIUM::Instance().acquire<char>(max_len * size);
    if (MPI_Allgather(self.c_str(), self.size(), MPI_CHAR, buf, max_len, MPI_CHAR, MPI_COMM_WORLD) != MPI_SUCCESS) {
        delete [] buf;
        return MDHIM_ERROR;
    }

    // copy the addresses into strings
    // and map the strings to unique IDs
    for(int i = 0; i < size; i++) {
        const char *remote = &buf[max_len * i];
        addrs[i].assign(remote, strlen(remote));
    }

    Memory::FBP_MEDIUM::Instance().release(buf);

    return MDHIM_SUCCESS;
}

int mdhim_private_init(mdhim_private* mdp, int dbtype, int transporttype) {
    int rc = MDHIM_ERROR;
    if (!mdp) {
        // goto err_out;
        return MDHIM_ERROR;
    }

    if (dbtype == LEVELDB) {
        rc = MDHIM_SUCCESS;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid data store type specified");
        rc = MDHIM_DB_ERROR;
        return MDHIM_ERROR;
        // goto err_out;
    }

    mdp->transport = new ((Transport *)Memory::FBP_MEDIUM::Instance().acquire(sizeof(Transport))) Transport(MPIInstance::instance().Rank());

    if (transporttype == MDHIM_TRANSPORT_MPI) {
        // create mapping between unique IDs and ranks
        for(int i = 0; i < MPIInstance::instance().Size(); i++) {
            mdp->transport->AddEndpoint(i, new ((MPIEndpoint *)Memory::FBP_MEDIUM::Instance().acquire()) MPIEndpoint(MPIInstance::instance().Comm(), i, mdp->shutdown));
        }

        // remove loopback endpoint
        mdp->transport->RemoveEndpoint(mdp->transport->EndpointID());

        mdp->listener_thread = MPIRangeServer::listener_thread;
        mdp->send_client_response = MPIRangeServer::send_client_response;

        rc = MDHIM_SUCCESS;
        goto err_out;
    }
    else if (transporttype == MDHIM_TRANSPORT_THALLIUM) {
        // create the engine (only 1 instance per process)
        thallium::engine *engine = new ((thallium::engine *)Memory::FBP_MEDIUM::Instance().acquire())
            thallium::engine("tcp", THALLIUM_SERVER_MODE);

        // create client to range server RPC
        thallium::remote_procedure *rpc = new ((thallium::remote_procedure *)Memory::FBP_MEDIUM::Instance().acquire())
            thallium::remote_procedure(engine->define(ThalliumRangeServer::CLIENT_TO_RANGE_SERVER_NAME,
                                                      ThalliumRangeServer::receive_rangesrv_work));

        // give the range server access to the mdhim_t data
        ThalliumRangeServer::init(mdp);

        // wait for every engine to start up
        MPI_Barrier(mdp->mdhim_comm);

        // get a mapping of unique IDs to thallium addresses
        std::map<int, std::string> addrs;
        if (get_addrs(engine, mdp->mdhim_comm, addrs) != MDHIM_SUCCESS) {
            rc = MDHIM_ERROR;
            goto err_out;
        }

        // do not remove loopback endpoint - need to keep a reference to engine somewhere

        // add the remote thallium endpoints to the tranport
        for(std::pair<const int, std::string> const &addr : addrs) {
            thallium::endpoint *server = new ((thallium::endpoint *) Memory::FBP_MEDIUM::Instance().acquire())
                thallium::endpoint(engine->lookup(addr.second));
            // ThalliumEndpoint* ep = new ((ThalliumEndpoint *) Memory::FBP_MEDIUM::Instance().acquire())
            //     ThalliumEndpoint(engine, rpc, server);
            // mdp->transport->AddEndpoint(addr.first, ep);
            std::cout << addr.first << " " << addrs.size() <<  std::endl;
            rpc->on(*server)(std::string("abcdef"));
        }

        mdp->listener_thread = nullptr;
        mdp->send_client_response = ThalliumRangeServer::send_client_response;

        rc = MDHIM_SUCCESS;
        goto err_out;
    }
    else {
        mlog(MDHIM_CLIENT_CRIT, "Invalid transport type specified");
        rc = MDHIM_ERROR;
        goto err_out;
    }

err_out:
    return rc;
}

TransportRecvMessage *_put_record(mdhim_t *md, index_t *index,
                                  void *key, int key_len,
                                  void *value, int value_len) {
    if (!md || !md->p ||
        !index ||
        !key || !key_len ||
        !value || !value_len) {
        return nullptr;
    }

    rangesrv_list *rl;
    index_t *lookup_index, *put_index;

    put_index = index;
    if (index->type == LOCAL_INDEX) {
        lookup_index = get_index(md, index->primary_id);
        if (!lookup_index) {
            return NULL;
        }
    } else {
        lookup_index = index;
    }

    //Get the range server this key will be sent to
    if (put_index->type == LOCAL_INDEX) {
        if ((rl = get_range_servers(md, lookup_index, value, value_len)) ==
            NULL) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in mdhimBPut",
                 md->p->transport->EndpointID());
            return NULL;
        }
    } else {
        //Get the range server this key will be sent to
        if ((rl = get_range_servers(md, lookup_index, key, key_len)) == NULL) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while determining range server in _put_record",
                 md->p->transport->EndpointID());
            return NULL;
        }
    }

    TransportRecvMessage *rm = nullptr;
    while (rl) {
        TransportPutMessage *pm = Memory::FBP_MEDIUM::Instance().acquire<TransportPutMessage>();
        if (!pm) {
            mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank: %d - "
                 "Error while allocating memory in _put_record",
                 md->p->transport->EndpointID());
            return NULL;
        }

        //Initialize the put message
        pm->mtype = TransportMessageType::PUT;
        pm->key = key;
        pm->key_len = key_len;
        pm->value = value;
        pm->value_len = value_len;
        pm->src = md->p->transport->EndpointID();
        pm->dst = rl->ri->rank;
        pm->index = put_index->id;
        pm->index_type = put_index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(put_index) && md->p->transport->EndpointID() == pm->dst) {
            rm = local_client_put(md, pm);
        } else {
            //Send the message through the network as this message is for another rank
            rm = md->p->transport->Put(pm);
            Memory::FBP_MEDIUM::Instance().release(pm);
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return rm;
}

TransportGetRecvMessage *_get_record(mdhim_t *md, index_t *index,
                                     void *key, int key_len,
                                     enum TransportGetMessageOp op) {
    if (!md || !md->p || !index || !key || !key_len) {
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
        TransportGetMessage *gm = Memory::FBP_MEDIUM::Instance().acquire<TransportGetMessage>();
        gm->key = key;
        gm->key_len = key_len;
        gm->num_keys = 1;
        gm->src = md->p->transport->EndpointID();
        gm->dst = rl->ri->rank;
        gm->mtype = TransportMessageType::GET;
        gm->op = (op == TransportGetMessageOp::GET_PRIMARY_EQ)?TransportGetMessageOp::GET_EQ:op;
        gm->index = index->id;
        gm->index_type = index->type;

        //If I'm a range server and I'm the one this key goes to, send the message locally
        if (im_range_server(index) && md->p->transport->EndpointID() == gm->dst) {
            grm = local_client_get(md, gm);
        } else {
            //Send the message through the network as this message is for another rank
            grm = md->p->transport->Get(gm);
            Memory::FBP_MEDIUM::Instance().release(gm);
        }

        rangesrv_list *rlp = rl;
        rl = rl->next;
        free(rlp);
    }

    return grm;
}

/* Creates a linked list of mdhim_rm_t messages */
TransportBRecvMessage *_create_brm(TransportRecvMessage *rm) {
    if (!rm) {
        return nullptr;
    }

    TransportBRecvMessage *brm = Memory::FBP_MEDIUM::Instance().acquire<TransportBRecvMessage>();
    brm->error = rm->error;
    brm->mtype = rm->mtype;
    brm->index = rm->index;
    brm->index_type = rm->index_type;
    brm->dst = rm->dst;

    return brm;
}

/* adds new to the list pointed to by head */
void _concat_brm(TransportBRecvMessage *head, TransportBRecvMessage *addition) {
    TransportBRecvMessage *brmp = head;
    while (brmp->next) {
        brmp = brmp->next;
    }

    brmp->next = addition;
}

int _which_server(struct mdhim *md, void *key, int key_len)
{
    rangesrv_list *rl;
    rl = get_range_servers(md, md->p->primary_index, key, key_len);
    /* what is the difference between 'rank' and 'rangeserv_num' ? */
    return (rl->ri->rank);
}
