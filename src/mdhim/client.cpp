/*
 * MDHIM TNG
 *
 * Client specific implementation for sending to range server that is not yourself
 */

#include <cstdlib>

#include "client.h"
#include "mdhim_private.h"
#include "transport_private.hpp"
#include "transport_mpi.hpp"

/**
 * Send put to range server
 *
 * @param md main MDHIM struct
 * @param pm pointer to put message to be sent or inserted into the range server's work queue
 * @return return_message structure with error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportRecvMessage *client_put(mdhim_t *md, TransportPutMessage *pm) {
    if (!md || !pm) {
        return nullptr;
    }

    // do put
    if (md->p->transport->AddPutRequest(pm) != MDHIM_SUCCESS) {
        return nullptr;
    }

    // wait for result
    TransportRecvMessage *rm = nullptr;
    md->p->transport->AddPutReply(pm->dst, &rm);

    return rm;
}

/**
 * Send get to range server
 *
 * @param md main MDHIM struct
 * @param gm pointer to get message to be sent or inserted into the range server's work queue
 * @return return_message structure with error = MDHIM_SUCCESS or MDHIM_ERROR
 */
TransportGetRecvMessage *client_get(mdhim_t *md, TransportGetMessage *gm) {
    if (!md || !gm) {
        return nullptr;
    }

    // do get
    if (md->p->transport->AddGetRequest(gm) != MDHIM_SUCCESS) {
        return nullptr;
    }

    // wait for result
    TransportGetRecvMessage *grm = nullptr;
    md->p->transport->AddGetReply(gm->dst, &grm);

    return grm;
}

// /**
//  * Send bulk put to range server
//  *
//  * @param md main MDHIM struct
//  * @param bpm_list double pointer to an array of bulk put messages
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// TransportBRecvMessage *client_bput(mdhim_t *md, index_t *index, TransportBPutMessage **bpm_list) {
//     int num_srvs = 0;
//     TransportAddress *srvs = new MPIAddress[index->num_rangesrvs]();
//     for (int i = 0; i < index->num_rangesrvs; i++) {
//         if (!bpm_list[i]) {
//             continue;
//         }

//         ((MPIAddress)srvs[num_srvs]).SetRank(bpm_list[i]->dst);
//         num_srvs++;
//     }

//     if (!num_srvs) {
//         delete [] srvs;
//         return nullptr;
//     }

//     // send all puts out
//     if (md->p->transport->EndpointGroup()->AddBPutRequest(bpm_list, num_srvs) != MDHIM_SUCCESS) {
//         // TODO: probably should not return here
//         delete [] srvs;
//         return nullptr;
//     }
//     // wait for all responses
//     TransportRecvMessage **rm_list = new TransportRecvMessage *[num_srvs]();
//     if (md->p->transport->EndpointGroup()->AddBPutReply(srvs, num_srvs, rm_list) != MDHIM_SUCCESS) {
//         // TODO: probably should not return here
//         delete [] srvs;
//         return nullptr;
//     }

//     TransportBRecvMessage *brm_head = nullptr;
//     TransportBRecvMessage *brm_tail = nullptr;
//     for (int i = 0; i < num_srvs; i++) {
//         TransportRecvMessage *rm = rm_list[i];
//         if (!rm) {
//             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
//                  "Error: did not receive a response message in client_bput",
//                  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//             //Skip this as the message doesn't exist
//             continue;
//         }

//         TransportBRecvMessage *brm = new TransportBRecvMessage();
//         brm->error = rm->error;
//         brm->mtype = rm->mtype;
//         brm->dst = rm->dst;
//         free(rm);

//         //Build the linked list to return
//         brm->next = nullptr;
//         if (!brm_head) {
//             brm_head = brm;
//             brm_tail = brm;
//         } else {
//             brm_tail->next = brm;
//             brm_tail = brm;
//         }
//     }

//     delete [] rm_list;
//     delete [] srvs;

//     // Return response message
//     return brm_head;
// }

// /** Send bulk get to range server
//  *
//  * @param md main MDHIM struct
//  * @param bgm_list double pointer to an array or bulk get messages
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// TransportBGetRecvMessage *client_bget(mdhim_t *md, index_t *index,
//                                       TransportBGetMessage **bgm_list) {
//     int return_code;
//     int num_srvs = 0;

//     TransportAddress *srvs = new MPIAddress[index->num_rangesrvs]();
//     for (int i = 0; i < index->num_rangesrvs; i++) {
//         if (bgm_list[i]) {
//             dynamic_cast<MPIAddress *>(srvs)[num_srvs].SetRank(bgm_list[i]->dst);
//             num_srvs++;
//         }
//     }

//     if (!num_srvs) {
//         delete [] srvs;
//         return nullptr;
//     }

//     // Request data from a range server
//     if (md->p->transport->EndpointGroup()->AddBGetRequest(bgm_list, num_srvs) != MDHIM_SUCCESS) {
//         delete [] srvs;
//         return nullptr;
//     }

//     // Wait for data from the range server
//     TransportBGetRecvMessage **bgrm_list = new TransportBGetRecvMessage*[num_srvs]();
//     if (md->p->transport->EndpointGroup()->AddBGetReply(srvs, num_srvs, bgrm_list) != MDHIM_SUCCESS) {
//         delete [] srvs;
//         for(int i = 0; i < num_srvs; i++) {
//             delete bgrm_list[i];
//         }

//         delete [] bgrm_list;
//         return nullptr;
//     }

//     // create a linked list of all responses received for returning
//     TransportBGetRecvMessage *bgrm_head = nullptr;
//     TransportBGetRecvMessage *bgrm_tail = nullptr;
//     for (int i = 0; i < num_srvs; i++) {
//         TransportBGetRecvMessage *bgrm = bgrm_list[i];
//         if (!bgrm) {
//             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
//                  "Error: did not receive a response message in client_bget",
//                  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//             //Skip this as the message doesn't exist
//             continue;
//         }
//          //Build the linked list to return
//         bgrm->next = nullptr;
//         if (!bgrm_head) {
//             bgrm_head = bgrm;
//             bgrm_tail = bgrm;
//         } else {
//             bgrm_tail->next = bgrm;
//             bgrm_tail = bgrm;
//         }
//     }

//     delete [] bgrm_list;
//     delete [] srvs;

//     // Return response message
//     return bgrm_head;
// }

// /** Send get to range server with an op and number of records greater than one
//  *
//  * @param md main MDHIM struct
//  * @param gm pointer to get message to be sent or inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// struct mdhim_bgetrm_t *client_bget_op(mdhim_t *md, struct mdhim_getm_t *gm) {

//     int return_code;
//     struct mdhim_bgetrm_t *brm;

//     return_code = send_rangesrv_work(md, gm->basem.dst, gm);
//     // If the send did not succeed then log the error code and return MDHIM_ERROR
//     if (return_code != MDHIM_SUCCESS) {
//         mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - Error: %d from server while sending "
//              "get record request",  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
//         return nullptr;
//     }

//     return_code = receive_client_response(md, gm->basem.dst, (void **) &brm);
//     // If the receive did not succeed then log the error code and return MDHIM_ERROR
//     if (return_code != MDHIM_SUCCESS) {
//         mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - Error: %d from server while receiving "
//              "get record request",  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
//         brm->error = MDHIM_ERROR;
//     }

//     // Return response message
//     return brm;
// }

// /**
//  * Send delete to range server
//  *
//  * @param md main MDHIM struct
//  * @param dm pointer to del message to be sent or inserted into the range server's work queue
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// struct mdhim_rm_t *client_delete(mdhim_t *md, struct mdhim_delm_t *dm) {

//     int return_code;
//     struct mdhim_rm_t *rm;

//     return_code = send_rangesrv_work(md, dm->basem.dst, dm);
//     // If the send did not succeed then log the error code and return MDHIM_ERROR
//     if (return_code != MDHIM_SUCCESS) {
//         mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - Error: %d from server while sending "
//              "delete record request",  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
//         return nullptr;
//     }

//     return_code = receive_client_response(md, dm->basem.dst, (void **) &rm);
//     // If the receive did not succeed then log the error code and return MDHIM_ERROR
//     if (return_code != MDHIM_SUCCESS) {
//         mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - Error: %d from server while receiving "
//              "delete record request",  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
//         rm->error = MDHIM_ERROR;
//     }

//     // Return response
//     return rm;
// }

// /**
//  * Send bulk delete to range server
//  *
//  * @param md main MDHIM struct
//  * @param bdm_list double pointer to an array of bulk del messages
//  * @return return_message structure with ->error = MDHIM_SUCCESS or MDHIM_ERROR
//  */
// struct mdhim_brm_t *client_bdelete(mdhim_t *md, index_t *index,
//                                    struct mdhim_bdelm_t **bdm_list) {
//     int return_code;
//     struct mdhim_brm_t *brm_head, *brm_tail, *brm;
//     struct mdhim_rm_t **rm_list, *rm;
//     int i;
//     int *srvs;
//     int num_srvs;

//     num_srvs = 0;
//     srvs = (int*)malloc(sizeof(int) * index->num_rangesrvs);
//     for (i = 0; i < index->num_rangesrvs; i++) {
//         if (!bdm_list[i]) {
//             continue;
//         }

//         srvs[num_srvs] = bdm_list[i]->basem.dst;
//         num_srvs++;
//     }

//     return_code = send_all_rangesrv_work(md, (void **) bdm_list, index->num_rangesrvs);
//     // If the send did not succeed then log the error code and return MDHIM_ERROR
//     if (return_code != MDHIM_SUCCESS) {
//         mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - Error: %d from server while sending "
//              "bdel record request",  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);

//         return nullptr;
//     }

//     rm_list = (mdhim_rm_t**)malloc(sizeof(struct mdhim_rm_t *) * num_srvs);
//     memset(rm_list, 0, sizeof(struct mdhim_rm_t *) * num_srvs);
//     return_code = receive_all_client_responses(md, srvs, num_srvs, (void ***) &rm_list);
//     // If the receives did not succeed then log the error code and return MDHIM_ERROR
//     if (return_code != MDHIM_SUCCESS) {
//         mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - Error: %d from server while receiving "
//              "bdel record requests",  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str(), return_code);
//     }

//     brm_head = brm_tail = nullptr;
//     for (i = 0; i < num_srvs; i++) {
//         rm = rm_list[i];
//         if (!rm) {
//             mlog(MDHIM_CLIENT_CRIT, "MDHIM Rank %s - "
//                  "Error: did not receive a response message in client_bdel",
//                  ((std::string) (*md->p->transport->Endpoint()->Address())).c_str());
//             //Skip this as the message doesn't exist
//             continue;
//         }

//         brm = (mdhim_brm_t*)malloc(sizeof(struct mdhim_brm_t));
//         brm->error = rm->error;
//         brm->basem.mtype = rm->basem.mtype;
//         brm->basem.dst = rm->basem.dst;
//         free(rm);

//         //Build the linked list to return
//         brm->next = nullptr;
//         if (!brm_head) {
//             brm_head = brm;
//             brm_tail = brm;
//         } else {
//             brm_tail->next = brm;
//             brm_tail = brm;
//         }
//     }

//     free(rm_list);
//     free(srvs);

//     // Return response message
//     return brm_head;
// }
