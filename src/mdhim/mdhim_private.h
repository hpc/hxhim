#ifndef MDHIM_PRIVATE_H
#define MDHIM_PRIVATE_H
#include "mdhim.h"
#include "comm.h"
#include "comm_mpi.h"

/**
 * Struct that contains the private details about MDHim's implementation
 */
struct mdhim_private {
    CommTransport *comm;
};
typedef struct mdhim_private mdhim_private_t;

/**
 *
 * @param mdp An allocated MDHim private data structure
 * @param dstype The data store type to instantiate
 * @param commtype The communication type to instantiate
 * @return 0 on success, non-zero on failre
 */
int mdhim_private_init(struct mdhim_private* mdp, int dstype, int commtype);

struct mdhim_rm_t *_put_record(struct mdhim *md, struct index_t *index,
			       void *key, int key_len, 
			       void *value, int value_len);
struct mdhim_brm_t *_create_brm(struct mdhim_rm_t *rm);
void _concat_brm(struct mdhim_brm_t *head, struct mdhim_brm_t *addition);
struct mdhim_brm_t *_bput_records(struct mdhim *md, struct index_t *index,
				  void **keys, int *key_lens, 
				  void **values, int *value_lens, int num_records);
struct mdhim_bgetrm_t *_bget_records(struct mdhim *md, struct index_t *index,
				     void **keys, int *key_lens, 
				     int num_keys, int num_records, int op);
struct mdhim_brm_t *_bdel_records(struct mdhim *md, struct index_t *index,
				  void **keys, int *key_lens,
				  int num_records);

#endif