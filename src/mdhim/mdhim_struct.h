#ifndef MDHIM_STRUCT
#define MDHIM_STRUCT

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Struct that contains the private details about MDHim's implementation
 */
typedef struct mdhim_private mdhim_private_t;

/*
 * mdhim data
 * Contains an opaque pointer to the actual implementation
 */
typedef struct mdhim {
    // ///////////////////////////////////////
    // MPI boostrap
    MPI_Comm mdhim_comm;
    pthread_mutex_t mdhim_comm_lock;

    //The size of mdhim_comm
    int mdhim_comm_size;

    //Rank within mdhim_comm
    //Used as this instance's unique ID
    int mdhim_rank;
    // ///////////////////////////////////////

    mdhim_private_t *p;
} mdhim_t;

#ifdef __cplusplus
}
#endif
#endif
