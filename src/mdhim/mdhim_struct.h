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
    MPI_Comm comm;
    pthread_mutex_t lock;

    //The size of comm
    int size;

    //Rank within comm
    //Used as this instance's unique ID
    int rank;
    // ///////////////////////////////////////

    mdhim_private_t *p;
} mdhim_t;

#ifdef __cplusplus
}
#endif
#endif
