#ifndef MDHIM_OPTIONS_STRUCT
#define MDHIM_OPTIONS_STRUCT

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct mdhim_options_private mdhim_options_private_t;

typedef struct mdhim_options {
    // ///////////////////////////////////////
    // MPI boostrap
    MPI_Comm comm;

    //The size of comm
    int size;

    //Rank within comm
    //Used as this instance's unique ID
    int rank;
    // ///////////////////////////////////////

    mdhim_options_private_t *p;
} mdhim_options_t;

#ifdef __cplusplus
}
#endif

#endif
