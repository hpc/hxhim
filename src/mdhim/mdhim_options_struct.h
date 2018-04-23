#ifndef MDHIM_OPTIONS_STRUCT
#define MDHIM_OPTIONS_STRUCT

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct mdhim_options_private mdhim_options_private_t;

typedef struct mdhim_options {
    mdhim_options_private_t *p;
} mdhim_options_t;

#ifdef __cplusplus
}
#endif

#endif
