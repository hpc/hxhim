#ifndef MDHIM_TRANSPORT_CONSTANTS_H
#define MDHIM_TRANSPORT_CONSTANTS_H

#define TRANSPORT_SUCCESS 5
#define TRANSPORT_ERROR   6

namespace Transport {

enum Type {
    TRANSPORT_NULL,
    TRANSPORT_MPI,

    #if HXHIM_HAVE_THALLIUM
    TRANSPORT_THALLIUM,
    #endif

};

}

#endif
