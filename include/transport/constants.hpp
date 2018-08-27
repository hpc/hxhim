#ifndef MDHIM_TRANSPORT_CONSTANTS_H
#define MDHIM_TRANSPORT_CONSTANTS_H

#define TRANSPORT_SUCCESS 0
#define TRANSPORT_ERROR   1

namespace Transport {

enum Type {
    TRANSPORT_MPI,

    #if HXHIM_HAVE_THALLIUM
    TRANSPORT_THALLIUM,
    #endif
};

}

#endif
