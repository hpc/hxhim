#ifndef HXHIM_STATS_H
#define HXHIM_STATS_H

#include "utils/Stats.h"

#ifdef __cplusplus
extern "C"
{
#endif
/** Timestamps of each request from the user's perspective */
/** Common across multiple sections of the code            */

/** Timestamps before calling transport layer */
struct Send {
    struct timespec cached;
    struct timespec shuffled;    /** the first time shuffle was called on this request       */
    struct Monostamp hashed;     /** how long hashing took                                   */
    struct timespec bulked;      /** when this request is finally placed in a message packet */
};

/** Transport timestamps */
struct SendRecv {
    struct Monostamp pack;
    struct timespec send_start;  /** send and recv might be one call */
    struct timespec recv_end;    /** send and recv might be one call */
    struct Monostamp unpack;
};

/** Timestamps of events after receiving responses */
struct Recv {
    struct Monostamp result;
};

#ifdef __cplusplus
}
#endif

#endif
