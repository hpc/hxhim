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
    struct timespec shuffled;    /** the first time shuffle was called on this request    */
    struct Monostamp hashed;     /** how long hashing took                                */
    long double find_dst;        /** time to figure out which packet this request goes to */
                                 /** individual timestamps are not kept, since this can   */
                                 /** happen multiple times                                */
    struct Monostamp bulked;     /** how long it took to place the request into a packet  */
                                 /** This does not include the time between hashing and   */
                                 /** failing to put the request into the packet           */
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
