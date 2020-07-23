#ifndef HXHIM_STATS_HPP
#define HXHIM_STATS_HPP

#include <deque>

#include "utils/Stats.hpp"

namespace hxhim {
namespace Stats {

/** Timestamps of each request from the user's perspective */
/** Common across multiple sections of the code            */

/** Timestamps before calling transport layer */
struct Send {
    struct timespec cached;
    struct timespec shuffled;                /** the first time shuffle was called on this request    */
    struct Monostamp hashed;                 /** how long hashing took (start should be close to      */
                                             /** shuffled timestamp                                   */
    std::deque<struct Monostamp> find_dsts;  /** time to figure out which packet this request goes to */
    struct Monostamp bulked;                 /** how long it took to place the request into a packet  */
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

}
}

#endif
