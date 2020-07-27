#ifndef STATS_HPP
#define STATS_HPP

#include <chrono>
#include <cstdint>
#include <deque>

namespace Stats {

using Clock = std::chrono::steady_clock; // monotonic
using Chronopoint = std::chrono::time_point<Clock>;

struct Chronostamp {
    Chronopoint start;
    Chronopoint end;
};

// get the current time since epoch
Chronopoint now();

uint64_t nano(const Chronopoint &start, const Chronopoint &end);
uint64_t nano(const Chronostamp &duration);

long double sec(const Chronopoint &start, const Chronopoint &end);
long double sec(const Chronostamp &duration);

// /////////////////////////////////////////////////////////

// Timestamps of each request from the user's perspective

// Timestamps before calling transport layer
struct Send {
    Chronopoint cached;
    Chronopoint shuffled;               // the first time shuffle was called on this request
    Chronostamp hashed;                 // how long hashing took (start should be close to
                                        // shuffled timestamp
    std::deque<Chronostamp> find_dsts;  // time to figure out which packet this request goes to
    Chronostamp bulked;                 // how long it took to place the request into a packet
                                        // This does not include the time between hashing and
                                        // failing to put the request into the packet
};

// Transport timestamps
struct SendRecv {
    Chronostamp pack;
    Chronopoint send_start;             // send and recv might be one call
    Chronopoint recv_end;               // send and recv might be one call
    Chronostamp unpack;
};

// Timestamps of events after receiving responses
struct Recv {
    Chronostamp result;
};

}

#endif
