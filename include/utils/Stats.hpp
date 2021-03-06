#ifndef STATS_HPP
#define STATS_HPP

#include <chrono>
#include <cstdint>
#include <sstream>
#include <string>

namespace Stats {

using Clock = std::chrono::steady_clock; // monotonic
using Chronopoint = std::chrono::time_point<Clock>;

extern Chronopoint global_epoch;
Chronopoint init();

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
    Chronostamp hash;        // how long hashing took (start should be close to
                             // shuffled timestamp
    Chronostamp insert;      // how long it took to [alloc and] insert into the packet
};

// Transport timestamps
struct SendRecv {
    Chronopoint start;
    Chronostamp pack;
    Chronopoint send_start;  // send and recv might be one call
    Chronopoint recv_end;    // send and recv might be one call
    Chronostamp unpack;
    Chronostamp cleanup_rpc;
    Chronopoint end;
};

// Timestamps of events after receiving responses
struct Recv {
    Chronostamp result;
};

// print timestamps in the format of
// <rank> <name> <start - epoch (ns)>
std::ostream &print_event(std::ostream &stream,
                          const int rank,
                          const std::string &name,
                          const Chronopoint &epoch,
                          const Chronopoint &timestamp);

// print timestamps in the format of
// <rank> <name> <start - epoch (ns)> <end - epoch (ns)> <end - start (sec)>
std::ostream &print_event(std::ostream &stream,
                          const int rank,
                          const std::string &name,
                          const Chronopoint &epoch,
                          const Chronopoint &start,
                          const Chronopoint &end);

/**
 * print_event
 * Print a timestamp pair in the format
 * <rank> <name> <start - epoch (ns)> <end - epoch (ns)> <end - start (sec)>
 *
 * @param stream     the stream to print to
 * @param rank       the rank this is printing from
 * @param name       the name of the event
 * @param epoch      the epoch timestamps are counting from
 * @param timestamp  a structure with Chronopoints start and end
 * @return the stream
 */
template <typename T>
std::ostream &print_event(std::ostream &stream,
                          const int rank,
                          const std::string &name,
                          const Stats::Chronopoint &epoch,
                          const T &timestamp) {
    return print_event(stream, rank, name, epoch, timestamp.start, timestamp.end);
}

void print_event_to_mlog(const int level,
                         const int rank,
                         const std::string &name,
                         const Chronopoint &epoch,
                         const Chronopoint &start,
                         const Chronopoint &end);

}

#endif
