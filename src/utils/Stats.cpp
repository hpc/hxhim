#include <sstream>

#include "utils/Stats.hpp"
#include "utils/mlogfacs2.h"

Stats::Chronopoint Stats::global_epoch = {};

Stats::Chronopoint Stats::init() {
    static bool global_epoch_initialized = false;
    if (!global_epoch_initialized) {
        Stats::global_epoch = Stats::now();
        global_epoch_initialized = true;
    }
    return Stats::global_epoch;
}

Stats::Chronopoint Stats::now() {
    return Stats::Clock::now();
}

uint64_t Stats::nano(const Stats::Chronopoint &start, const Stats::Chronopoint &end) {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
}

uint64_t Stats::nano(const Stats::Chronostamp &duration) {
    return Stats::nano(duration.start, duration.end);
}

long double Stats::sec(const Stats::Chronopoint &start, const Stats::Chronopoint &end) {
    return static_cast<long double>(Stats::nano(start, end)) / 1e9;
}

long double Stats::sec(const Stats::Chronostamp &duration) {
    return Stats::sec(duration.start, duration.end);
}

/**
 * print_event
 * Print a single timestamp since the epoch in the format
 * <rank> <name> <start - epoch (ns)>
 *
 * @param stream     the stream to print to
 * @param rank       the rank this is printing from
 * @param name       the name of the event
 * @param epoch      the epoch timestamps are counting from
 * @param timestamp  the singular timestamp to print
 * @return the stream
 */
std::ostream &Stats::print_event(std::ostream &stream,
                                 const int rank,
                                 const std::string &name,
                                 const Stats::Chronopoint &epoch,
                                 const Stats::Chronopoint &timestamp) {
    return stream << rank << " "
                  << name << " "
                  << Stats::nano(epoch, timestamp)
                  << std::endl;
}

/**
 * print_event
 * Print a timestamp pair in the format
 * <rank> <name> <start - epoch (ns)> <end - epoch (ns)> <end - start (sec)>
 *
 * @param stream     the stream to print to
 * @param rank       the rank this is printing from
 * @param name       the name of the event
 * @param epoch      the epoch timestamps are counting from
 * @param start      the starting timestamp
 * @param end        the ending timestamp
 * @return the stream
 */
std::ostream &Stats::print_event(std::ostream &stream,
                                 const int rank,
                                 const std::string &name,
                                 const Stats::Chronopoint &epoch,
                                 const Stats::Chronopoint &start,
                                 const Stats::Chronopoint &end) {
    return stream << rank << " "
                  << name << " "
                  << Stats::nano(epoch, start) << " "
                  << Stats::nano(epoch, end) << " "
                  << Stats::sec(start, end)
                  << std::endl;
}

/**
 * print_event
 * Print a timestamp pair in the format
 * <rank> <name> <start - epoch (ns)> <end - epoch (ns)> <end - start (sec)>
 *
 * @param stream     the stream to print to
 * @param rank       the rank this is printing from
 * @param name       the name of the event
 * @param epoch      the epoch timestamps are counting from
 * @param timestamp  the timestamp start/end pair
 * @return the stream
 */
std::ostream &Stats::print_event(std::ostream &stream,
                                 const int rank,
                                 const std::string &name,
                                 const Stats::Chronopoint &epoch,
                                 const Stats::Chronostamp &timestamp) {
    return Stats::print_event(stream, rank, name, epoch, timestamp.start, timestamp.end);
}

/**
 * print_event_to_log
 * Print a timestamp pair to mlog in the format
 * <rank> <name> <start - epoch (ns)> <end - epoch (ns)> <end - start (sec)>
 *
 * @param level      the mlog level
 * @param rank       the rank this is printing from
 * @param name       the name of the event
 * @param epoch      the epoch timestamps are counting from
 * @param start      the starting timestamp
 * @param end        the ending timestamp
 */
void Stats::print_event_to_mlog(const int level,
                                const int rank,
                                const std::string &name,
                                const Stats::Chronopoint &epoch,
                                const Stats::Chronopoint &start,
                                const Stats::Chronopoint &end) {
    std::stringstream s;
    print_event(s, rank, name, epoch, start, end);
    mlog(level, "\n%s", s.str().c_str());
}
