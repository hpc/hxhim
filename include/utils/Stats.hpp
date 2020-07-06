#ifndef STATS_HPP
#define STATS_HPP

#include <chrono>

using Clock = std::chrono::steady_clock;
using Timepoint = std::chrono::time_point<Clock>;

struct Timestamp {
    Timepoint start;
    Timepoint end;
};

// get the current time since epoch
Timepoint now();

template <typename Duration>
long double elapsed(const Timestamp &time) {
    return static_cast<long double>(std::chrono::duration_cast<Duration>(time.end - time.start).count());
}

#endif
