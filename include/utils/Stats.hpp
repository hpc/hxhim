#ifndef STATS_HPP
#define STATS_HPP

#include <chrono>

#include "utils/Stats.h"

long double elapsed(const struct timespec *start, const struct timespec *end);

using Clock = std::chrono::steady_clock;
using Chronopoint = std::chrono::time_point<Clock>;

struct Chronostamp {
    Chronopoint start;
    Chronopoint end;
};

/** get the current time since epoch */
Chronopoint now();

template <typename Duration>
long double elapsed(const struct Chronostamp &duration) {
    return static_cast<long double>(std::chrono::duration_cast<Duration>(duration.end - duration.start).count());
}

#endif
