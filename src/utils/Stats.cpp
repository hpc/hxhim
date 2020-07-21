#include "utils/Stats.hpp"

long double elapsed(const struct Monostamp *duration) {
    return elapsed2(&duration->start, &duration->end);
}

long double elapsed2(const struct timespec *start, const struct timespec *end) {
    long double s = start->tv_nsec;
    s /= 1e9;
    s += start->tv_sec;
    long double e = end->tv_nsec;
    e /= 1e9;
    e += end->tv_sec;

    return e - s;
}

Chronopoint now() {
    return Clock::now();
}
