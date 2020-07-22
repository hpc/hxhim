#include "utils/Stats.hpp"

long double elapsed(const struct Monostamp *duration) {
    return elapsed2(&duration->start, &duration->end);
}

long double elapsed2(const struct timespec *start, const struct timespec *end) {
    return ((long double) nano2(start, end)) / 1e9;
}

uint64_t nano(const struct Monostamp *duration) {
    return nano2(&duration->start, &duration->end);
}

uint64_t nano2(const struct timespec *start, const struct timespec *end) {
    uint64_t s = start->tv_sec;
    s *= 1000000000;
    s += start->tv_nsec;

    uint64_t e = end->tv_sec;
    e *= 1000000000;
    e += end->tv_nsec;

    return e - s;
}

Chronopoint now() {
    return Clock::now();
}
