#include "utils/Stats.hpp"

long double elapsed(const struct Monostamp *duration) {
    long double start = duration->start.tv_nsec;
    start /= 1e9;
    start += duration->start.tv_sec;
    long double end = duration->end.tv_nsec;
    end /= 1e9;
    end += duration->end.tv_sec;

    return end - start;
}

Chronopoint now() {
    return Clock::now();
}
