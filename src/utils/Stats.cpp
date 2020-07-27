#include "utils/Stats.hpp"

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
