#include "hxhim/Stats.hpp"
#include "utils/memory.hpp"

hxhim::Stats::Send::Send()
    : cached(),
      shuffled(),
      hashed(),
      find_dst(),
      bulked()
{}

hxhim::Stats::Send::~Send() {}
