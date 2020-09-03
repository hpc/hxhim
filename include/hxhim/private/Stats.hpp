#ifndef HXHIM_PRIVATE_STATS_HPP
#define HXHIM_PRIVATE_STATS_HPP

#include <map>
#include <list>
#include <ostream>

#include "hxhim/constants.h"
#include "utils/Stats.hpp"

namespace hxhim {
namespace Stats {

// Events sorted by op type
struct Global {
    // how long each single operation called by the user took
    std::map<enum hxhim_op_t, std::list<::Stats::Chronostamp> > single_op;

    // how long each bulk operation called by the user took
    std::map<enum hxhim_op_t, std::list<::Stats::Chronostamp> > bulk_op;

    // how many entries of a message packet were used before sending
    // max number of entries per message should never change
    std::map<enum hxhim_op_t, std::list<std::size_t> > used;

    // how long each transport took
    std::map<enum hxhim_op_t, std::list <::Stats::Chronostamp> > transport;

    // distribution of outgoing packets
    std::map<enum hxhim_op_t, std::map<int, std::size_t> > outgoing;

    // distribution of incoming packets
    std::map<enum hxhim_op_t, std::map<int, std::size_t> > incoming;

    std::ostream &print(const int rank,
                        const std::size_t max_ops_per_send,
                        const ::Stats::Chronopoint epoch,
                        std::ostream &stream,
                        const std::string &indent = "    ");

};

}
}

#endif
