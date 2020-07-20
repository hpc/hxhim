#ifndef HXHIM_STATS_HPP
#define HXHIM_STATS_HPP

#include <map>
#include <list>
#include <ostream>

#include "transport/Messages/Message.hpp"
#include "utils/Stats.hpp"

namespace hxhim {
namespace Stats {

struct Global {
    // how long each single operation called by the user took
    std::map<Transport::Message::Type, std::list<Chronostamp> > single_op;

    // how long each bulk operation called by the user took
    std::map<Transport::Message::Type, std::list<Chronostamp> > bulk_op;

    // how many entries of a message packet were used before sending
    // max number of entries per message should never change
    std::map<Transport::Message::Type, std::list<std::size_t> > used;

    // how long each transport took
    std::map<Transport::Message::Type, std::list <Chronostamp> > transport;

    // distribution of outgoing packets
    std::map<Transport::Message::Type, std::map<int, std::size_t> > outgoing;

    // distribution of incoming packets
    std::map<Transport::Message::Type, std::map<int, std::size_t> > incoming;

    std::ostream &print(const std::map<Transport::Message::Type, std::size_t> &max_ops_per_send,
                        std::ostream &stream,
                        const std::string &indent = "    ");

};

}
}

#endif
