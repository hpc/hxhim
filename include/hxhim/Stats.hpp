#ifndef HXHIM_STATS_HPP
#define HXHIM_STATS_HPP

#include <chrono>
#include <map>
#include <list>
#include <iostream>

#include "transport/Messages/Message.hpp"

namespace hxhim {
namespace Stats {

struct Timestamp {
    std::chrono::time_point<std::chrono::high_resolution_clock> start;
    std::chrono::time_point<std::chrono::high_resolution_clock> end;
};

struct Stats {
    // how long each single operation called by the user took
    std::map<Transport::Message::Type, std::list<Timestamp> > single_op;

    // how long each bulk operation called by the user took
    std::map<Transport::Message::Type, std::list<Timestamp> > bulk_op;

    // how many entries of a message packet were used before sending
    // max number of entries per message should never change
    std::map<Transport::Message::Type, std::list<std::size_t> > used;

    // how long each transport took
    std::map<Transport::Message::Type, std::list <Timestamp> > transport;

    // distribution of outgoing packets
    std::map<Transport::Message::Type, std::map<int, std::size_t> > outgoing;

    // distribution of incoming packets
    std::map<Transport::Message::Type, std::map<int, std::size_t> > incoming;


    std::ostream &print(const std::map<Transport::Message::Type, std::size_t> &max_ops_per_send,
                        std::ostream &stream = std::cout,
                        const std::string &indent = "    ");

};

}
}

#endif
