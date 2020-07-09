#include <iomanip>

#include "hxhim/Stats.hpp"

std::ostream &hxhim::Stats::Stats::print(const std::map<Transport::Message::Type, std::size_t> &max_ops_per_send,

                                         std::ostream &stream,
                                         const std::string &indent) {
    std::ios_base::fmtflags flags(stream.flags());

    stream << std::fixed << std::setprecision(3);
    {
        stream << indent << "Single Ops:" << std::endl;

        std::size_t total_ops = 0;
        std::chrono::nanoseconds total_time(0);
        for(typename decltype(single_op)::const_iterator it = single_op.begin();
            it != single_op.end(); it++) {
            std::chrono::nanoseconds op_time(0);
            for(Timestamp const &event : it->second) {
                op_time += std::chrono::duration_cast<std::chrono::nanoseconds>(event.end - event.start);
            }
            stream << indent << indent << Transport::Message::TypeStr[it->first] << " count: " << it->second.size() << " duration: "  << std::chrono::duration<long double>(op_time).count() << " seconds" << std::endl;

            total_ops += it->second.size();
            total_time += op_time;
        }
    }

    {
        stream << indent << "Bulk Ops:" << std::endl;

        std::size_t total_ops = 0;
        std::chrono::nanoseconds total_time(0);
        for(typename decltype(bulk_op)::const_iterator it = bulk_op.begin();
            it != bulk_op.end(); it++) {
            std::chrono::nanoseconds op_time(0);
            for(Timestamp const &event : it->second) {
                op_time += std::chrono::duration_cast<std::chrono::nanoseconds>(event.end - event.start);
            }
            stream << indent << indent << Transport::Message::TypeStr[it->first] << " count: " << it->second.size() << " duration: "  << std::chrono::duration<long double>(op_time).count() << " seconds" << std::endl;

            total_ops += it->second.size();
            total_time += op_time;
        }
    }

    {
        stream << indent << "Average Message Packet Fill Rate:" << std::endl;

        for(typename decltype(used)::const_iterator it = used.begin();
            it != used.end(); it++) {
            std::size_t sum = 0;
            for(std::size_t const &filled : it->second) {
                sum += filled;
            }
            const double avg = ((double) sum) / it->second.size();
            sum /= used.size();
            stream << indent << indent << Transport::Message::TypeStr[it->first] << " count: " << it->second.size() << " average filled: "  << avg << "/" << max_ops_per_send.at(it->first) << " (" << 100.0 * avg / max_ops_per_send.at(it->first) << "%)" << std::endl;
        }
    }

    {
        stream << indent << "Outgoing:" << std::endl;

        for(typename decltype(outgoing)::const_iterator type = outgoing.begin();
            type != outgoing.end(); type++) {
            for(typename decltype(type->second)::const_iterator dst = type->second.begin();
                dst != type->second.end(); dst++) {
                stream << indent << indent << Transport::Message::TypeStr[type->first] << " -> " << dst->first << ": " << dst->second << std::endl;
            }
        }
    }

    stream.flags(flags);
    return stream;
}
