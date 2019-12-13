#ifndef TRANSPORT_UNPACKER_HPP
#define TRANSPORT_UNPACKER_HPP

#include "transport/Messages/Messages.hpp"
#include "utils/memory.hpp"

namespace Transport {

/**
 * Unpacker
 * A collection of functions that unpack
 * formatted buffers into Transport::Messages
 *
 * @param message address of the pointer that will be created and unpacked into
 * @param buf     the data to convert into the message
*/
class Unpacker {
    public:
        static int unpack(Request::Request     **req,    void *buf, const std::size_t bufsize);
        static int unpack(Request::SendBPut        **bpm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::SendBGet        **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::SendBGetOp      **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::SendBDelete     **bdm,    void *buf, const std::size_t bufsize);
        // static int unpack(Request::SendBHistogram  **bhist,  void *buf, const std::size_t bufsize);

        static int unpack(Request::RecvBPut        **bpm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::RecvBGet        **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::RecvBGetOp      **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Request::RecvBDelete     **bdm,    void *buf, const std::size_t bufsize);
        // static int unpack(Request::RecvBHistogram  **bhist,  void *buf, const std::size_t bufsize);

        static int unpack(Response::Response   **res,    void *buf, const std::size_t bufsize);
        static int unpack(Response::SendBPut       **bpm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::SendBGet       **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::SendBGetOp     **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::SendBDelete    **bdm,    void *buf, const std::size_t bufsize);
        // static int unpack(Response::SendBHistogram **bhist,  void *buf, const std::size_t bufsize);

        static int unpack(Response::RecvBPut       **bpm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::RecvBGet       **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::RecvBGetOp     **bgm,    void *buf, const std::size_t bufsize);
        static int unpack(Response::RecvBDelete    **bdm,    void *buf, const std::size_t bufsize);
        // static int unpack(Response::RecvBHistogram **bhist,  void *buf, const std::size_t bufsize);

    private:
        /** Allocates space for a temporary message and unpacks only the header */
        static int unpack(Message              **msg,    void *buf, const std::size_t bufsize);

        /** Unpacks the message header in a preallocated space */
        static int unpack(Message              *msg,     void *buf, const std::size_t bufsize, char **curr);
};

}

#endif
