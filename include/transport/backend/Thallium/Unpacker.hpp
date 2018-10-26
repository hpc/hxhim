#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_UNPACKER_HPP
#define TRANSPORT_THALLIUM_UNPACKER_HPP

#include "transport/Messages/Messages.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace Thallium {

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
        static int unpack(Request::Request     **req,    void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Put         **pm,     void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Get         **gm,     void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Delete      **dm,     void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Histogram   **hist,   void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BPut        **bpm,    void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BGet        **bgm,    void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BGetOp      **bgm,    void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BDelete     **bdm,    void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BHistogram  **bhist,  void *buf, const std::size_t bufsize, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);

        static int unpack(Response::Response   **res,    void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Put        **pm,     void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Get        **gm,     void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Delete     **dm,     void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Histogram  **hist,   void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BPut       **bpm,    void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BGet       **bgm,    void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BGetOp     **bgm,    void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BDelete    **bdm,    void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BHistogram **bhist,  void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);

    private:
        /** Allocates space for a temporary message and unpacks only the header */
        static int unpack(Message              **msg,    void *buf, const std::size_t bufsize, FixedBufferPool *responses, FixedBufferPool *arrays,  FixedBufferPool *buffers);

        /** Unpacks the message header in a preallocated space */
        static int unpack(Message              *msg,     void *buf, const std::size_t bufsize, char **curr);
};

}
}

#endif

#endif
