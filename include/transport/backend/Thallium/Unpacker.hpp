#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_UNPACKER_HPP
#define TRANSPORT_THALLIUM_UNPACKER_HPP

#include <sstream>

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
        static int unpack(Request::Request     **req,    const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Put         **pm,     const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Get         **gm,     const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Delete      **dm,     const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::Histogram   **hist,   const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BPut        **bpm,    const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BGet        **bgm,    const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BGetOp      **bgm,    const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BDelete     **bdm,    const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Request::BHistogram  **bhist,  const std::string &buf, FixedBufferPool *requests,  FixedBufferPool *arrays, FixedBufferPool *buffers);

        static int unpack(Response::Response   **res,    const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Put        **pm,     const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Get        **gm,     const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Delete     **dm,     const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::Histogram  **hist,   const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BPut       **bpm,    const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BGet       **bgm,    const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BGetOp     **bgm,    const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BDelete    **bdm,    const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);
        static int unpack(Response::BHistogram **bhist,  const std::string &buf, FixedBufferPool *responses, FixedBufferPool *arrays, FixedBufferPool *buffers);

    private:
        /** Allocates space for a temporary message and unpacks only the header */
        static int unpack(Message              **msg,    const std::string &buf, FixedBufferPool *fbp, FixedBufferPool *arrays,  FixedBufferPool *buffers);

        /** Unpacks the message header in a preallocated space */
        static int unpack(Message              *msg,     std::stringstream &s);
};

}
}

#endif

#endif
