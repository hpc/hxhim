#ifndef TRANSPORT_MESSAGES_HPP
#define TRANSPORT_MESSAGES_HPP

#include "transport/messages/Blob.hpp"
#include "transport/messages/BPut.hpp"
#include "transport/messages/BGet.hpp"
#include "transport/messages/BGetOp.hpp"
#include "transport/messages/BDelete.hpp"

namespace Transport {
    namespace Request {
        // types to use when sending a request (client)
        typedef BPut   <Transport::ReferenceBlob> SendBPut;
        typedef BGet   <Transport::ReferenceBlob> SendBGet;
        typedef BGetOp <Transport::ReferenceBlob> SendBGetOp;
        typedef BDelete<Transport::ReferenceBlob> SendBDelete;

        // types to use when receiving a request (server)
        typedef BPut   <Transport::DeepCopyBlob>  RecvBPut;
        typedef BGet   <Transport::DeepCopyBlob>  RecvBGet;
        typedef BGetOp <Transport::DeepCopyBlob>  RecvBGetOp;
        typedef BDelete<Transport::DeepCopyBlob>  RecvBDelete;
    }

    namespace Response {
        // types to use when sending a response (server)
        typedef BPut   <Transport::DeepCopyBlob> SendBPut;
        typedef BGet   <Transport::DeepCopyBlob> SendBGet;
        typedef BGetOp <Transport::DeepCopyBlob> SendBGetOp;
        typedef BDelete<Transport::DeepCopyBlob> SendBDelete;

        // types to use when receiving a response (client)
        typedef BPut   <Transport::DeepCopyBlob> RecvBPut;
        typedef BGet   <Transport::DeepCopyBlob> RecvBGet;
        typedef BGetOp <Transport::DeepCopyBlob> RecvBGetOp;
        typedef BDelete<Transport::DeepCopyBlob> RecvBDelete;
    }
}

#endif
