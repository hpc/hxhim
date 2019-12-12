#ifndef HXHIM_TRANSPORT_MESSAGES_HPP
#define HXHIM_TRANSPORT_MESSAGES_HPP

#include "transport/messages/Blob.hpp"
#include "transport/messages/BPut.hpp"
#include "transport/messages/BGet.hpp"
#include "transport/messages/BGetOp.hpp"
#include "transport/messages/BDelete.hpp"

namespace Transport {
    namespace Request {
        // types to use when sending a request (client)
        typedef BPut   <Transport::ReferenceBlob> SendRequestBPut;
        typedef BGet   <Transport::ReferenceBlob> SendRequestBGet;
        typedef BGetOp <Transport::ReferenceBlob> SendRequestBGetOp;
        typedef BDelete<Transport::ReferenceBlob> SendRequestBDelete;

        // types to use when receiving a request (server)
        typedef BPut   <Transport::DeepCopyBlob>  RecvRequestBPut;
        typedef BGet   <Transport::DeepCopyBlob>  RecvRequestBGet;
        typedef BGetOp <Transport::DeepCopyBlob>  RecvRequestBGetOp;
        typedef BDelete<Transport::DeepCopyBlob>  RecvRequestBDelete;
    }

    namespace Response {
        // types to use when sending a response (server)
        typedef BPut   <Transport::DeepCopyBlob> SendRequestBPut;
        typedef BGet   <Transport::DeepCopyBlob> SendRequestBGet;
        typedef BGetOp <Transport::DeepCopyBlob> SendRequestBGetOp;
        typedef BDelete<Transport::DeepCopyBlob> SendRequestBDelete;

        // types to use when receiving a response (client)
        typedef BPut   <Transport::DeepCopyBlob> RecvRequestBPut;
        typedef BGet   <Transport::DeepCopyBlob> RecvRequestBGet;
        typedef BGetOp <Transport::DeepCopyBlob> RecvRequestBGetOp;
        typedef BDelete<Transport::DeepCopyBlob> RecvRequestBDelete;
    }
}

#endif
