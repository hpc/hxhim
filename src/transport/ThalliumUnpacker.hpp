#ifndef MDHIM_TRANSPORT_THALLIUM_UNPACKER_HPP
#define MDHIM_TRANSPORT_THALLIUM_UNPACKER_HPP

#include <sstream>

#include <thallium.hpp>

#include "transport.hpp"

/**
 * ThalliumUnpacker
 * A collection of functions that unpack
 * formatted buffers into TransportMessages
 *
 * @param message address of the pointer that will be created and unpacked into
 * @param buf     the data to convert into the message
*/
class ThalliumUnpacker {
    public:
        static int any   (TransportMessage         **msg,  const std::string &buf);

        static int unpack(TransportRequestMessage  **req,  const std::string &buf);
        static int unpack(TransportPutMessage      **pm,   const std::string &buf);
        static int unpack(TransportBPutMessage     **bpm,  const std::string &buf);
        static int unpack(TransportGetMessage      **gm,   const std::string &buf);
        static int unpack(TransportBGetMessage     **bgm,  const std::string &buf);
        static int unpack(TransportDeleteMessage   **dm,   const std::string &buf);
        static int unpack(TransportBDeleteMessage  **bdm,  const std::string &buf);

        static int unpack(TransportResponseMessage **res,  const std::string &buf);
        static int unpack(TransportRecvMessage     **rm,   const std::string &buf);
        static int unpack(TransportGetRecvMessage  **grm,  const std::string &buf);
        static int unpack(TransportBGetRecvMessage **bgrm, const std::string &buf);
        static int unpack(TransportBRecvMessage    **brm,  const std::string &buf);

    private:
        /**
         * unpack TransportMessage *
         *
         * This function is only called by ThalliumUnpacker::any
         * to extract the mtype from the packed buffer.
         */
        static int unpack(TransportMessage         **msg,  const std::string &buf);

        /**
         * unpack TransportMessage *
         *
         * This function can only be called by the other functions.
         * It assumes that the message pointer is valid.
         */
        static int unpack(TransportMessage         *msg,   std::stringstream &s);

        /**
         * unpack TransportRequestMessage *
         *
         * This function is only be called by the functions that unpack buffers containing TransportRequestMessage messages
         */
        static int unpack(TransportRequestMessage  *req,   std::stringstream &s);

        /**
         * unpack TransportResponseMessage *
         *
         * This function is only be called by the functions that unpack buffers containing TransportResponseMessage messages
         */
        static int unpack(TransportResponseMessage *res,   std::stringstream &s);

        /**
         * unpack TransportRequestMessage *, given a type
         *
         * This function is only be called by ThalliumUnpacker::any and unpack TransportRequestMessage
         */
        static int unpack(TransportRequestMessage  **req,  const std::string &buf, const TransportMessageType mtype);

        /**
         * unpack TransportResponseMessage *, given a type
         *
         * This function is only be called by ThalliumUnpacker::any and unpack TransportResponseMessage
         */
        static int unpack(TransportResponseMessage **res,  const std::string &buf, const TransportMessageType mtype);
};

#endif
