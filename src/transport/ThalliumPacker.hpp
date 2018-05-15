#ifndef MDHIM_TRANSPORT_THALLIUM_PACKER_HPP
#define MDHIM_TRANSPORT_THALLIUM_PACKER_HPP

#include <sstream>

#include <thallium.hpp>

#include "transport.hpp"

/**
 * ThalliumPacker
 * A collection of functions that pack
 * TransportMessages using thallium
 *
 * @param message pointer to the mesage that will be packed
 * @param buf     address of the new memory location where the message will be packed into
 * @param bufsize size of the new memory location
 */
class ThalliumPacker {
    public:
        static int any (const TransportMessage         *msg,  std::string &buf);

        static int pack(const TransportPutMessage      *pm,   std::string &buf);
        static int pack(const TransportBPutMessage     *bpm,  std::string &buf);
        static int pack(const TransportGetMessage      *gm,   std::string &buf);
        static int pack(const TransportBGetMessage     *bgm,  std::string &buf);
        static int pack(const TransportDeleteMessage   *dm,   std::string &buf);
        static int pack(const TransportBDeleteMessage  *bdm,  std::string &buf);
        static int pack(const TransportRecvMessage     *rm,   std::string &buf);
        static int pack(const TransportGetRecvMessage  *grm,  std::string &buf);
        static int pack(const TransportBGetRecvMessage *bgrm, std::string &buf);
        static int pack(const TransportBRecvMessage    *brm,  std::string &buf);

    private:
        /**
         * Common to all public functions
         */
        static int pack(const TransportMessage         *msg,  std::stringstream &s);
};

#endif
