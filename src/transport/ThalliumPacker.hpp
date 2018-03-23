#ifndef HXHIM_TRANSPORT_THALLIUM_PACKER
#define HXHIM_TRANSPORT_THALLIUM_PACKER

#include <sstream>

#include <thallium.hpp>

#include "transport.hpp"
#include "Packer.hpp"

/**
 * ThalliumPacker
 * A collection of functions that pack TransportMessages
 * using thallium
 *
 * @param message pointer to the mesage that will be packed
 * @param buf     address of the new memory location where the message will be packed into
 * @param bufsize size of the new memory location
 */
class ThalliumPacker : private Packer {
    public:
        static int any (const TransportMessage         *msg, std::string &buf);

        static int pack(const TransportPutMessage      *pm,  std::string &buf);
        static int pack(const TransportGetMessage      *gm,  std::string &buf);
        static int pack(const TransportRecvMessage     *rm,  std::string &buf);
        static int pack(const TransportGetRecvMessage  *grm, std::string &buf);

    private:
        /**
         * Common to all public functions
         */
        static int pack(const TransportMessage         *msg, std::stringstream &s);
};

#endif
