#ifndef TRANSPORT_LOCAL_RANGE_SERVER_HPP
#define TRANSPORT_LOCAL_RANGE_SERVER_HPP

#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace local {

class RangeServer {
    public:
        static int init();
        static void destroy();

        /**
         * Function that will be used to listen for MPI messages
         */
        static void *listener_thread(void *data);

    private:
};

}
}

#endif
