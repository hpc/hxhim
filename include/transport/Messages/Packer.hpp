#ifndef TRANSPORT_PACKER_HPP
#define TRANSPORT_PACKER_HPP

#include "transport/Messages/Messages.hpp"
#include "utils/memory.hpp"

namespace Transport {

/**
 * Packer
 * A collection of functions that pack
 * Transport::Messages using thallium
 *
 * @param message pointer to the mesage that will be packed
 * @param buf     the string where the packed data will be placed into
 */
class Packer {
    public:
        static int pack(const Request::Request         *req,   void **buf, std::size_t *bufsize);
        static int pack(const Request::SendBPut        *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::SendBGet        *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::SendBGetOp      *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::SendBDelete     *bdm,   void **buf, std::size_t *bufsize);
        // static int pack(const Request::SendBHistogram  *bhist, void **buf, std::size_t *bufsize);

        static int pack(const Request::RecvBPut        *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::RecvBGet        *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::RecvBGetOp      *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::RecvBDelete     *bdm,   void **buf, std::size_t *bufsize);
        // static int pack(const Request::RecvBHistogram  *bhist, void **buf, std::size_t *bufsize);

        static int pack(const Response::Response       *res,   void **buf, std::size_t *bufsize);
        static int pack(const Response::SendBPut       *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::SendBGet       *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::SendBGetOp     *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::SendBDelete    *bdm,   void **buf, std::size_t *bufsize);
        // static int pack(const Response::SendBHistogram *bhist, void **buf, std::size_t *bufsize);

        static int pack(const Response::RecvBPut       *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::RecvBGet       *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::RecvBGetOp     *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::RecvBDelete    *bdm,   void **buf, std::size_t *bufsize);
        // static int pack(const Response::RecvBHistogram *bhist, void **buf, std::size_t *bufsize);

    private:
        static int pack(const Message              *msg,   void **buf, std::size_t *bufsize, char **curr);
        static int pack(const Request::Request     *req,   void **buf, std::size_t *bufsize, char **curr);
        static int pack(const Response::Response   *res,   void **buf, std::size_t *bufsize, char **curr);
};

}

#endif
