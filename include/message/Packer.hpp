#ifndef MESSAGE_PACKER_HPP
#define MESSAGE_PACKER_HPP

#include "message/Messages.hpp"

namespace Message {

/**
 * Packer
 * A collection of functions that pack messages
 *
 * @param message pointer to the mesage that will be packed
 * @param buf     the string where the packed data will be placed into
 */
class Packer {
    public:
        static int pack(const Request::Request     *req,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BPut        *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BGet        *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BGetOp      *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BDelete     *bdm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BHistogram  *bhm,   void **buf, std::size_t *bufsize);

        static int pack(const Response::Response   *res,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BPut       *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BGet       *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BGetOp     *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BDelete    *bdm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BHistogram *bhm,   void **buf, std::size_t *bufsize);

    private:
        static int pack(const Message              *msg,   void **buf, std::size_t *bufsize, char **curr);
        static int pack(const Request::Request     *req,   void **buf, std::size_t *bufsize, char **curr);
        static int pack(const Response::Response   *res,   void **buf, std::size_t *bufsize, char **curr);
};

}

#endif
