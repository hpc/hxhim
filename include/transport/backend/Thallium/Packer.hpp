#if HXHIM_HAVE_THALLIUM

#ifndef TRANSPORT_THALLIUM_PACKER_HPP
#define TRANSPORT_THALLIUM_PACKER_HPP

#include "transport/Messages/Messages.hpp"

namespace Transport {
namespace Thallium {

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
        static int pack(const Request::Request     *req,   void **buf, std::size_t *bufsize);
        static int pack(const Request::Put         *pm,    void **buf, std::size_t *bufsize);
        static int pack(const Request::Get         *gm,    void **buf, std::size_t *bufsize);
        static int pack(const Request::Get2        *gm,    void **buf, std::size_t *bufsize);
        static int pack(const Request::Delete      *dm,    void **buf, std::size_t *bufsize);
        static int pack(const Request::Histogram   *hist,  void **buf, std::size_t *bufsize);
        static int pack(const Request::BPut        *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BGet        *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BGet2       *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BGetOp      *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BDelete     *bdm,   void **buf, std::size_t *bufsize);
        static int pack(const Request::BHistogram  *bhist, void **buf, std::size_t *bufsize);

        static int pack(const Response::Response   *res,   void **buf, std::size_t *bufsize);
        static int pack(const Response::Put        *pm,    void **buf, std::size_t *bufsize);
        static int pack(const Response::Get        *gm,    void **buf, std::size_t *bufsize);
        static int pack(const Response::Get2       *gm,    void **buf, std::size_t *bufsize);
        static int pack(const Response::Delete     *dm,    void **buf, std::size_t *bufsize);
        static int pack(const Response::Histogram  *hist,  void **buf, std::size_t *bufsize);
        static int pack(const Response::BPut       *bpm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BGet       *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BGet2      *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BGetOp     *bgm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BDelete    *bdm,   void **buf, std::size_t *bufsize);
        static int pack(const Response::BHistogram *bhist, void **buf, std::size_t *bufsize);

    private:
        static int pack(const Message              *msg,   void **buf, std::size_t *bufsize, char **curr);
        static int pack(const Request::Request     *req,   void **buf, std::size_t *bufsize, char **curr);
        static int pack(const Response::Response   *res,   void **buf, std::size_t *bufsize, char **curr);
};

}
}

#endif

#endif
