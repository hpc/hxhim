#ifndef TRANSPORT_THALLIUM_PACKER_HPP
#define TRANSPORT_THALLIUM_PACKER_HPP

#include <sstream>

#include <thallium.hpp>

#include "transport/Messages.hpp"

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
        static int any (const Message            *msg,  std::string &buf);

        static int pack(const Request::Request   *req,  std::string &buf);
        static int pack(const Request::Put       *pm,   std::string &buf);
        static int pack(const Request::Get       *gm,   std::string &buf);
        static int pack(const Request::Delete    *dm,   std::string &buf);
        static int pack(const Request::BPut      *bpm,  std::string &buf);
        static int pack(const Request::BGet      *bgm,  std::string &buf);
        static int pack(const Request::BGetOp    *bgm,  std::string &buf);
        static int pack(const Request::BDelete   *bdm,  std::string &buf);

        static int pack(const Response::Response *res,  std::string &buf);
        static int pack(const Response::Put      *pm,   std::string &buf);
        static int pack(const Response::Get      *gm,   std::string &buf);
        static int pack(const Response::Delete   *dm,   std::string &buf);
        static int pack(const Response::BPut     *bpm,  std::string &buf);
        static int pack(const Response::BGet     *bgm,  std::string &buf);
        static int pack(const Response::BGetOp   *bgm,  std::string &buf);
        static int pack(const Response::BDelete  *bdm,  std::string &buf);

    private:
        static int pack(const Message            *msg, std::stringstream &s);
        static int pack(const Request::Request   *req, std::stringstream &s);
        static int pack(const Response::Response *res, std::stringstream &s);
};

}
}

#endif
