#ifndef TRANSPORT_THALLIUM_OPTIONS_HPP
#define TRANSPORT_THALLIUM_OPTIONS_HPP

#include <string>

#include "transport/constants.hpp"
#include "transport/Options.hpp"

namespace Transport {
namespace Thallium {

struct Options : ::Transport::Options {
    Options(const std::string &module, const int thread_count)
        : ::Transport::Options(TRANSPORT_THALLIUM),
          module(module),
          thread_count(thread_count)
    {}

    const std::string module;
    const int thread_count;
};

}
}

#endif
