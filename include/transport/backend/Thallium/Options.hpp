#ifndef TRANSPORT_THALLIUM_OPTIONS_HPP
#define TRANSPORT_THALLIUM_OPTIONS_HPP

#include <string>

#include <thallium.hpp>

#include "transport/constants.h"
#include "transport/options.hpp"

namespace Transport {
namespace Thallium {

struct Options : ::Transport::Options {
    Options(const std::string &module)
        : ::Transport::Options(TRANSPORT_THALLIUM),
          module(module)
    {}

    const std::string module;
};

}
}

#endif
