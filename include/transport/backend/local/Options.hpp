#ifndef TRANSPORT_LOCAL_OPTIONS_HPP
#define TRANSPORT_LOCAL_OPTIONS_HPP

#include <cstddef>

#include "transport/constants.hpp"
#include "transport/options.hpp"

namespace Transport {
namespace local {

struct Options : ::Transport::Options {
    Options()
        : ::Transport::Options(TRANSPORT_LOCAL)
    {}
};

}
}

#endif
