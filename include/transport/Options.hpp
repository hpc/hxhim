#ifndef TRANSPORT_OPTIONS_HPP
#define TRANSPORT_OPTIONS_HPP

#include "transport/constants.hpp"

namespace Transport {

class Options {
    public:
        Options(const Type type)
          : type(type)
        {}

        virtual ~Options() {}

        const Type type;
};

}

#endif
