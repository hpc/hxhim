#ifndef MDHIM_TRANSPORT_THALLIUM_OPTIONS_HPP
#define MDHIM_TRANSPORT_THALLIUM_OPTIONS_HPP

#include <string>

#include <thallium.hpp>

#include "transport_options.hpp"

class ThalliumOptions : virtual public TransportOptions {
    public:
        ThalliumOptions(const std::string &protocol)
          : TransportOptions(MDHIM_TRANSPORT_THALLIUM),
            protocol_(protocol)
        {}

        const std::string protocol_;
};

#endif
