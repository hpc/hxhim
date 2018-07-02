#ifndef MDHIM_TRANSPORT_OPTIONS_PRIVATE_HPP
#define MDHIM_TRANSPORT_OPTIONS_PRIVATE_HPP

#include "mdhim/constants.h"

class TransportOptions {
    public:
        TransportOptions(const int type)
          : type_(type)
        {}

        virtual ~TransportOptions() {}

        const int type_;
};

#endif
