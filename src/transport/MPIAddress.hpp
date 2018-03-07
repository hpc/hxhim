#ifndef HXHIM_TRANSPORT_MPI_ADDRESS
#define HXHIM_TRANSPORT_MPI_ADDRESS

#include <sstream>

#include "mlog2.h"
#include "mlogfacs2.h"

#include "mdhim_constants.h"
#include "transport.hpp"

/**
 * MPIAddress
 * Class for extracting a MPI rank
 */
class MPIAddress: virtual public TransportAddress {
    public:
        MPIAddress(const int rank = -1);

        operator std::string() const;
        operator int() const;

        int SetRank(const int rank);
        int Rank() const;

    private:
        int rank_;
};

#endif
