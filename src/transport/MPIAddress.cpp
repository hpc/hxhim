#include "MPIAddress.hpp"

MPIAddress::MPIAddress(const int rank)
  : rank_(rank)
{}

MPIAddress::operator std::string() const {
    std::stringstream s;
    s << rank_;
    return s.str();
}

MPIAddress::operator int() const {
    return rank_;
}

int MPIAddress::SetRank(const int rank) {
    return rank_ = rank;
}

int MPIAddress::Rank() const {
    return rank_;
}

bool MPIAddress::equals(const TransportAddress &rhs) const {
    const MPIAddress *mpirhs = dynamic_cast<const MPIAddress *>(&rhs);
    return mpirhs?(rank_ == mpirhs->rank_):false;
}
