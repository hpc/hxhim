//
// Created by bws on 8/30/17.
//

#include "comm_mpi.h"
using namespace std;

int MPIEndpoint::PutMessageImpl(const CommEndpoint& dest) {
    return -1;
}

int MPIEndpoint::GetMessageImpl(const CommEndpoint& src) {
    return -1;
}

int MPIEndpoint::PollForMessageImpl(size_t timeoutSecs) {
    return -1;
}

int MPIEndpoint::WaitForMessageImpl(size_t timeoutSecs) {
    return -1;
}
