#if HXHIM_HAVE_THALLIUM

#include "transport/backend/Thallium/Utilities.hpp"

/**
 * get_thallium_addrs
 *
 * @param comm the bootstrapping MPI communicator
 * @param engine the engine being used
 * @param addrs the map that will be filled with the thallium addresses
 * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
 */
int Transport::Thallium::get_addrs(const MPI_Comm comm, const thallium::engine &engine, std::map<int, std::string> &addrs) {
    int rank;
    if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    int size;
    if (MPI_Comm_size(comm, &size) != MPI_SUCCESS) {
        return TRANSPORT_ERROR;
    }

    // get local engine's address
    const std::string self = static_cast<std::string>(engine.self());

    // get maximum size of all addresses
    const int self_len = self.size();
    int max_len = 0;
    if (MPI_Allreduce(&self_len, &max_len, 1, MPI_INT, MPI_MAX, comm) != MPI_SUCCESS) {
        return TRANSPORT_ERROR;
    }
    max_len++; // null terminate

    // copy the address string and ensure the string is null terminated
    char *copy = new char[max_len]();
    memcpy(copy, self.c_str(), self_len);

    // get addresses
    char *buf = new char[max_len * size]();
    if (MPI_Allgather(copy, max_len, MPI_CHAR, buf, max_len, MPI_CHAR, comm) != MPI_SUCCESS) {
        delete [] copy;
        delete [] buf;
        return TRANSPORT_ERROR;
    }

    delete [] copy;

    // copy the addresses into strings
    // and map the strings to unique IDs
    for(int i = 0; i < size; i++) {
        const char *remote = &buf[max_len * i];
        addrs[i] = std::string(remote, strlen(remote));
    }

    delete [] buf;

    return TRANSPORT_SUCCESS;
}

#endif
