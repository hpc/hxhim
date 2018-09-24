#include <iostream>
#include <map>

#include <mpi.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

int get_addrs(const MPI_Comm comm, const thallium::engine &engine, std::map<int, std::string> &addrs);

const std::string str('A', 64);

void f(const thallium::request &req, const std::string &str) {
    req.respond(str);
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    thallium::engine engine("bmi+tcp", THALLIUM_SERVER_MODE, true, -1);
    thallium::remote_procedure rpc = engine.define("f", f);

    std::map<int, std::string> addrs;
    get_addrs(MPI_COMM_WORLD, engine, addrs);

    // don't call self
    addrs.erase(rank);

    std::cout << rank << " got addresses" << std::endl;

    MPI_Barrier(MPI_COMM_WORLD);
    std::cout << "calling rpc" << std::endl;

    for(decltype(addrs)::value_type const & addr : addrs) {
        thallium::endpoint ep = engine.lookup(addr.second);
        const std::string res = rpc.on(ep)(str);
        std::cout << "from " << (std::string) ep << " " << res << std::endl;
    }

    std::cout << "done calling rpc" << std::endl;
    std::cout.flush();
    MPI_Barrier(MPI_COMM_WORLD);

    engine.finalize();
    MPI_Finalize();

    return 0;
}

int get_addrs(const MPI_Comm comm, const thallium::engine &engine, std::map<int, std::string> &addrs) {
    int rank;
    if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
        return -1;
    }

    int size;
    if (MPI_Comm_size(comm, &size) != MPI_SUCCESS) {
        return -1;
    }

    // get local engine's address
    const std::string self = static_cast<std::string>(engine.self());

    // get maximum size of all addresses
    const int self_len = self.size();
    int max_len = 0;
    if (MPI_Allreduce(&self_len, &max_len, 1, MPI_INT, MPI_MAX, comm) != MPI_SUCCESS) {
        return -1;
    }
    max_len++; // nullptr terminate

    // copy the address string and ensure the string is null terminated
    char *copy = new char[max_len]();
    memcpy(copy, self.c_str(), self_len);

    // get addresses
    char *buf = new char[max_len * size]();
    if (MPI_Allgather(copy, max_len, MPI_CHAR, buf, max_len, MPI_CHAR, MPI_COMM_WORLD) != MPI_SUCCESS) {
        delete [] copy;
        delete [] buf;
        return -1;
    }

    delete [] copy;

    // copy the addresses into strings
    // and map the strings to unique IDs
    for(int i = 0; i < size; i++) {
        const char *remote = &buf[max_len * i];
        addrs[i] = std::string(remote, strlen(remote));
    }

    delete [] buf;

    return 0;
}
