#include <iostream>
#include <map>
#include <sys/types.h>

#include <mpi.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

// get all thallium lookup addresses
int get_addrs(thallium::engine *engine, const MPI_Comm comm, std::map<int, std::string> &addrs);

struct Address {
    const thallium::remote_procedure &rpc;
    thallium::endpoint ep;

    Address(thallium::engine *engine,
            thallium::remote_procedure &rp,
            const std::string &proto)
        : rpc(rp), ep(engine->lookup(proto))
    {}

    int f() {
        return (int) rpc.on(ep)(std::string("ABC"));
    }
};

struct A {
    static void func(const thallium::request &req, const std::string &str) {
        std::cout << "RPC got " << str << std::endl;
        req.respond(1);
    }
};

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    thallium::engine *engine = new thallium::engine("tcp", THALLIUM_SERVER_MODE);
    thallium::remote_procedure rpc = engine->define("func", A::func);

    // wait for all local resources to be created
    MPI_Barrier(MPI_COMM_WORLD);

    // get the addresses of all other ranks
    std::map<int, std::string> addrs;
    get_addrs(engine, MPI_COMM_WORLD, addrs);

    if (rank == 0) {
        std::cout << addrs[0] << std::endl;
    // }
    // else
    // {
        thallium::endpoint ep = engine->lookup(addrs[0]);
        rpc.on(ep)(std::string("123"));
    }

    // // create MPI rank -> thallium endpoint mapping
    // std::map<int, Address *> mapping;
    // for(std::pair<const int, std::string> const &addr : addrs) {
    //     mapping[addr.first] = new Address(engine, rpc, addr.second);
    // }

    // // perform RPC on all other ranks
    // for(std::pair<const int, Address *> &addr : mapping) {
    //     std::cout << (int) addr.second->f() << std::endl;
    //     delete addr.second;
    //     addr.second = nullptr;
    // }

    MPI_Barrier(MPI_COMM_WORLD);

    // engine->finalize();
    delete engine;

    MPI_Finalize();
    return 0;
}
