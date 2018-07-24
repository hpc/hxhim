#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <thread>

#include <mpi.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>

class Server {
    public:
        struct Data {
            Data(const thallium::request &req, const std::string &str)
                : request(req),
                  string(str)
                {}

            const thallium::request &request;
            const std::string &string;
        };

        static std::mutex mutex;
        static std::condition_variable cond;
        static std::list<Data *> unprocessed;

        // queue up the work for later processing
        static void enqueue(const thallium::request& req, const std::string &str) {
            std::lock_guard<std::mutex> lock(mutex);
            unprocessed.push_back(new Data(req, str));

            if (rand() & 3) {
                cond.notify_all();
            }
        }

        // process the work and remove it from the queue
        static void process(bool &running) {
            std::unique_lock<std::mutex> lock(mutex);
            while (running) {
                cond.wait(lock, [&]{
                        return !running || unprocessed.size();
                    });

                while (unprocessed.size()) {
                    Data *d = unprocessed.front();
                    unprocessed.pop_front(); // comment for error
                    d->request.respond(d->string);
                    // delete d;
                }
            }
        }
};

std::mutex Server::mutex = {};
std::condition_variable Server::cond = {};
std::list<Server::Data *> Server::unprocessed = {};

int get_addrs(const MPI_Comm comm, const thallium::engine &engine, std::map<int, std::string> &addrs) {
    int rank;
    if (MPI_Comm_rank(comm, &rank) != MPI_SUCCESS) {
        return 1;
    }

    int size;
    if (MPI_Comm_size(comm, &size) != MPI_SUCCESS) {
        return 1;
    }

    // get local engine's address
    const std::string self = static_cast<std::string>(engine.self());

    // get maximum size of all addresses
    const int self_len = self.size();
    int max_len = 0;
    if (MPI_Allreduce(&self_len, &max_len, 1, MPI_INT, MPI_MAX, comm) != MPI_SUCCESS) {
        return 1;
    }
    max_len++; // nullptr terminate

    // get addresses
    char *buf = new char[max_len * size]();
    if (MPI_Allgather(self.c_str(), self.size(), MPI_CHAR, buf, max_len, MPI_CHAR, MPI_COMM_WORLD) != MPI_SUCCESS) {
        delete [] buf;
        return 1;
    }

    // copy the addresses into strings
    // and map the strings to unique IDs
    for(int i = 0; i < size; i++) {
        const char *remote = &buf[max_len * i];
        addrs[i].assign(remote, strlen(remote));
    }

    delete [] buf;

    return 0;
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // create the engine
    thallium::engine engine("na+sm", THALLIUM_SERVER_MODE, true, -1);

    // create client to range server RPC
    thallium::remote_procedure rpc = engine.define("tcp", Server::enqueue);

    bool running = true;
    std::thread thread(Server::process, std::ref(running));

    // get a mapping of unique IDs (ranks) to thallium addresses (strings)
    std::map<int, std::string> addrs;
    if (get_addrs(MPI_COMM_WORLD, engine, addrs) != 0) {
        return 1;
    }

    // // remove the loopback endpoint
    // addrs.erase(rank);

    // create mapping between ranks and addresses
    std::map<int, thallium::endpoint *> endpoints;
    for(std::pair<const int, std::string> const &addr : addrs) {
        endpoints[addr.first] = new thallium::endpoint(engine.lookup(addr.second));
    }

    // send to all other processes
    const std::string data = "abcd";
    for(std::pair<const int, thallium::endpoint *> const &ep : endpoints) {
        rpc.on(*ep.second)(data);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // cleanup
    running = false;
    Server::cond.notify_all();
    if (thread.joinable()) {
        thread.join();
    }

    for(std::pair<const int, thallium::endpoint *> const &ep : endpoints) {
        delete ep.second;
    }

    engine.finalize();

    MPI_Finalize();

    return 0;
}
