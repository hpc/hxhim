#include <algorithm>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <sstream>

#include "mdhim.h"

#include "util.hpp"
#include "put.hpp"
#include "get.hpp"
#include "bput.hpp"
#include "bget.hpp"
#include "del.hpp"
#include "bdel.hpp"

// A quick and dirty cleanup function
void cleanup(mdhim_t *md, mdhim_options_t *opts) {
    mdhimClose(md);
    mdhim_options_destroy(opts);
    MPI_Finalize();
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // initialize options through config
    mdhim_options_t opts;
    if (mdhim_default_config_reader(&opts, MPI_COMM_WORLD) != MDHIM_SUCCESS) {
        std::cerr << "Failed to initialize mdhim options" << std::endl;
        cleanup(NULL, &opts);
        return MDHIM_ERROR;
    }

    // initialize mdhim context
    mdhim_t md;
    if (mdhimInit(&md, &opts) != MDHIM_SUCCESS) {
        std::cerr << "Failed to initialize mdhim" << std::endl;
        cleanup(&md, &opts);
        return MDHIM_ERROR;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        std::cout << "World Size: " << size << std::endl;

        std::string line;
        while (std::cout << ">> ", std::getline(std::cin, line)) {
            std::stringstream s(line);
            std::string cmd;
            if (!(s >> cmd) || !cmd.size()) {
                continue;
            }

            if (cmd.size() < 3) {
                std::cerr << "Error: Unknown command " << cmd << std::endl;
                continue;
            }

            // capitalize the command
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

            if (cmd == "QUIT") {
                break;
            }

            if (cmd == "COMMIT") {
                if (mdhimCommit(&md, nullptr) != MDHIM_SUCCESS) {
                    std::cerr << "Could not commit" << std::endl;
                }
                else {
                    std::cout << "Committed" << std::endl;
                }
                continue;
            }

            std::size_t fields = 1 + (cmd.substr(cmd.size() - 3, 3) == "PUT"); // only PUT and BPUT have values in addition to keys
            void ***data = nullptr;
            std::size_t **lens = nullptr;
            std::size_t num_keys = 0;
            bool read_rows = (cmd[0] == 'B');                             // bulk operations take in a number before the data to indicate how many key-pair values there are

            if (bulk_read(s, fields, &data, &lens, num_keys, read_rows) == MDHIM_SUCCESS) {
                if (cmd == "PUT") {
                    put(&md, data[0][0], lens[0][0], data[1][0], lens[1][0]);
                }
                else if (cmd == "BPUT") {
                    bput(&md, data[0], lens[0], data[1], lens[1], num_keys);
                }
                else if (cmd == "GET") {
                    get(&md, data[0][0], lens[0][0]);
                }
                else if (cmd == "BGET") {
                    bget(&md, data[0], lens[0], num_keys);
                }
                else if (cmd == "DEL") {
                    del(&md, data[0][0], lens[0][0]);
                }
                else if (cmd == "BDEL") {
                    bdel(&md, data[0], lens[0], num_keys);
                }
                else if (cmd == "WHICH") {
                    std::cout << std::string((char *)data[0][0], lens[0][0]) << " belongs on range server " <<  mdhimWhichServer(&md, data[0][0], lens[0][0]) << std::endl;
                }
                else if (cmd == "BWHICH") {
                    for(int i = 0; i < num_keys; i++) {
                        std::cout << std::string((char *)data[0][i], lens[0][i]) << " belongs on range server " <<  mdhimWhichServer(&md, data[0][i], lens[0][i]) << std::endl;
                    }
                }
                else {
                    std::cerr << "Error: Unknown command " << cmd << std::endl;
                }
            }
            else {
                std::cerr << "Error: Bad input" << std::endl;
            }

            // cleanup input data
            bulk_clean(fields, data, lens, num_keys);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    cleanup(&md, &opts);

    return MDHIM_SUCCESS;
}
