#include <algorithm>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <iomanip>
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
    if (argc < 2) {
        std::cerr << "Syntax: " << argv[0] << " print?" << std::endl
                  << std::endl
                  << "Input is pass in through stdin in the following formats:" << std::endl
                  << "    PUT <KEY> <VALUE>" << std::endl
                  << "    GET|DEL|WHICH <KEY>" << std::endl
                  << "    BPUT N <KEY_1> <VALUE_1> ... <KEY_N> <VALUE_N>" << std::endl
                  << "    BGET|BDEL|BWHICH N <KEY_1> ... <KEY_N>" << std::endl
                  << "    COMMIT" << std::endl;
        return 1;
    }

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

    if (rank == 0) {
        std::cout << "World Size: " << size << std::endl;

        bool print = true;
        if (!(std::stringstream(argv[1]) >> std::boolalpha >> print)) {
            std::cerr << "Bad print argument: " << argv[1] << ". Should be true or false" << std::endl;
            cleanup(&md, &opts);
            return MDHIM_ERROR;
        }

        // if don't want to print, set failbit
        if (!print) {
            std::cout.setstate(std::ios_base::failbit);
        }

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
            bool read_rows = (cmd[0] == 'B');                                  // bulk operations take in a number before the data to indicate how many key-pair values there are

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
                    std::cout << std::string((char *)data[0][0], lens[0][0]) << " belongs on database " <<  mdhimWhichDB(&md, data[0][0], lens[0][0]) << std::endl;
                }
                else if (cmd == "BWHICH") {
                    for(std::size_t i = 0; i < num_keys; i++) {
                        std::cout << std::string((char *)data[0][i], lens[0][i]) << " belongs on database " <<  mdhimWhichDB(&md, data[0][i], lens[0][i]) << std::endl;
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

        std::cout.clear();

        long double *put_times = new long double[md.size]();
        std::size_t *num_puts = new std::size_t[md.size]();
        long double *get_times = new long double[md.size]();
        std::size_t *num_gets = new std::size_t[md.size]();

        mdhimGetStats(&md, 0, 1, put_times, 1, num_puts, 1, get_times, 1, num_gets);

        std::size_t puts = 0;
        long double put_rate = 0;
        std::size_t gets = 0;
        long double get_rate = 0;

        for(std::size_t i = 0; i < md.size; i++) {
            if (num_puts[i]) {
                put_rate += num_puts[i] / put_times[i];
                puts++;
            }
            if (num_gets[i]) {
                get_rate += num_gets[i] / get_times[i];
                gets++;
            }
        }

        std::cout << put_rate / puts << " PUTs/sec " << get_rate / gets << " GETs/sec" << std::endl;

        delete [] put_times;
        delete [] num_puts;
        delete [] get_times;
        delete [] num_gets;
    }
    else {
        mdhimGetStats(&md, 0, 1, nullptr, 1, nullptr, 1, nullptr, 1, nullptr);
    }

    cleanup(&md, &opts);

    return MDHIM_SUCCESS;
}
