#include <algorithm>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <list>
#include <sstream>

#include "../util.hpp"
#include "hxhim/hxhim.h"
#include "print_results.h"

std::ostream &help(char *self, std::ostream &stream = std::cout) {
    return stream << "Syntax: " << self << std::endl
                  << std::endl
                  << "Input is passed in through stdin, delimited with newlines, in the following formats:" << std::endl
                  << "    PUT <SUBJECT> <PREDICATE> <OBJECT>" << std::endl
                  << "    GET|DEL <SUBJECT> <PREDICATE> " << std::endl
                  << "    BPUT N <SUBJECT_1> <PREDICATE_1> <OBJECT_1> ... <SUBJECT_N> <PREDICATE_N> <OBJECT_N>" << std::endl
                  << "    BGET|BDEL N <SUBJECT_1> <PREDICATE_1> ... <SUBJECT_N> <PREDICATE_N>" << std::endl
                  << "    COMMIT" << std::endl;
}

// A quick and dirty cleanup function
void cleanup(hxhim_t *hx) {
    hxhimClose(hx);
    MPI_Finalize();
}

int main(int argc, char *argv[]) {
    if (argc > 1) {
        help(argv[0], std::cerr);
        return 1;
    }

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // initialize hxhim context
    hxhim_t hx;
    if (hxhimOpen(&hx, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        std::cerr << "Failed to initialize hxhim" << std::endl;
        cleanup(&hx);
        return HXHIM_ERROR;
    }

    if (rank == 0) {
        std::cout << "World Size: " << size << std::endl;

        // store user input until it is flushed, or the data used will be invalid
        std::list <UserInput> inputs;

        std::string line;
        while (std::cout << ">> ", std::getline(std::cin, line)) {
            std::stringstream s(line);
            std::string cmd;
            if (!(s >> cmd) || !cmd.size()) {
                continue;
            }

            if (cmd[0] == '#') {
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
            else if (cmd == "HELP") {
                help(argv[0]);
            }

            if (cmd == "FLUSH") {
                hxhim_results_t *results = hxhimFlush(&hx);
                if (!results) {
                    std::cerr << "Could not Flush" << std::endl;
                }
                else {
                    print_results(-1, results);
                }

                hxhim_results_destroy(results);

                // clean up user input
                for(UserInput &input : inputs) {
                    bulk_clean(input);
                }
                inputs.clear();

                continue;
            }

            UserInput input;
            input.fields = 2 + (cmd.substr(cmd.size() - 3, 3) == "PUT"); // only PUT and BPUT have values in addition to keys
            input.data = nullptr;
            input.lens = nullptr;
            input.num_keys = 0;
            bool read_rows = (cmd[0] == 'B');                            // bulk operations take in a number before the data to indicate how many key-pair values there are

            if (bulk_read(s, input, read_rows)) {
                if (cmd == "PUT") {
                    hxhimPut(&hx, input.data[0][0], input.lens[0][0], input.data[1][0], input.lens[1][0], input.data[2][0], input.lens[2][0]);
                }
                else if (cmd == "BPUT") {
                    hxhimBPut(&hx, input.data[0], input.lens[0], input.data[1], input.lens[1], input.data[2], input.lens[2], input.num_keys);
                }
                else if (cmd == "GET") {
                    hxhimGet(&hx, input.data[0][0], input.lens[0][0], input.data[1][0], input.lens[1][0]);
                }
                else if (cmd == "BGET") {
                    hxhimBGet(&hx, input.data[0], input.lens[0], input.data[1], input.lens[1], input.num_keys);
                }
                else if (cmd == "DEL") {
                    hxhimDelete(&hx, input.data[0][0], input.lens[0][0], input.data[1][0], input.lens[1][0]);
                }
                else if (cmd == "BDEL") {
                    hxhimBDelete(&hx, input.data[0], input.lens[0], input.data[1], input.lens[1], input.num_keys);
                }
                else {
                    std::cerr << "Error: Unknown command " << cmd << std::endl;
                }
            }
            else {
                std::cerr << "Error: Bad input: " << line << std::endl;
            }

            // store user input because HXHIM does not own any of the pointers
            inputs.emplace_back(input);
        }

        // clean up unflushed user input
        for(UserInput &input : inputs) {
            bulk_clean(input);
        }
        inputs.clear();

        long double *put_times = new long double[size]();
        std::size_t *num_puts = new std::size_t[size]();
        long double *get_times = new long double[size]();
        std::size_t *num_gets = new std::size_t[size]();

        hxhimGetStats(&hx, 0, 1, put_times, 1, num_puts, 1, get_times, 1, num_gets);

        std::size_t puts = 0;
        long double put_rate = 0;
        std::size_t gets = 0;
        long double get_rate = 0;

        for(std::size_t i = 0; i < size; i++) {
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
        hxhimGetStats(&hx, 0, 1, nullptr, 1, nullptr, 1, nullptr, 1, nullptr);
    }

    cleanup(&hx);

    return HXHIM_SUCCESS;
}
