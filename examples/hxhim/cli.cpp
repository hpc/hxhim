#include <algorithm>
#include <cctype>
#include <cstddef>
#include <iostream>
#include <list>
#include <sstream>
#include <type_traits>

#include "../util.hpp"
#include "hxhim/hxhim.h"
#include "print_results.h"

std::ostream &help(char *self, std::ostream &stream = std::cout) {
    return stream << "Syntax: " << self << "[-h | --help] [--db db_name] [object_type=BYTE]" << std::endl
                  << std::endl
                  << "types: INT, SIZE, FLOAT, DOUBLE, BYTE" << std::endl
                  << std::endl
                  << "Input is passed in through stdin, delimited with newlines, in the following formats:" << std::endl
                  << "    PUT <SUBJECT> <PREDICATE> <OBJECT>" << std::endl
                  << "    GET|DEL <SUBJECT> <PREDICATE> " << std::endl
                  << "    BPUT N <SUBJECT_1> <PREDICATE_1> <OBJECT_1> ... <SUBJECT_N> <PREDICATE_N> <OBJECT_N>" << std::endl
                  << "    BGET|BDEL N <SUBJECT_1> <PREDICATE_1> ... <SUBJECT_N> <PREDICATE_N>" << std::endl
                  << "    COMMIT" << std::endl;
}

// A quick and dirty cleanup function
void cleanup(hxhim_t *hx, hxhim_options_t *opts) {
    hxhimClose(hx);
    hxhim_options_destroy(opts);
    MPI_Finalize();
}

template <typename T, typename NotStr = std::enable_if_t<!std::is_same<T, std::string>::value> >
int read_bgetop_input(std::stringstream & s, void **prefix, std::size_t *prefix_len, int &count) {
    if (!prefix || !prefix_len) {
        return HXHIM_ERROR;
    }

    T subject;
    if (!(s >> subject >> count)) {
        return HXHIM_ERROR;
    }

    *prefix_len = sizeof(T);
    if (!(*prefix = ::operator new(*prefix_len))) {
        return HXHIM_ERROR;
    }

    memcpy(*prefix, &subject, *prefix_len);
    return HXHIM_SUCCESS;
}

int main(int argc, char *argv[]) {
    char *db = nullptr;
    enum hxhim_type_t type = HXHIM_BYTE_TYPE;
    for(int i = 1; i < argc; i++) {
        std::string args(argv[i]);
        if ((args == "-h") || (args == "--help")) {
            help(argv[0], std::cerr);
            return HXHIM_SUCCESS;
        }
        else if (args == "--db") {
            db = argv[++i];
        }
        else {
            std::transform(args.begin(), args.end(), args.begin(), ::toupper);

            if (args == "INT") {
                type = HXHIM_INT_TYPE;
            }
            else if (args == "SIZE") {
                type = HXHIM_SIZE_TYPE;
            }
            else if (args == "FLOAT") {
                type = HXHIM_FLOAT_TYPE;
            }
            else if (args == "DOUBLE") {
                type = HXHIM_DOUBLE_TYPE;
            }
            else if (args == "BYTE") {
                type = HXHIM_BYTE_TYPE;
            }
            else {
                std::cerr << "Error: Bad Type: " << args << std::endl;
                return HXHIM_ERROR;
            }
        }
    }

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        printf("Failed to read configuration");
        return 1;
    }

    // initialize hxhim context
    hxhim_t hx;
    if (db) {
        if (hxhimOpenOne(&hx, &opts, db, strlen(db)) != HXHIM_SUCCESS) {
            std::cerr << "Failed to initialize hxhim" << std::endl;
            cleanup(&hx, &opts);
            return HXHIM_ERROR;
        }
    }
    else {
        if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
            std::cerr << "Failed to initialize hxhim" << std::endl;
            cleanup(&hx, &opts);
            return HXHIM_ERROR;
        }
    }

    if (rank == 0) {
        std::cout << "World Size: " << size << std::endl;

        // store user input until it is flushed, or the data used will be invalid
        std::list <UserInput> inputs;
        std::list <void *> bgetop_inputs;

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
            else if (cmd == "FLUSH") {
                hxhim_results_t *results = hxhimFlush(&hx);
                if (!results) {
                    std::cerr << "Could not Flush" << std::endl;
                }
                else {
                    print_results(&hx, 0, results);
                }

                hxhim_results_destroy(results);

                // clean up user input
                for(UserInput &input : inputs) {
                    bulk_clean(input);
                }
                inputs.clear();

                for(void *ptr : bgetop_inputs) {
                    ::operator delete(ptr);
                }
                bgetop_inputs.clear();

                continue;
            }
            else if (cmd == "BGETOP") {
                void *prefix = nullptr;
                std::size_t prefix_len = 0;
                int count = 0;
                int ret = HXHIM_SUCCESS;

                std::string subject;
                if (!(s >> subject >> count)) {
                    ret = HXHIM_ERROR;
                    std::cerr << "Bad BGetOp input: " << line << std::endl;
                    continue;
                }

                prefix_len = subject.size();
                prefix = ::operator new(prefix_len);
                memcpy(prefix, subject.c_str(), prefix_len);

                // negative indicates "get previous"
                enum hxhim_get_op_t op = HXHIM_GET_EQ;
                if (count < 0) {
                    op = HXHIM_GET_PREV;
                }
                // positive indicates "get next"
                else if (count > 0) {
                    op = HXHIM_GET_NEXT;
                }
                // 0 indicates "get equal"
                else {
                    count = 1;
                }

                hxhimBGetOp(&hx, prefix, prefix_len, nullptr, 0, type, std::abs(count), op);
                bgetop_inputs.push_back(prefix);
            }
            else {
                UserInput input;
                input.fields = 2 + (cmd.substr(cmd.size() - 3, 3) == "PUT"); // only PUT and BPUT have values in addition to keys
                input.data = nullptr;
                input.lens = nullptr;
                input.num_keys = 0;
                bool read_rows = (cmd[0] == 'B');                            // bulk operations take in a number before the data to indicate how many key-pair values there are

                static const bool encode[] = {false, false, true};

                bool ret = false;
                switch (type) {
                    case HXHIM_INT_TYPE:
                        ret = bulk_read<int>(s, input, encode, read_rows);
                        break;
                    case HXHIM_SIZE_TYPE:
                        ret = bulk_read<std::size_t>(s, input, encode, read_rows);
                        break;
                    case HXHIM_INT64_TYPE:
                        ret = bulk_read<int64_t>(s, input, encode, read_rows);
                        break;
                    case HXHIM_FLOAT_TYPE:
                        ret = bulk_read<float>(s, input, encode, read_rows);
                        break;
                    case HXHIM_DOUBLE_TYPE:
                        ret = bulk_read<double>(s, input, encode, read_rows);
                        break;
                    case HXHIM_BYTE_TYPE:
                        ret = bulk_read<std::string>(s, input, encode, read_rows);
                        break;
                }

                if (ret) {
                    if (cmd == "PUT") {
                        hxhimPut(&hx, input.data[0][0], input.lens[0][0], input.data[1][0], input.lens[1][0], type, input.data[2][0], input.lens[2][0]);
                    }
                    else if (cmd == "BPUT") {
                        hxhimBPutSingleType(&hx, input.data[0], input.lens[0], input.data[1], input.lens[1], type, input.data[2], input.lens[2], input.num_keys);
                    }
                    else if (cmd == "GET") {
                        hxhimGet(&hx, input.data[0][0], input.lens[0][0], input.data[1][0], input.lens[1][0], type);
                    }
                    else if (cmd == "BGET") {
                        hxhimBGetSingleType(&hx, input.data[0], input.lens[0], input.data[1], input.lens[1], type, input.num_keys);
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
        }

        // clean up unflushed user input
        for(UserInput &input : inputs) {
            bulk_clean(input);
        }
        inputs.clear();

        for(void *ptr : bgetop_inputs) {
            ::operator delete(ptr);
        }
        bgetop_inputs.clear();

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

    cleanup(&hx, &opts);

    return HXHIM_SUCCESS;
}
