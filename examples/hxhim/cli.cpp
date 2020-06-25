#include <list>
#include <map>
#include <iostream>
#include <sstream>
#include <cstring>

#include "hxhim/hxhim.h"
#include "print_results.h"

std::ostream &help(char *self, std::ostream &stream = std::cout) {
    return stream << "Syntax: " << self << " [-h | --help] [--ds <name>]" << std::endl
                  << std::endl
                  << "--ds implies a single rank. Running multiple MPI ranks will error." << std::endl
                  << std::endl
                  << "Input is passed in through the stdin of rank 0, delimited with newlines, in the following formats:" << std::endl

                  << "    PUT <SUBJECT> <PREDICATE> <OBJECT>" << std::endl
                  << "    GET <SUBJECT> <PREDICATE> " << std::endl
                  << "    DEL <SUBJECT> <PREDICATE> " << std::endl
                  << "    BPUT N <SUBJECT_1> <PREDICATE_1> <OBJECT_1> ... <SUBJECT_N> <PREDICATE_N> <OBJECT_N>" << std::endl
                  << "    BGET N <SUBJECT_1> <PREDICATE_1> ... <SUBJECT_N> <PREDICATE_N>" << std::endl
                  << "    BDEL N <SUBJECT_1> <PREDICATE_1> ... <SUBJECT_N> <PREDICATE_N>" << std::endl
                  << "    FLUSHPUTS" << std::endl
                  << "    FLUSHGETS" << std::endl
                  << "    FLUSHDELS" << std::endl
                  << "    FLUSH" << std::endl
                  << std::endl
                  << "Data is currently expected to all be strings" << std::endl;
}

enum HXHIM_OP {
    PUT,
    GET,
    DEL,
    FLUSHPUTS,
    FLUSHGETS,
    FLUSHDELS,
    FLUSH,
};

const std::map<std::string, HXHIM_OP> USER2OP = {
    std::make_pair("PUT",       HXHIM_OP::PUT),
    std::make_pair("BPUT",      HXHIM_OP::PUT),
    std::make_pair("GET",       HXHIM_OP::GET),
    std::make_pair("BGET",      HXHIM_OP::GET),
    std::make_pair("DEL",       HXHIM_OP::DEL),
    std::make_pair("BDEL",      HXHIM_OP::DEL),
    std::make_pair("FLUSHPUTS", HXHIM_OP::FLUSHPUTS),
    std::make_pair("FLUSHGETS", HXHIM_OP::FLUSHGETS),
    std::make_pair("FLUSHDELS", HXHIM_OP::FLUSHDELS),
    std::make_pair("FLUSH",     HXHIM_OP::FLUSH),
};

struct SubjectPredicateObject {
    HXHIM_OP op;
    std::string subject;
    std::string predicate;
    std::string object;
};

using UserInput = std::list<SubjectPredicateObject>;

// A quick and dirty cleanup function
void cleanup(hxhim_t *hx, hxhim_options_t *opts) {
    hxhimClose(hx);
    hxhim_options_destroy(opts);
    MPI_Finalize();
}

// serialize user input
// B* becomes individual operations
std::size_t parse_commands(std::istream & stream, UserInput & commands) {
    std::string line;
    while (std::getline(stream, line)) {
        std::stringstream command(line);

        // empty line
        std::string op_str;
        if (!(command >> op_str)) {
            continue;
        }

        const std::map<std::string, HXHIM_OP>::const_iterator op_it = USER2OP.find(op_str);
        if (op_it == USER2OP.end()) {
            std::cerr << "Error: Bad operation: " << op_str << std::endl;
            continue;
        }

        const HXHIM_OP op = op_it->second;

        std::size_t count = 1;

        // bulk, so get count first
        if (op_str[0] == 'B') {
            if (!(command >> count)) {
                std::cerr << "Error: Bad count: " << line << std::endl;
                continue;
            }
        }

        // read input
        for(std::size_t i = 0; i < count; i++) {
            SubjectPredicateObject input;

            bool ok = true;
            switch ((input.op = op)) {
                case HXHIM_OP::PUT:
                    if (!(command >> input.subject >> input.predicate >> input.object)) {
                        ok = false;
                    }
                    break;
                case HXHIM_OP::GET:
                case HXHIM_OP::DEL:
                    if (!(command >> input.subject >> input.predicate)) {
                        ok = false;
                    }
                    break;
                case HXHIM_OP::FLUSHPUTS:
                case HXHIM_OP::FLUSHGETS:
                case HXHIM_OP::FLUSHDELS:
                case HXHIM_OP::FLUSH:
                    break;
            }

            if (!ok) {
                std::cerr << "Error: Could not parse line \"" << line << "\"" << std::endl;
                break;
            }

            commands.emplace_back(input);
        }
    }

    return commands.size();
}

std::size_t run_commands(hxhim_t * hx, const UserInput &commands) {
    std::size_t successful = 0;

    for(UserInput::value_type const &cmd : commands) {
        int rc = HXHIM_SUCCESS;
        hxhim_results *res = nullptr;

        switch (cmd.op) {
            case HXHIM_OP::PUT:
                rc = hxhimPut(hx,
                              (void *) cmd.subject.c_str(), cmd.subject.size(),
                              (void *) cmd.predicate.c_str(), cmd.predicate.size(),
                              HXHIM_BYTE_TYPE,
                              (void *) cmd.object.c_str(), cmd.object.size());
                break;
            case HXHIM_OP::GET:
                rc = hxhimGet(hx,
                              (void *) cmd.subject.c_str(), cmd.subject.size(),
                              (void *) cmd.predicate.c_str(), cmd.predicate.size(),
                              HXHIM_BYTE_TYPE);
                break;
            case HXHIM_OP::DEL:
                rc = hxhimDelete(hx,
                                 (void *) cmd.subject.c_str(), cmd.subject.size(),
                                 (void *) cmd.predicate.c_str(), cmd.predicate.size());
                break;
            case HXHIM_OP::FLUSHPUTS:
                res = hxhimFlushPuts(hx);
                break;
            case HXHIM_OP::FLUSHGETS:
                res = hxhimFlushGets(hx);
                break;
            case HXHIM_OP::FLUSHDELS:
                res = hxhimFlushDeletes(hx);
                break;
            case HXHIM_OP::FLUSH:
                res = hxhimFlush(hx);
                break;
        }

        print_results(hx, 0, res);
        for(hxhim_results_goto_head(res);
            hxhim_results_valid(res) == HXHIM_SUCCESS;
            hxhim_results_goto_next(res)) {

            enum hxhim_result_type type;
            hxhim_results_type(res, &type);

            switch (type) {
                case HXHIM_RESULT_GET:
                    {
                        char *object = nullptr;
                        size_t object_len = 0;
                        hxhim_results_object(res, (void **) &object, &object_len);
                    }
                    break;
                default:
                    break;
            }
        }
        hxhim_results_destroy(res);

        if (rc == HXHIM_ERROR) {
            std::cerr << "Error" << std::endl;
        }
        else {
            successful++;
        }
    }

    // do not implicity flush

    return successful;
}

int main(int argc, char * argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    char * ds = nullptr;

    for(int i = 1; i < argc;) {
        const std::string args(argv[i]);
        if ((args == "-h")|| (args == "--help")) {
            if (rank == 0) {
                help(argv[0], std::cerr);
            }
            MPI_Finalize();
            return 0;
        }

        if (args == "--ds") {
            // --ds implies a single rank, but a single rank does not imply --ds
            if (size != 1) {
                std::cerr << "Error: The --ds flag implies 1 rank. Have " << size << std::endl;
                MPI_Finalize();
                return 1;
            }

            ds = argv[++i];
        }

        i++;
    }

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        if (rank == 0) {
            std::cout << "Error: Failed to read configuration" << std::endl;
        }
        return 1;
    }

    // initialize hxhim context
    hxhim_t hx;
    if (ds) {
        if (hxhimOpenOne(&hx, &opts, ds, strlen(ds)) != HXHIM_SUCCESS) {
            if (rank == 0) {
                std::cerr << "Failed to initialize hxhim" << std::endl;
            }
            cleanup(&hx, &opts);
            return 1;
        }
    }
    else {
        if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
            if (rank == 0) {
                std::cerr << "Failed to initialize hxhim" << std::endl;
            }
            cleanup(&hx, &opts);
            return 1;
        }
    }

    // parse and run input
    if (rank == 0) {
        UserInput cmds;
        const std::size_t valid_count = parse_commands(std::cin, cmds);
        std::cout << "Read " << valid_count << " valid commands" << std::endl;
        run_commands(&hx, cmds);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    cleanup(&hx, &opts);

    return 0;
}
