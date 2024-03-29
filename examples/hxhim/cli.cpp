#include <cstring>
#include <ios>
#include <iostream>
#include <list>
#include <map>
#include <sstream>

#include "hxhim/hxhim.hpp"
#include "print_results.h"
#include "utils/Blob.hpp" // private header
#include "utils/memory.hpp"

enum HXHIM_OP {
    PUT,
    GET,
    GETOP,
    DEL,
    BPUT,
    BGET,
    BGETOP,
    BDEL,
    FLUSHPUTS,
    FLUSHGETS,
    FLUSHDELS,
    FLUSH,
};

const std::map<HXHIM_OP, std::string> FORMAT = {
    std::make_pair(HXHIM_OP::PUT,       "PUT <SUBJECT> <PREDICATE> <OBJECT>"),
    std::make_pair(HXHIM_OP::GET,       "GET <SUBJECT> <PREDICATE> <OBJECT_TYPE>"),
    std::make_pair(HXHIM_OP::GETOP,     "GETOP <SUBJECT> <PREDICATE> <OBJECT_TYPE> <NUM_RECS> <OP>"),
    std::make_pair(HXHIM_OP::DEL,       "DEL <SUBJECT> <PREDICATE>"),
    std::make_pair(HXHIM_OP::BPUT,      "BPUT N <SUBJECT_1> <PREDICATE_1> <OBJECT_1> ... <SUBJECT_N> <PREDICATE_N> <OBJECT_N>"),
    std::make_pair(HXHIM_OP::BGET,      "BGET N <SUBJECT_1> <PREDICATE_1> <OBJECT_TYPE_1> ... <SUBJECT_N> <PREDICATE_N> <OBJECT_TYPE_N>"),
    std::make_pair(HXHIM_OP::BGETOP,    "BGETOP <SUBJECT_1> <PREDICATE_1> <OBJECT_TYPE_1> <NUM_RECS_1> <OP_1> ... <SUBJECT_N> <PREDICATE_N> <OBJECT_TYPE_N> <NUM_RECS_N> <OP_N>"),
    std::make_pair(HXHIM_OP::BDEL,      "BDEL N <SUBJECT_1> <PREDICATE_1> ... <SUBJECT_N> <PREDICATE_N>"),
    std::make_pair(HXHIM_OP::FLUSHPUTS, "FLUSHPUTS"),
    std::make_pair(HXHIM_OP::FLUSHGETS, "FLUSHGETS"),
    std::make_pair(HXHIM_OP::FLUSHDELS, "FLUSHDELS"),
    std::make_pair(HXHIM_OP::FLUSH,     "FLUSH"),
};

const std::map<std::string, HXHIM_OP> USER2OP = {
    std::make_pair("PUT",       HXHIM_OP::PUT),
    std::make_pair("BPUT",      HXHIM_OP::BPUT),
    std::make_pair("GET",       HXHIM_OP::GET),
    std::make_pair("BGET",      HXHIM_OP::BGET),
    std::make_pair("GETOP",     HXHIM_OP::GETOP),
    std::make_pair("BGETOP",    HXHIM_OP::BGETOP),
    std::make_pair("DEL",       HXHIM_OP::DEL),
    std::make_pair("BDEL",      HXHIM_OP::BDEL),
    std::make_pair("FLUSHPUTS", HXHIM_OP::FLUSHPUTS),
    std::make_pair("FLUSHGETS", HXHIM_OP::FLUSHGETS),
    std::make_pair("FLUSHDELS", HXHIM_OP::FLUSHDELS),
    std::make_pair("FLUSH",     HXHIM_OP::FLUSH),
};

const std::map<std::string, hxhim_data_t> USER2OT = {
    std::make_pair("INT32",  HXHIM_DATA_INT32),
    std::make_pair("INT64",  HXHIM_DATA_INT64),
    std::make_pair("UINT32", HXHIM_DATA_UINT32),
    std::make_pair("UINT64", HXHIM_DATA_UINT64),
    std::make_pair("FLOAT",  HXHIM_DATA_FLOAT),
    std::make_pair("DOUBLE", HXHIM_DATA_DOUBLE),
    std::make_pair("BYTE",   HXHIM_DATA_BYTE),
};

const std::map<std::string, hxhim_getop_t> USER2GETOP = {
    std::make_pair("EQ",    HXHIM_GETOP_EQ),
    std::make_pair("NEXT",  HXHIM_GETOP_NEXT),
    std::make_pair("PREV",  HXHIM_GETOP_PREV),
    std::make_pair("FIRST", HXHIM_GETOP_FIRST),
    std::make_pair("LAST",  HXHIM_GETOP_LAST),
    // std::make_pair("PRIMARY_EQ", HXHIM_GETOP_PRIMARY_EQ),
};

std::ostream &help(char *self, std::ostream &stream = std::cout) {
    stream << "Syntax: " << self << " [-h | --help] [--ds <name>]" << std::endl
           << std::endl
           << "--ds implies a single rank. Running multiple MPI ranks will error." << std::endl
           << std::endl
           << "Input is passed in through the stdin of rank 0, delimited with newlines, in the following formats:" << std::endl;

    for(decltype(FORMAT)::value_type const &format : FORMAT) {
        stream << "    " << format.second << std::endl;
    }

    stream << "Where <SUBJECT>, <PREDICATE>, and <OBJECT> are pairs of <type, data>" << std::endl
           << "<OBJECT_TYPE> is only the type" << std::endl
           << std::endl
           << "Available Types:" << std::endl;

    for(decltype(USER2OT)::value_type const &type : USER2OT) {
        stream << "    " << type.first << std::endl;
    }

    stream << std::endl
           << "The <OP> argument to (B)GETOP can be one of the following:" << std::endl;

    for(decltype(USER2GETOP)::value_type const &op : USER2GETOP) {
        stream << "    " << op.first << std::endl;
    }

    return stream;
}

std::istream &operator>>(std::istream &stream, hxhim_data_t &type) {
    std::string str;
    if ((stream >> str)) {
        const decltype(USER2OT)::const_iterator it = USER2OT.find(str);
        if (it == USER2OT.end()) {
            stream.setstate(std::ios::failbit);
        }
        else {
            type = it->second;
        }
    }

    return stream;
}

std::istream &operator>>(std::istream &stream, Blob &input) {
    hxhim_data_t type;
    if (!(stream >> type)) {
        return stream;
    }

    void *data = nullptr;
    std::size_t size = 0;
    switch (type) {
        case HXHIM_DATA_INT32:
            size = sizeof(int32_t);
            data = alloc(size);
            stream >> * (int32_t *) data;
            break;
        case HXHIM_DATA_INT64:
            size = sizeof(int64_t);
            data = alloc(size);
            stream >> * (int64_t *) data;
            break;
        case HXHIM_DATA_UINT32:
            size = sizeof(uint32_t);
            data = alloc(size);
            stream >> * (uint32_t *) data;
            break;
        case HXHIM_DATA_UINT64:
            size = sizeof(uint64_t);
            data = alloc(size);
            stream >> * (uint64_t *) data;
            break;
        case HXHIM_DATA_FLOAT:
            size = sizeof(float);
            data = alloc(size);
            stream >> * (float *) data;
            break;
        case HXHIM_DATA_DOUBLE:
            size = sizeof(double);
            data = alloc(size);
            stream >> * (double *) data;
            break;
        case HXHIM_DATA_BYTE:
            {
                std::string str;
                if ((stream >> str)) {
                    size = str.size();
                    data = alloc(size);
                    memcpy(data, str.c_str(), str.size());
                }
            }
            break;
        default:
            stream.setstate(std::ios::failbit);
            break;
    }

    // on error, will be deallocated automatically
    input = RealBlob(data, size, type);

    return stream;
}

struct UserInput {
    HXHIM_OP hxhim_op;
    Blob subject;
    Blob predicate;
    Blob object; // data field is only used by (B)PUT

    // only used by (B)GETOP
    std::size_t num_recs;
    hxhim_getop_t op;
};

using UserInputs = std::list<UserInput>;

// A quick and dirty cleanup function
void cleanup(hxhim_t *hx) {
    hxhim::Close(hx);
    MPI_Finalize();
}

// serialize user input
// B* becomes individual operations
std::size_t parse_commands(std::istream &stream, UserInputs &commands) {
    std::string line;
    while (std::getline(stream, line)) {
        std::stringstream command(line);

        // empty line
        std::string op_str;
        if (!(command >> op_str)) {
            continue;
        }

        const decltype(USER2OP)::const_iterator op_it = USER2OP.find(op_str);
        if (op_it == USER2OP.end()) {
            std::cerr << "Error: Bad operation: " << op_str << std::endl;
            continue;
        }

        const HXHIM_OP hxhim_op = op_it->second;

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
            UserInput input;

            bool ok = true;
            switch ((input.hxhim_op = hxhim_op)) {
                case HXHIM_OP::PUT:
                case HXHIM_OP::BPUT:
                    {
                        if (!(command >> input.subject >> input.predicate >> input.object)) {
                            ok = false;
                        }
                    }
                    break;
                case HXHIM_OP::GET:
                case HXHIM_OP::BGET:
                    {
                        hxhim_data_t object_type;
                        if (!(command >> input.subject >> input.predicate >> object_type)) {
                            ok = false;
                        }
                        else {
                            input.object.set_type(object_type);
                        }
                    }
                    break;
                case HXHIM_OP::GETOP:
                case HXHIM_OP::BGETOP:
                    {
                        hxhim_data_t object_type;
                        std::string op;
                        if (!(command >> input.subject >> input.predicate >> object_type >> input.num_recs >> op)) {
                            ok = false;
                        }
                        else {
                            input.object.set_type(object_type);
                        }

                        const decltype(USER2GETOP)::const_iterator getop_it = USER2GETOP.find(op);
                        if (getop_it == USER2GETOP.end()) {
                            ok = false;
                        }
                        else {
                            input.op = getop_it->second;
                        }
                    }
                    break;
                case HXHIM_OP::DEL:
                case HXHIM_OP::BDEL:
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
                std::cerr << "Error: Could not parse line \"" << line << "\"." << std::endl
                          << "Expected: " << FORMAT.at(hxhim_op) << std::endl;
                break;
            }

            commands.emplace_back(std::move(input));
        }
    }

    return commands.size();
}

std::size_t run_commands(hxhim_t *hx, const UserInputs &commands) {
    std::size_t successful = 0;

    for(UserInputs::value_type const &cmd : commands) {
        int rc = HXHIM_SUCCESS;
        hxhim_results *res = nullptr;

        switch (cmd.hxhim_op) {
            case HXHIM_OP::PUT:
            case HXHIM_OP::BPUT:
                rc = hxhimPut(hx,
                              (void *) cmd.subject.data(),   cmd.subject.size(),   cmd.subject.data_type(),
                              (void *) cmd.predicate.data(), cmd.predicate.size(), cmd.predicate.data_type(),
                              (void *) cmd.object.data(),    cmd.object.size(),    cmd.object.data_type(),
                              HXHIM_PUT_ALL);
                break;
            case HXHIM_OP::GET:
            case HXHIM_OP::BGET:
                rc = hxhimGet(hx,
                              (void *) cmd.subject.data(),   cmd.subject.size(),   cmd.subject.data_type(),
                              (void *) cmd.predicate.data(), cmd.predicate.size(), cmd.predicate.data_type(),
                              cmd.object.data_type());
                break;
            case HXHIM_OP::GETOP:
            case HXHIM_OP::BGETOP:
                rc = hxhimGetOp(hx,
                                (void *) cmd.subject.data(),   cmd.subject.size(),   cmd.subject.data_type(),
                                (void *) cmd.predicate.data(), cmd.predicate.size(), cmd.predicate.data_type(),
                                cmd.object.data_type(),
                                cmd.num_recs, cmd.op);
                break;
            case HXHIM_OP::DEL:
            case HXHIM_OP::BDEL:
                rc = hxhimDelete(hx,
                                 (void *) cmd.subject.data(),   cmd.subject.size(),   cmd.subject.data_type(),
                                 (void *) cmd.predicate.data(), cmd.predicate.size(), cmd.predicate.data_type());
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

int main(int argc, char *argv[]) {
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
    hxhim_t hx;
    if (hxhimInit(&hx, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        if (rank == 0) {
            std::cout << "Error: Failed to read configuration" << std::endl;
        }
        cleanup(&hx);
        return 1;
    }

    // initialize hxhim context
    if (ds) {
        if (hxhimOpenOne(&hx, ds, strlen(ds)) != HXHIM_SUCCESS) {
            if (rank == 0) {
                std::cerr << "Failed to initialize hxhim" << std::endl;
            }
            cleanup(&hx);
            return 1;
        }
    }
    else {
        if (hxhimOpen(&hx) != HXHIM_SUCCESS) {
            if (rank == 0) {
                std::cerr << "Failed to initialize hxhim" << std::endl;
            }
            cleanup(&hx);
            return 1;
        }
    }

    // parse and run input
    if (rank == 0) {
        UserInputs cmds;
        const std::size_t valid_count = parse_commands(std::cin, cmds);
        std::cout << "Read " << valid_count << " valid commands" << std::endl;
        run_commands(&hx, cmds);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    cleanup(&hx);

    return 0;
}
