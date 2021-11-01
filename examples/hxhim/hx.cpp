#include <iostream>
#include <sstream>

#include <mpi.h>

#include "hxhim/hxhim.hpp"
#include "print_results.h"
#include "spo_gen.h"
#include "timestamps.h"

static void print_results(const int rank, hxhim::Results *results) {
    if (!results) {
        return;
    }

    for(results->GoToHead(); results->ValidIterator(); results->GoToNext()) {
        std::cout << "Rank " << rank << " ";
        hxhim_op_t op = hxhim_op_t::HXHIM_INVALID;
        results->Op(&op);

        int status = HXHIM_ERROR;
        results->Status(&status);

        int range_server = -1;
        results->RangeServer(&range_server);

        switch (op) {
            case HXHIM_PUT:
                {
                    std::cout << "PUT returned " << ((status == HXHIM_SUCCESS)?std::string("SUCCESS"):std::string("ERROR")) << " from range server " << range_server << std::endl;
                }
                break;
            case HXHIM_GET:
                std::cout << "GET returned ";
                if (status == HXHIM_SUCCESS) {
                    void *subject = nullptr;
                    std::size_t subject_len = 0;
                    hxhim_data_t subject_type;
                    results->Subject(&subject, &subject_len, &subject_type);

                    void *predicate = nullptr;
                    std::size_t predicate_len = 0;
                    hxhim_data_t predicate_type;
                    results->Predicate(&predicate, &predicate_len, &predicate_type);

                    void *object = nullptr;
                    std::size_t object_len = 0;
                    hxhim_data_t object_type;
                    results->Object(&object, &object_len, &object_type);

                    std::cout << "{";
                    print_by_type(subject, subject_len, subject_type);
                    std::cout << ", ";
                    print_by_type(predicate, predicate_len, predicate_type);
                    std::cout << "} -> ";
                    print_by_type(object, object_len, object_type);
                }
                else {
                    std::cout << "ERROR";
                }

                std::cout << " from range server " << range_server << std::endl;
                break;
            default:
                std::cout << "Bad Operation: " << op << std::endl;
                break;
        }
    }
}

const std::size_t bufsize = 100;

int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "Syntax: " << argv[0] << " count print?" << std::endl;
        return 1;
    }

    std::size_t count = 0;
    if (!(std::stringstream(argv[1]) >> count)) {
        std::cerr << "Error: Could not parse count argument: " << argv[1] << std::endl;
        return 1;
    }

    bool print = 0;
    if (!(std::stringstream(argv[2]) >> print)) {
        std::cerr << "Error: Could not parse print argument: " << argv[2] << std::endl;
        return 1;
    }

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // read the config
    hxhim_options_t opts;
    if (hxhim_config_default_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        if (rank == 0) {
            std::cerr << "Failed to read configuration" << std::endl;
        }
        MPI_Finalize();
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhim::Open(&hx, &opts) != HXHIM_SUCCESS) {
        if (rank == 0) {
            std::cerr << "Failed to initialize hxhim" << std::endl;
        }
        hxhim_options_destroy(&opts);
        MPI_Finalize();
        return 1;
    }

    struct timespec epoch;
    hxhimGetEpoch(&hx, &epoch);

    // Generate some subject-predicate-object triples
    void **subjects = NULL, **predicates = NULL;
    std::size_t *subject_lens = NULL, *predicate_lens = NULL;
    if (spo_gen_fixed(count, bufsize, rank, &subjects, &subject_lens, &predicates, &predicate_lens, NULL, NULL) != count) {
        return 1;
    }

    double *doubles = (double *) malloc(sizeof(double) * count);
    for(size_t i = 0; i < count; i++) {
        doubles[i] = rand();
        doubles[i] /= rand();
        doubles[i] *= rand();
        doubles[i] /= rand();
    }

   // PUT the subject-predicate-object triples
    timestamp_start(put);
    for(std::size_t i = 0; i < count; i++) {
        hxhim::PutDouble(&hx,
                         subjects[i], subject_lens[i], HXHIM_DATA_BYTE,
                         predicates[i], predicate_lens[i], HXHIM_DATA_BYTE,
                         &doubles[i],
                         HXHIM_PUT_SPO);
        if (print) {
            std::cout << "Rank " << rank << " PUT {" << std::string((char *) subjects[i], subject_lens[i]) << ", " << std::string((char *) predicates[i], predicate_lens[i]) << "} -> " << doubles[i] << std::endl;
        }
    }
    timestamp_end(put);

    barrier;

    if (rank == 0) {
        std::cout << "GET before flushing PUTs" << std::endl;
    }

    barrier;

    // GET them back, flushing only the GETs
    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetDouble(&hx,
                         subjects[i], subject_lens[i], HXHIM_DATA_BYTE,
                         predicates[i], predicate_lens[i], HXHIM_DATA_BYTE);
    }

    hxhim::Results *flush_gets_early = hxhim::FlushGets(&hx);
    if (print) {
        print_results(rank, flush_gets_early);
    }
    hxhim::Results::Destroy(flush_gets_early);

    // flush PUTs
    barrier;

    if (rank == 0) {
        std::cout << "Flush PUTs" << std::endl;
    }

    barrier;

    timestamp_start(flush_put);
    hxhim::Results *flush_puts = hxhim::FlushPuts(&hx);
    timestamp_end(flush_put);

    barrier;

    if (print) {
        print_results(rank, flush_puts);
    }

    long double duration = 0;
    duration += nano(&put_start, &put_end);
    duration += nano(&flush_put_start, &flush_put_end);
    duration /= 1e9;

    for(int i = 0; i < size; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        if (i == rank) {
            fprintf(stderr, "Rank %d: %zu PUTs in %.3Lf seconds (%.3Lf PUTs/sec)\n",
                    i, count, duration, count / duration);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    timestamp_start(destroy);
    hxhim::Results::Destroy(flush_puts);
    timestamp_end(destroy);

    barrier;

    // GET again, now that all PUTs have completed
    for(std::size_t i = 0; i < count; i++) {
        hxhim::GetDouble(&hx,
                         subjects[i], subject_lens[i], HXHIM_DATA_BYTE,
                         predicates[i], predicate_lens[i], HXHIM_DATA_BYTE);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        std::cout << "GET after flushing PUTs" << std::endl;
    }
    MPI_Barrier(MPI_COMM_WORLD);

    hxhim::Results *flush_gets = hxhim::Flush(&hx);
    if (print) {
        print_results(rank, flush_gets);
    }
    hxhim::Results::Destroy(flush_gets);

    // cleanup
    free(doubles);
    spo_clean(count, &subjects, &subject_lens, &predicates, &predicate_lens, nullptr, nullptr);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhim::Close(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
