#include <iostream>
#include <sstream>

#include <mpi.h>

#include "hxhim/hxhim.hpp"
#include "spo_gen.h"

static void print_results(const int rank, hxhim::Results *results) {
    if (!results) {
        return;
    }

    for(results->GoToHead(); results->Valid(); results->GoToNext()) {
        std::cout << "Rank " << rank << " ";
        hxhim::Results::Result *curr = results->Curr();
        switch (curr->type) {
            case HXHIM_RESULT_PUT:
                std::cout << "PUT returned " << ((curr->error == HXHIM_SUCCESS)?std::string("SUCCESS"):std::string("ERROR")) << " from database " << curr->database << std::endl;
                break;
            case HXHIM_RESULT_GET:
                std::cout << "GET returned ";
                if (curr->error == HXHIM_SUCCESS) {
                    hxhim::Results::Get *get = static_cast<hxhim::Results::Get *>(curr);
                    std::cout << "{" << std::string((char *) get->subject, get->subject_len)
                              << ", " << std::string((char *) get->predicate, get->predicate_len)
                              << "} -> " << std::string((char *) get->object, get->object_len);
                }
                else {
                    std::cout << "ERROR";
                }

                std::cout << " from database " << curr->database << std::endl;
                break;
            case HXHIM_RESULT_DEL:
                std::cout << "DEL returned " << ((curr->error == HXHIM_SUCCESS)?std::string("SUCCESS"):std::string("ERROR")) << " from database " << curr->database << std::endl;
                break;
            default:
                std::cout << "Bad type: " << curr->type << std::endl;
                break;
        }
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        std::cerr << "Failed to read configuration" << std::endl;
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhim::Open(&hx, &opts) != HXHIM_SUCCESS) {
        std::cerr << "Failed to initialize hxhim" << std::endl;
        return 1;
    }

    // Generate some subject-predicate-object triples
    const std::size_t count = 10;
    void **subjects = NULL, **predicates = NULL, **objects = NULL;
    std::size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
    if (spo_gen_fixed(count, 100, rank, &subjects, &subject_lens, &predicates, &predicate_lens, &objects, &object_lens) != count) {
        return 1;
    }

    // PUT the subject-predicate-object triples
    for(std::size_t i = 0; i < count; i++) {
        hxhim::Put(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i], objects[i], object_lens[i]);
        std::cout << "Rank " << rank << " PUT {" << std::string((char *) subjects[i], subject_lens[i]) << ", " << std::string((char *) predicates[i], predicate_lens[i]) << "} -> " << std::string((char *) objects[i], object_lens[i]) << std::endl;
    }

    // GET them back, flushing only the GETs
    for(std::size_t i = 0; i < count; i++) {
        hxhim::Get(&hx, subjects[i], subject_lens[i], predicates[i], predicate_lens[i]);
    }
    hxhim::Results *flush_get_res = hxhim::FlushGets(&hx);
    std::cout << "GET before flushing PUTs" << std::endl;
    print_results(rank, flush_get_res);
    delete flush_get_res;

    // GET again, but flush everything this time
    hxhim::BGet(&hx, subjects, subject_lens, predicates, predicate_lens, count);
    hxhim::Results *flush_all_res = hxhim::Flush(&hx);
    std::cout << "GET after flushing PUTs" << std::endl;
    print_results(rank, flush_all_res);
    delete flush_all_res;
    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhim::Close(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
