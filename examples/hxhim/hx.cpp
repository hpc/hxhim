#include <iostream>
#include <sstream>

#include <mpi.h>

#include "hxhim.hpp"
#include "spo_gen.h"

static void print_results(const int rank, hxhim::Return *results) {
    while (results) {
        // iterate through each range server
        for(results->MoveToFirstRS(); results->ValidRS() == HXHIM_SUCCESS; results->NextRS()) {
            // iterate through each key value pair
            for(results->MoveToFirstSPO(); results->ValidSPO() == HXHIM_SUCCESS; results->NextSPO()) {
                char *subject = nullptr; std::size_t subject_len = 0;
                char *predicate = nullptr; std::size_t predicate_len = 0;
                results->GetSPO((void **) &subject, &subject_len, (void **) &predicate, &predicate_len, nullptr, nullptr);
                std::cout << "Rank " << rank << " GET {" << std::string(subject, subject_len) << ", " << std::string(predicate, predicate_len) << "} ";
                if (results->GetError() == HXHIM_SUCCESS) {
                    char *object = nullptr; std::size_t object_len = 0;
                    results->GetSPO(nullptr, nullptr, nullptr, nullptr, (void **) &object, &object_len);
                    std::cout << "-> " << std::string(object, object_len);
                }
                else {
                    std::cout << "failed";
                }
                std::cout << " on range server " << results->GetSrc() << std::endl;
            }
        }

        results = results->Next();
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // start hxhim
    hxhim_t hx;
    if (hxhim::Open(&hx, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        std::cerr << "Failed to initialize hxhim" << std::endl;
        return 1;
    }

    // Generate some subject-predicate-object triples
    const std::size_t count = 10;
    void **subjects = NULL, **predicates = NULL, **objects = NULL;
    std::size_t *subject_lens = NULL, *predicate_lens = NULL, *object_lens = NULL;
    if (spo_gen_fixed(count, 100, rank, &subjects, &subject_lens, &predicates, &predicate_lens, &objects, &object_lens) != count) {
        return -1;
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
    hxhim::Return *flush_get_res = hxhim::FlushGets(&hx);
    std::cout << "GET before flushing PUTs" << std::endl;
    print_results(rank, flush_get_res);
    delete flush_get_res;

    // GET again, but flush everything this time
    hxhim::BGet(&hx, subjects, subject_lens, predicates, predicate_lens, count);
    hxhim::Return *flush_all_res = hxhim::Flush(&hx);
    std::cout << "GET after flushing PUTs" << std::endl;
    print_results(rank, flush_all_res);
    delete flush_all_res;
    spo_clean(count, subjects, subject_lens, predicates, predicate_lens, objects, object_lens);

    MPI_Barrier(MPI_COMM_WORLD);

    hxhim::Close(&hx);
    MPI_Finalize();

    return 0;
}
