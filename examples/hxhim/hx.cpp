#include <iostream>
#include <sstream>

#include <mpi.h>

#include "hxhim.hpp"

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    const std::size_t count = 10;

    // Generate some key value pairs
    std::pair<std::string, std::string> kvs[count];
    for(std::size_t i = 0; i < count; i++) {
        std::stringstream key, value;
        key << "key" << rank << i;
        value << "value" << rank << i;
        kvs[i].first = key.str();
        kvs[i].second = value.str();
    }

    // start hxhim
    hxhim_t hx;
    hxhim::Open(&hx, MPI_COMM_WORLD, "mdhim.conf");

    // PUT the key value pairs into MDHIM
    for(std::pair<std::string, std::string> const & kv : kvs) {
        hxhim::Put(&hx, (void *)kv.first.c_str(), kv.first.size(), (void *)kv.second.c_str(), kv.second.size());
    }

    // GET them back
    for(std::pair<std::string, std::string> const & kv : kvs) {
        hxhim::Get(&hx, (void *)kv.first.c_str(), kv.first.size());
    }

    // DEL the key value pairs
    for(std::pair<std::string, std::string> const & kv : kvs) {
        hxhim::Delete(&hx, (void *)kv.first.c_str(), kv.first.size());
    }

    // try to GET the key value pairs again
    for(std::pair<std::string, std::string> const & kv : kvs) {
        hxhim::Get(&hx, (void *)kv.first.c_str(), kv.first.size());
    }

    // perform the queued operations
    hxhim::Return *results = hxhim::Flush(&hx);

    // iterate through the results
    while (results) {
        for(results->MoveToFirstRS(); results->ValidRS() == HXHIM_SUCCESS; results->NextRS()) {
            switch (results->GetOp()) {
                case hxhim_work_op::HXHIM_PUT:
                    std::cout << "Rank " << rank << " PUT returned status " << results->GetError() << " on range server " << results->GetSrc() << std::endl;
                    break;
                case hxhim_work_op::HXHIM_GET:
                    if (results->GetError() == HXHIM_SUCCESS) {
                        for(results->MoveToFirstKV(); results->ValidKV() == HXHIM_SUCCESS; results->NextKV()) {
                            char *key, *value;
                            std::size_t key_len, value_len;
                            if (results->GetKV((void **) &key, &key_len, (void **) &value, &value_len) == HXHIM_SUCCESS) {
                                std::cout << "Rank " << rank << " GET " << std::string(key, key_len) << " -> " << std::string(value, value_len) << " on range server " << results->GetSrc() << std::endl;
                            }
                        }
                    }
                    else {
                        std::cout << "Rank " << rank << " GET failed on range server " << results->GetSrc() << std::endl;
                    }
                    break;
                case hxhim_work_op::HXHIM_DEL:
                    std::cout << "Rank " << rank << " DEL returned status " << results->GetError() << " on range server " << results->GetSrc() << std::endl;
                    break;
                default:
                    break;
            }
        }

        hxhim::Return *next = results->Next();
        delete results;
        results = next;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    hxhim::Close(&hx);
    MPI_Finalize();

    return 0;
}
