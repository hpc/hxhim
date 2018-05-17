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
    hxhim_session_t hx;
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
        switch (results->GetOp()) {
            case hxhim_work_op::HXHIM_PUT:
                {
                    std::cout << "PUT returned status " << results->GetError() << std::endl;
                    break;
                }
            case hxhim_work_op::HXHIM_GET:
                {
                    hxhim::GetReturn *get_results = dynamic_cast<hxhim::GetReturn *>(results);
                    for(get_results->MoveToFirstRS(); get_results->ValidRS() == HXHIM_SUCCESS; get_results->NextRS()) {
                        if (get_results->GetError() == HXHIM_SUCCESS) {
                            for(get_results->MoveToFirstKV(); get_results->ValidKV() == HXHIM_SUCCESS; get_results->NextKV()) {
                                char *key, *value;
                                std::size_t key_len, value_len;
                                if (get_results->GetKV((void **) &key, &key_len, (void **) &value, &value_len) == HXHIM_SUCCESS) {
                                    std::cout << "Get " << std::string(key, key_len) << " -> " << std::string(value, value_len) << std::endl;
                                }
                            }
                        }
                        else {
                            std::cout << "Get failed for database " << get_results->GetSrc() << std::endl;
                        }
                    }
                }
                break;
            case hxhim_work_op::HXHIM_DEL:
                {
                    std::cout << "DEL returned status " << results->GetError() << std::endl;
                }
                break;
            case hxhim_work_op::HXHIM_NOP:
            default:
                break;
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
