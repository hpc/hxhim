#include <iostream>

#include "transport/MPI/Instance.hpp"

namespace Transport {
namespace MPI {

/** Singleton accessor */
const Instance& Instance::instance() {
    static Instance instance;
    return instance;
}

/** Constructor */
Instance::Instance() {
    int mpiIsInitialized = 0;

    // If mpi is not initialized, try to initialize it
    int rc = MPI_Initialized(&mpiIsInitialized);
    if (rc == 0 && !mpiIsInitialized) {
        // Retrieve the command line arguments (not portable)
        int argc = 0;
        char **argv = nullptr;
        //GetCommandLineArguments(&argc, &argv);

        // Perform initialization
        int provided = 0;
        rc = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
        if (rc == 0 && provided >= MPI_THREAD_MULTIPLE) {
            wasInitializedHere_ = 1;
            mpiIsInitialized = 1;
        }
        else {
            std::cerr << __FILE__ << ":" << __LINE__
                      << ":MPI Initialization failed:" << rc << "-" << provided << std::endl;
        }
    }

    // If initialization succeeded, finish retrieving runtime info
    if (mpiIsInitialized) {
        // Retrieve world rank and size
        worldRank_ = worldSize_ = -1;
        rc += MPI_Comm_rank(MPI_COMM_WORLD, &worldRank_);
        rc += MPI_Comm_size(MPI_COMM_WORLD, &worldSize_);
        if (rc != 0)
            std::cerr << __FILE__ << ":" << __LINE__ << ":Failed getting rank and size" << std::endl;

        // Create a communicator so that all address spaces join it
        instanceComm_ = MPI_COMM_WORLD;
        instanceRank_ = worldRank_;
        instanceSize_ = worldSize_;
    }
}

/** Destructor */
Instance::~Instance() {
    if (wasInitializedHere_)
        MPI_Finalize();
}

}
}
