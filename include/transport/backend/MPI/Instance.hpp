#ifndef TRANSPORT_MPI_INSTANCE_HPP
#define TRANSPORT_MPI_INSTANCE_HPP

#include "mpi.h"

namespace Transport {
namespace MPI {

/**
 * Singleton helper class that initializes MPI exactly once if it is not initialized
 */
class Instance {
    public:
        /** @return the unique instance of MPIInstance */
        static const Instance& instance();

        /** @return communicator for the HXHIM instance */
        MPI_Comm Comm() const { return instanceComm_; }

        /** @return processes rank in HXHIM instance */
        int Rank() const { return instanceRank_; }

        /** @return processes size in HXHIM instance */
        int Size() const { return instanceSize_; }

        /** @return processes rank in MPI_COMM_WORLD */
        int WorldRank() const { return worldRank_; }

        /** @return processes size in MPI_COMM_WORLD */
        int WorldSize() const { return worldSize_; }

    protected:
        /** Construct and initialize an MPI instance */
        Instance();

        /** Disable the copy constructor */
        Instance(const Instance& other) = default;

        /** Destructor called as part of atexit() */
        ~Instance();

    private:
        MPI_Comm instanceComm_;
        int instanceRank_;
        int instanceSize_;
        int wasInitializedHere_;
        int worldRank_;
        int worldSize_;
};

}
}

#endif
