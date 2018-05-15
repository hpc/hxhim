#ifndef MDHIM_TRANSPORT_MPI_INSTANCE_HPP
#define MDHIM_TRANSPORT_MPI_INSTANCE_HPP

#include <iostream>

#include "mpi.h"

/**
 * Singleton helper class that initializes MPI exactly once if it is not initialized
 */
class MPIInstance {
    public:
        /** @return the unique instance of MPIInstance */
        static const MPIInstance& instance();

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
        MPIInstance();

        /** Disable the copy constructor */
        MPIInstance(const MPIInstance& other) = default;

        /** Destructor called as part of atexit() */
        ~MPIInstance();

    private:
        MPI_Comm instanceComm_;
        int instanceRank_;
        int instanceSize_;
        int wasInitializedHere_;
        int worldRank_;
        int worldSize_;
};

#endif
