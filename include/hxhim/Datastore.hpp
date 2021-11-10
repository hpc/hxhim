#ifndef HXHIM_DATASTORE_HPP
#define HXHIM_DATASTORE_HPP

#include <cstddef>

#include "hxhim/hxhim.hpp"

namespace hxhim {

/** This namespace contains utility functions, not data structures */
namespace Datastore {

/**
 * Each range server can have multiple datastores connected to it.
 *
 * e.g.
 *     Client : Server = 5 : 3
 *     Datastores per server: 3
 *     Block:    |                   0                        |                     1                     |
 *     MPI Rank: |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     Client:   |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     RS:       |   0   |   1   |   2   |         |          |    3     |    4     |    5     |          |
 *     DS:       | 0,1,2 | 3,4,5 | 6,7,8 |         |          | 9,10,11  | 12,13,14 | 15,16,17 |          |
 *
 *     Client : Server = 3 : 5
 *     Datastores per server: 3
 *     Block:    |                   0                        |                     1                     |
 *     MPI Rank: |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     Client:   |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     RS:       |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     DS:       | 0,1,2 | 3,4,5 | 6,7,8 | 9,10,11 | 12,13,14 | 15,16,17 | 18,19,20 | 21,22,23 | 24,25,26 |
 */

// These functions assume an infinite MPI world size
int get_rank(const int id,
             const std::size_t client_ratio,
             const std::size_t server_ratio,
             const std::size_t datastores_per_server);

int get_id(const int rank,
           const std::size_t offset,
           const std::size_t client_ratio,
           const std::size_t server_ratio,
           const std::size_t datastores_per_server);

// These functions require a known finite MPI world size
int get_rank(const int id,
             const int size,
             const std::size_t client_ratio,
             const std::size_t server_ratio,
             const std::size_t datastores_per_server);

int get_id(const int rank,
           const std::size_t offset,
           const int size,
           const std::size_t client_ratio,
           const std::size_t server_ratio,
           const std::size_t datastores_per_server);

}
}

#endif
