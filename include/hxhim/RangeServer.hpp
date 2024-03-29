#ifndef RANGE_SERVER_HPP
#define RANGE_SERVER_HPP

#include <cstddef>

#include "hxhim/hxhim.hpp"

namespace hxhim {

/** This namespace contains utility functions, not data structures */
namespace RangeServer {

/**
 * Range servers are controlled by the CLIENT_RATIO and SERVER_RATIO
 * configuration keys. max(CLIENT_RATIO, SERVER_RATIO) defines a "block"
 * of MPI ranks, where the first C ranks are clients and the first S
 * ranks are servers. This is true even when there are not enough MPI
 * ranks to complete a "block". CLIENT_RATIO < SERVER_RATIO is treated
 * as CLIENT_RATIO == SERVER_RATIO, since MPI ranks are not prevented
 * from client operations.
 *
 * e.g.
 *     Client : Server = 5 : 3
 *     Block:    |                   0                        |                     1                     |
 *     MPI Rank: |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     Client:   |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     RS:       |   0   |   1   |   2   |         |          |    3     |    4     |    5     |          |
 *
 *     Client : Server = 3 : 5
 *     Block:    |                   0                        |                     1                     |
 *     MPI Rank: |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     Client:   |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 *     RS:       |   0   |   1   |   2   |    3    |    4     |    5     |    6     |    7     |    8     |
 */

// These functions assume an infinite MPI world size
int is_range_server(const int rank,
                    const std::size_t client_ratio,
                    const std::size_t server_ratio);

int get_rank(const int id,
             const std::size_t client_ratio,
             const std::size_t server_ratio);

int get_id(const int rank,
           const std::size_t client_ratio,
           const std::size_t server_ratio);

// These functions require a known finite MPI world size
int is_range_server(const int rank,
                    const int size,
                    const std::size_t client_ratio,
                    const std::size_t server_ratio);

int get_rank(const int id,
             const int size,
             const std::size_t client_ratio,
             const std::size_t server_ratio);

int get_id(const int rank,
           const int size,
           const std::size_t client_ratio,
           const std::size_t server_ratio);

int is_range_server(hxhim_t *hx);

}
}

#endif
