#ifndef RANGE_SERVER_HPP
#define RANGE_SERVER_HPP

#include <cstddef>

namespace hxhim {

/** This namespace is utility functions, not data structures */
namespace RangeServer {

/**
 * Range servers are controlled by the CLIENT_RATIO and SERVER_RATIO
 * configuration keys. max(CLIENT_RATIO, SERVER_RATIO) defines a "block"
 * of MPI ranks, where the first C ranks are clients and the first S
 * ranks are servers. This is true even when there are not enough MPI
 * ranks to complete a  "block".
 *
 * e.g.
 *     Client : Server = 5 : 3
 *     MPI Rank:    |  0  |  1  |  2  |  3  |  4  |  5  |  6  |
 *     Client:      |  0  |  1  |  2  |  3  |  4  |  5  |  6  |
 *     RangeServer: |  0  |  1  |  2  |     |     |  3  |  4  |
 *
 *     Client : Server = 3 : 5
 *     MPI Rank:    |  0  |  1  |  2  |  3  |  4  |  5  |  6  |
 *     Client:      |  0  |  1  |  2  |     |     |  5  |  6  |
 *     RangeServer: |  0  |  1  |  2  |  3  |  4  |  5  |  6  |
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

}
}

#endif
