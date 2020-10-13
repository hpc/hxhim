#include <algorithm>

#include "hxhim/RangeServer.hpp"

/**
 * is_range_server
 * Whether or not a rank would be a range server, if it existed
 *
 * @param rank              the MPI rank of the process
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @param -1 on error, 0 if not range server, 1 if range server
 */
int hxhim::RangeServer::is_range_server(const int rank,
                                        const std::size_t client_ratio,
                                        const std::size_t server_ratio) {
    if ((rank < 0)    ||
        !client_ratio ||
        !server_ratio) {
        return -1;
    }

    return ((rank % client_ratio) < server_ratio);
}

/**
 * is_range_server
 * Whether or not a rank in a world with a known size is a range server
 *
 * @param rank              the MPI rank of the process
 * @param size              the MPI world size
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @param -1 on error, 0 if not range server, 1 if range server
 */
int hxhim::RangeServer::is_range_server(const int rank,
                                        const int size,
                                        const std::size_t client_ratio,
                                        const std::size_t server_ratio) {
    if (rank >= size) {
        return -1;
    }

    return hxhim::RangeServer::is_range_server(rank, client_ratio, server_ratio);
}

/**
 * get_rank
 * Get the rank the datastore id would be on
 *
 * @param id                the range server id
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the rank of the given ID or -1 on error
 */
int hxhim::RangeServer::get_rank(const int id,
                                 const std::size_t client_ratio,
                                 const std::size_t server_ratio) {
    if ((id < 0)      ||
        !client_ratio ||
        !server_ratio) {
        return -1;
    }

    // all ranks are clients, so ids match ranks
    if (server_ratio >= client_ratio) {
        return id;
    }

    const std::size_t block  = id / server_ratio;
    const std::size_t offset = id % server_ratio;
    return block * client_ratio + offset;
}

/**
 * get_rank
 * Get the rank the datastore id is on within a world of known size
 *
 * @param id                the range server id
 * @param size              the MPI world size
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the rank of the given ID or -1 on error
 */
int hxhim::RangeServer::get_rank(const int id,
                                 const int size,
                                 const std::size_t client_ratio,
                                 const std::size_t server_ratio) {
    const int rank = hxhim::RangeServer::get_rank(id, client_ratio, server_ratio);
    if (rank >= size) {
        return -1;
    }

    return rank;
}

/**
 * get_id
 * Get the id of the datastore located on the given rank, if the rank has a range server
 *
 * @param rank              the range server rank
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the mapping from the rank to the database ID, or -1 on error
 */
int hxhim::RangeServer::get_id(const int rank,
                               const std::size_t client_ratio,
                               const std::size_t server_ratio) {
    if ((rank < 0)    ||
        !client_ratio ||
        !server_ratio) {
        return -1;
    }

    // all ranks are clients, so ids match ranks
    if (server_ratio >= client_ratio) {
        return rank;
    }

    const std::size_t block_offset = rank % client_ratio;
    if (block_offset >= server_ratio) {
        return -1;
    }

    // whole "blocks" get server_ratio servers per block
    const std::size_t whole_blocks = rank / client_ratio;
    return whole_blocks * server_ratio + block_offset;
}

/**
 * get_id
 * Get the id of the datastore located on the given rank, if the world size is known
 *
 * @param rank              the range server rank
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the mapping from the rank to the database ID, or -1 on error
 */
int hxhim::RangeServer::get_id(const int rank,
                               const int size,
                               const std::size_t client_ratio,
                               const std::size_t server_ratio) {
    if (rank >= size) {
        return -1;
    }

    return hxhim::RangeServer::get_id(rank, client_ratio, server_ratio);
}
