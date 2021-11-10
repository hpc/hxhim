#include <algorithm>

#include "hxhim/Datastore.hpp"

/**
 * get_rank
 * Get the rank the datastore id would be on
 *
 * @param id                the datastore id
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the rank of the given ID or -1 on error
 */
int hxhim::Datastore::get_rank(const int id,
                               const std::size_t client_ratio,
                               const std::size_t server_ratio,
                               const std::size_t datastores_per_server) {
    if ((id < 0)      ||
        !client_ratio ||
        !server_ratio ||
        !datastores_per_server) {
        return -1;
    }

    const std::size_t rs  = id / datastores_per_server;

    // all ranks are servers, so ranks match range servers
    if (server_ratio >= client_ratio) {
        return rs;
    }

    return ((rs / server_ratio) * client_ratio) + (rs % server_ratio);
}

/**
 * get_rank
 * Get the rank the datastore id is on within a world of known size
 *
 * @param id                the datastore id
 * @param size              the MPI world size
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the rank of the given ID or -1 on error
 */
int hxhim::Datastore::get_rank(const int id,
                               const int size,
                               const std::size_t client_ratio,
                               const std::size_t server_ratio,
                               const std::size_t datastores_per_server) {
    const int rank = hxhim::Datastore::get_rank(id, client_ratio, server_ratio, datastores_per_server);
    if (rank >= size) {
        return -1;
    }

    return rank;
}

/**
 * get_id
 * Get the id of the datastore located on the given rank, if the rank has a range server
 *
 * @param rank              the MPI rank
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the mapping from the rank to the database ID, or -1 on error
 */
int hxhim::Datastore::get_id(const int rank,
                             const std::size_t offset,
                             const std::size_t client_ratio,
                             const std::size_t server_ratio,
                             const std::size_t datastores_per_server) {
    if ((rank < 0)    ||
        !client_ratio ||
        !server_ratio ||
        !datastores_per_server ||
        (offset >= datastores_per_server)) {
        return -1;
    }

    // all clients have servers
    if (server_ratio >= client_ratio) {
        return rank * datastores_per_server + offset;
    }

    // not a range server
    const std::size_t rs_offset = rank % client_ratio;
    if (rs_offset >= server_ratio) {
        return -1;
    }

    // whole "blocks" get server_ratio servers per block
    const std::size_t whole_blocks = rank / client_ratio;
    return (whole_blocks * server_ratio + rs_offset) * datastores_per_server + offset;
}

/**
 * get_id
 * Get the id of the datastore located on the given rank, if the world size is known
 *
 * @param rank              the MPI rank
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @return the mapping from the rank to the database ID, or -1 on error
 */
int hxhim::Datastore::get_id(const int rank,
                             const std::size_t offset,
                             const int size,
                             const std::size_t client_ratio,
                             const std::size_t server_ratio,
                             const std::size_t datastores_per_server) {
    if (rank >= size) {
        return -1;
    }

    return hxhim::Datastore::get_id(rank, offset, client_ratio, server_ratio, datastores_per_server);
}
