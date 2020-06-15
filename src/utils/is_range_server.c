#include "utils/is_range_server.hpp"

/**
 * is_range_server
 *
 * @param rank              the rank of the process
 * @param client_ratio      the client portion of the client to server ratio
 * @param server_ratio      the server portion of the client to server ratio
 * @param -1 on error, 0 if not range server, 1 if range server
 */
int is_range_server(const int rank, const size_t client_ratio, const size_t server_ratio) {
    if ((rank < 0)    ||
        !client_ratio ||
        !server_ratio) {
        return -1;
    }

    return ((rank % client_ratio) < server_ratio);
}
