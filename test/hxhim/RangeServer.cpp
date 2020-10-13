#include <gtest/gtest.h>

#include "hxhim/RangeServer.hpp"

static const std::size_t LARGER  = 7;
static const std::size_t SMALLER = 3;

static_assert(LARGER > SMALLER);

/**
 * world_size is set to clients * 2 + servers + 1
 * when testing with clients > servers in order to
 * capture all cases:
 *
 *     - up to 1 full block of servers and/or non-servers
 *     - 2 full blocks
 *     - 1 partial block of servers and 1 non-server
 */

TEST(RangeServer, is_range_server) {
    // unknown world size
    {
        EXPECT_EQ(hxhim::RangeServer::is_range_server(-1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::is_range_server( 0, 0, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::is_range_server( 0, 1, 0), -1);

        // same clients to servers
        {
            for(std::size_t rank = 0; rank < 10; rank++) {
                const int csr = rand();
                EXPECT_EQ(hxhim::RangeServer::is_range_server(rank, csr, csr), 1);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t rank = 0; rank < servers; rank++) {
                    EXPECT_EQ(hxhim::RangeServer::is_range_server(block * clients + rank, clients, servers),
                              1);
                }
                for(std::size_t rank = servers; rank < clients; rank++) {
                    EXPECT_EQ(hxhim::RangeServer::is_range_server(block * clients + rank, clients, servers),
                              0);
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t rank = 0; rank < servers; rank++) {
                    EXPECT_EQ(hxhim::RangeServer::is_range_server(block * servers + rank, clients, servers),
                              1);
                }
            }
        }
    }

    // known world size
    {
        EXPECT_EQ(hxhim::RangeServer::is_range_server(-1, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::is_range_server( 1, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::is_range_server( 0, 0, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::is_range_server( 0, 1, 0, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::is_range_server( 0, 1, 1, 0), -1);

        // same clients to servers
        {
            const std::size_t ratio = LARGER;

            const int world_size = ratio * 2 + 1;
            for(int rank = 0; rank < world_size; rank++) {
                EXPECT_EQ(hxhim::RangeServer::is_range_server(rank, world_size, ratio, ratio),
                          1);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            const int world_size = clients * 2 + servers + 1;
            for(int rank = 0; rank < world_size; rank++) {
                EXPECT_EQ(hxhim::RangeServer::is_range_server(rank, world_size, clients, servers),
                          (rank % clients) < servers);
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;

            const int world_size = clients * 2 + servers + 1;
            for(int rank = 0; rank < world_size; rank++) {
                EXPECT_EQ(hxhim::RangeServer::is_range_server(rank, world_size, clients, servers),
                          1);
            }
        }
    }
}

TEST(RangeServer, get_rank) {
    // unknown world size
    {
        EXPECT_EQ(hxhim::RangeServer::get_rank(-1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_rank( 0, 0, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_rank( 0, 1, 0), -1);

        // same clients to servers
        // id matches rank
        {
            for(std::size_t id = 0; id < 10; id++) {
                const int csr = rand();
                EXPECT_EQ(hxhim::RangeServer::get_rank(id, csr, csr), id);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t id = 0; id < servers; id++) {
                    EXPECT_EQ(hxhim::RangeServer::get_rank(block * servers + id, clients, servers),
                              block * clients + id);
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t id = 0; id < servers; id++) {
                    EXPECT_EQ(hxhim::RangeServer::get_rank(block * servers + id, clients, servers),
                              block * servers + id);
                }
            }
        }
    }

    // known world size
    {
        EXPECT_EQ(hxhim::RangeServer::get_rank(-1, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_rank( 0, 0, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_rank( 0, 1, 0, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_rank( 0, 1, 1, 0), -1);
        EXPECT_EQ(hxhim::RangeServer::get_rank( 1, 1, 1, 1), -1);

        // same clients to servers
        {
            const std::size_t ratio = LARGER;
            const int world_size = ratio * 2 + 1;

            for(std::size_t id = 0; id < world_size; id++) {
                EXPECT_EQ(hxhim::RangeServer::get_rank(id, world_size, ratio, ratio), id);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;
            const int world_size = clients * 2 + servers + 1;

            const std::size_t max_id = ((world_size / clients) * servers) +
                std::min(world_size % clients, servers);

            for(std::size_t id = 0; id < max_id; id++) {
                EXPECT_EQ(hxhim::RangeServer::get_rank(id, world_size, clients, servers),
                          (id / servers) * clients + (id % servers));
            }
            for(std::size_t id = max_id; id < world_size; id++) {
                EXPECT_EQ(hxhim::RangeServer::get_rank(id, world_size, clients, servers),
                          -1);
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;
            const int world_size = clients * 2 + servers + 1;

            for(std::size_t id = 0; id < world_size; id++) {
                EXPECT_EQ(hxhim::RangeServer::get_rank(id, world_size, clients, servers),
                          id);
           }
        }
    }
}

TEST(RangeServer, get_id) {
    // unknown world size
    {
        EXPECT_EQ(hxhim::RangeServer::get_id(-1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_id( 0, 0, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_id( 0, 1, 0), -1);

        // same clients to servers
        // rank matches id
        {
            for(std::size_t rank = 0; rank < 10; rank++) {
                const int csr = rand();
                EXPECT_EQ(hxhim::RangeServer::get_id(rank, csr, csr), rank);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t rank = 0; rank < servers; rank++) {
                    EXPECT_EQ(hxhim::RangeServer::get_id(block * clients + rank, clients, servers),
                              block * servers + rank);
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t rank = 0; rank < servers; rank++) {
                    EXPECT_EQ(hxhim::RangeServer::get_id(block * clients + rank, clients, servers),
                              block * clients + rank);
                }
            }
        }
    }

    // known world size
    {
        EXPECT_EQ(hxhim::RangeServer::get_id(-1, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_id( 0, 0, 1, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_id( 0, 1, 0, 1), -1);
        EXPECT_EQ(hxhim::RangeServer::get_id( 0, 1, 1, 0), -1);
        EXPECT_EQ(hxhim::RangeServer::get_id( 1, 1, 1, 1), -1);

        // same clients to servers
        {
            const std::size_t ratio = LARGER;
            const int world_size = ratio * 2 + 1;

            for(int rank = 0; rank < world_size; rank++) {
                EXPECT_EQ(hxhim::RangeServer::get_id(rank, world_size, ratio, ratio), rank);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            const int world_size = (clients * 2) + servers + 1;
            const std::size_t blocks = (world_size / clients) + (bool) (world_size % clients);

            for(std::size_t block = 0; block < blocks; block++) {
                for(std::size_t offset = 0; offset < servers; offset++) {
                    const int rank = block * clients + offset;
                    if (rank >= world_size) {
                        break;
                    }

                    const int id = block * servers + offset;
                    EXPECT_EQ(hxhim::RangeServer::get_id(rank, world_size, clients, servers),
                              id);
                }

                for(std::size_t offset = servers; offset < clients; offset++) {
                    const int rank = block * clients + offset;
                    if (rank >= world_size) {
                        break;
                    }

                    EXPECT_EQ(hxhim::RangeServer::get_id(rank, world_size, clients, servers),
                              -1);
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;
            const int world_size = (clients * 2) + servers + 1;

            for(int rank = 0; rank < world_size; rank++) {
                EXPECT_EQ(hxhim::RangeServer::get_id(rank, world_size, clients, servers),
                          rank);
            }
        }
    }
}
