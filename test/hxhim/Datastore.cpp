#include <gtest/gtest.h>

#include "hxhim/Datastore.hpp"

static const std::size_t LARGER  = 7;
static const std::size_t SMALLER = 3;
static const std::size_t MAX_DATASTORES_PER_SERVER = 5;

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

TEST(Datastore, get_rank) {
    std::size_t dsps;
    do {
        dsps = rand() % MAX_DATASTORES_PER_SERVER;
    } while (dsps < 2);

    // unknown world size
    {
        EXPECT_EQ(hxhim::Datastore::get_rank(-1, 1, 1, dsps), -1);
        EXPECT_EQ(hxhim::Datastore::get_rank( 0, 0, 1, dsps), -1);
        EXPECT_EQ(hxhim::Datastore::get_rank( 0, 1, 0, dsps), -1);

        // 1:1 clients: servers
        // id matches rank
        {
            for(std::size_t ds_id = 0; ds_id < (dsps * 2 + 1); ds_id++) {
                const int csr = rand();
                EXPECT_EQ(hxhim::Datastore::get_rank(ds_id, csr, csr, dsps), ds_id / dsps);
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t block_rs = 0; block_rs < servers; block_rs++) {
                    const int rank = block * clients + block_rs;
                    const int rs = block * servers + block_rs;
                    for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                        const int ds_id = rs * dsps + ds_offset;
                        EXPECT_EQ(hxhim::Datastore::get_rank(ds_id, clients, servers, dsps),
                                  rank);
                    }
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t block_rs = 0; block_rs < servers; block_rs++) {
                    const int rs = block * servers + block_rs;
                    for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                        const int ds_id = rs * dsps + ds_offset;
                        EXPECT_EQ(hxhim::Datastore::get_rank(ds_id, clients, servers, dsps),
                                  rs);
                    }
                }
            }
        }
    }

    // known world size
    {
        EXPECT_EQ(hxhim::Datastore::get_rank(-1, 1, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_rank( 0, 0, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_rank( 0, 1, 0, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_rank( 0, 1, 1, 0, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_rank( 1, 1, 1, 1, 1), -1);

        // same clients to servers
        {
            const std::size_t ratio = LARGER;
            const int world_size = ratio * 2 + 1;

            for(std::size_t rs = 0; rs < world_size; rs++) {
                const int ds0_id = rs * dsps;
                for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                    EXPECT_EQ(hxhim::Datastore::get_rank(ds0_id + ds_offset, world_size, ratio, ratio, dsps), rs);
                }
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;
            const int world_size = clients * 2 + servers + 1;

            for(int block = 0; block < 2; block++) {
                for(std::size_t block_rs = 0; block_rs < servers; block_rs++) {
                    const int rank = block * clients + block_rs;
                    const int rs = block * servers + block_rs;
                    for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                        const int ds_id = rs * dsps + ds_offset;
                        EXPECT_EQ(hxhim::Datastore::get_rank(ds_id, world_size, clients, servers, dsps),
                                  (rank < world_size)?rank:-1);
                    }
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;
            const int world_size = clients * 2 + servers + 1;

            for(int block = 0; block < 2; block++) {
                for(std::size_t block_rs = 0; block_rs < servers; block_rs++) {
                    const int rs = block * servers + block_rs;
                    for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                        const int ds_id = rs * dsps + ds_offset;
                        EXPECT_EQ(hxhim::Datastore::get_rank(ds_id, clients, servers, dsps),
                                  (rs < world_size)?rs:-1);
                    }
                }
            }
        }
    }
}

TEST(Datastore, get_id) {
    std::size_t dsps;
    do {
        dsps = rand() % MAX_DATASTORES_PER_SERVER;
    } while (dsps < 2);

    // unknown world size
    {
        EXPECT_EQ(hxhim::Datastore::get_id(-1, 0, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_id( 0, 0, 0, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_id( 0, 0, 1, 0, 1), -1);

        // 1:1 clients:servers
        // rank matches id
        {
            for(std::size_t rank = 0; rank < 10; rank++) {
                for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                    const int csr = rand();
                    EXPECT_EQ(hxhim::Datastore::get_id(rank, ds_offset, csr, csr, dsps), rank * dsps + ds_offset);
                }
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t block_rs = 0; block_rs < clients; block_rs++) {
                    const int rank = block * clients + block_rs;
                    const int rs = block * servers + block_rs;
                    const int ds0 = rs * dsps;
                    for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                        const int ds_id = ds0 + ds_offset;
                        EXPECT_EQ(hxhim::Datastore::get_id(rank, ds_offset, clients, servers, dsps),
                                  (rank % clients < servers)?ds_id:-1);
                    }
                }
            }
        }

        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;

            for(int block = 0; block < 2; block++) {
                for(std::size_t block_rs = 0; block_rs < servers; block_rs++) {
                    const int rank = block * clients + block_rs;
                    const int ds0 = rank * dsps;
                    for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                        EXPECT_EQ(hxhim::Datastore::get_id(rank, ds_offset, clients, servers, dsps),
                                  ds0 + ds_offset);
                    }
                }
            }
        }
    }

    // known world size
    {
        EXPECT_EQ(hxhim::Datastore::get_id(-1, 0, 1, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_id( 0, 0, 0, 1, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_id( 0, 0, 1, 0, 1, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_id( 0, 0, 1, 1, 0, 1), -1);
        EXPECT_EQ(hxhim::Datastore::get_id( 1, 0, 1, 1, 1, 1), -1);

        // 1:1 clients:servers
        {
            const std::size_t ratio = LARGER;
            const int world_size = ratio * 2 + 1;

            for(int rank = 0; rank < world_size; rank++) {
                for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                    const int csr = rand();
                    EXPECT_EQ(hxhim::Datastore::get_id(rank, ds_offset, csr, csr, dsps), rank * dsps + ds_offset);
                }
            }
        }

        // more clients than servers
        {
            const std::size_t clients = LARGER;
            const std::size_t servers = SMALLER;
            const int world_size = (clients * 2) + servers + 1;

            for(int rank = 0; rank < world_size; rank++) {
                for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                    const int block = rank / clients;
                    const int block_rs = rank % clients;
                    const int ds_id = (block * servers + block_rs) * dsps + ds_offset;
                    EXPECT_EQ(hxhim::Datastore::get_id(rank, ds_offset, world_size, clients, servers, dsps),
                              (rank % clients < servers)?ds_id:-1);
                }
            }
        }


        // more servers than clients
        {
            const std::size_t clients = SMALLER;
            const std::size_t servers = LARGER;
            const int world_size = (clients * 2) + servers + 1;

            for(int rank = 0; rank < world_size; rank++) {
                for(std::size_t ds_offset = 0; ds_offset < dsps; ds_offset++) {
                    const int ds_id = rank * dsps + ds_offset;
                    EXPECT_EQ(hxhim::Datastore::get_id(rank, ds_offset, world_size, clients, servers, dsps),
                              ds_id);
                }
            }
        }
    }
}
