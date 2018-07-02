#include <iomanip>
#include <sstream>

#include <gtest/gtest.h>

#include "mdhim/config.hpp"
#include "mdhim/mdhim.h"
#include "mdhim/mdhim_options_private.h"
#include "transport/MPIOptions.hpp"
#include "transport/ThalliumOptions.hpp"
#include "utils/Configuration.hpp"

// Context for reading configuration from a string
class ConfigString : public ConfigReader {
    public:
        ConfigString(const std::string &src)
            : ConfigReader(),
              src_(src)
        {}

        bool process(Config &config) const {
            std::stringstream s(src_);
            std::string key, value;
            while (s >> key >> value) {
                config[key] = value;
            }

            return true;
        }

    private:
        std::string src_;
};

TEST(mdhimConfig, string) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->db, nullptr);
    ASSERT_EQ(opts.p->db->path, nullptr);

    ConfigSequence seq;

    // set initial value
    const std::string db_path1 = "db_path1";
    ConfigString str1(DB_PATH + " " + db_path1);
    seq.add(&str1);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    ASSERT_NE(opts.p->db->path, nullptr);
    EXPECT_EQ(std::string(opts.p->db->path, db_path1.size()), db_path1);

    // remove first reader
    seq.reset();

    // overwrite old value
    const std::string db_path2 = "db_path2";
    ConfigString str2(DB_PATH + " " + db_path2);
    seq.add(&str2);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    ASSERT_NE(opts.p->db->path, nullptr);
    EXPECT_EQ(std::string(opts.p->db->path, db_path2.size()), db_path2);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimConfig, integral) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->db, nullptr);
    ASSERT_EQ(opts.p->db->rserver_factor, 0);

    ConfigSequence seq;

    // set initial value
    const int rserver_factor1 = 123;
    std::stringstream s1;
    s1 << rserver_factor1;
    ConfigString str1(RSERVER_FACTOR + " " + s1.str());
    seq.add(&str1);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    EXPECT_EQ(opts.p->db->rserver_factor, rserver_factor1);

    // remove first reader
    seq.reset();

    // overwrite value
    const int rserver_factor2 = 123;
    std::stringstream s2;
    s2 << rserver_factor2;
    ConfigString str2(RSERVER_FACTOR + " " + s2.str());
    seq.add(&str2);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    EXPECT_EQ(opts.p->db->rserver_factor, rserver_factor2);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimConfig, boolean) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->db, nullptr);
    ASSERT_EQ(opts.p->db->create_new, 0);

    ConfigSequence seq;

    // set initial value
    const bool create_new_db1 = true;
    std::stringstream s1;
    s1 << std::boolalpha << create_new_db1;
    ConfigString str1(CREATE_NEW_DB + " " + s1.str());
    seq.add(&str1);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    EXPECT_EQ(opts.p->db->create_new, create_new_db1);

    // remove first reader
    seq.reset();

    // overwrite value
    const bool create_new_db2 = false;
    std::stringstream s2;
    s2 << std::boolalpha << create_new_db2;
    ConfigString str2(CREATE_NEW_DB + " " + s2.str());
    seq.add(&str2);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    EXPECT_EQ(opts.p->db->create_new, create_new_db2);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimConfig, mpi) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->transport, nullptr);
    ASSERT_EQ(opts.p->transport->transport_specific, nullptr);

    ConfigSequence seq;
    std::stringstream s;

    // use MPI, but missing MEMORY_ALLOC_SIZE and MEMORY_REGIONS
    const bool use_mpi = true;
    s << USE_MPI << " " << std::boolalpha << use_mpi << " NUM_LISTENERS 1" << std::endl;
    ConfigString str1(s.str());
    seq.add(&str1);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_ERROR);
    EXPECT_EQ(opts.p->transport->transport_specific, nullptr);

    // remove first reader
    seq.reset();

    // use MPI, and set MEMORY_ALLOC_SIZE and MEMORY_REGIONS
    const int alloc_size = 128;
    const int regions = 256;
    s << MEMORY_ALLOC_SIZE << " " << alloc_size << std::endl << MEMORY_REGIONS << " " << regions << " NUM_LISTENERS 1" << std::endl;
    ConfigString str2(s.str());
    seq.add(&str2);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    ASSERT_NE(opts.p->transport->transport_specific, nullptr);

    // check the values stored into the transport specific data
    MPIOptions *mpi_opts = nullptr;
    ASSERT_NE((mpi_opts = dynamic_cast<MPIOptions *>(opts.p->transport->transport_specific)), nullptr);
    EXPECT_EQ(mpi_opts->type_, MDHIM_TRANSPORT_MPI);
    EXPECT_EQ(mpi_opts->alloc_size_, alloc_size);
    EXPECT_EQ(mpi_opts->regions_, regions);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimConfig, thallium) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->transport, nullptr);
    ASSERT_EQ(opts.p->transport->transport_specific, nullptr);

    ConfigSequence seq;
    std::stringstream s;

    // use thallium, but missing THALLIUM_MODULE
    const bool use_thallium = true;
    s << USE_THALLIUM << " " << std::boolalpha << use_thallium << std::endl;
    ConfigString str1(s.str());
    seq.add(&str1);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_ERROR);
    EXPECT_EQ(opts.p->transport->transport_specific, nullptr);

    // remove first reader
    seq.reset();

    // use thallium, and set THALLIUM_MODULE
    const std::string module = "tcp";
    s << THALLIUM_MODULE << " " << module;
    ConfigString str2(s.str());
    seq.add(&str2);
    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    ASSERT_NE(opts.p->transport->transport_specific, nullptr);

    // check the values stored into the transport specific data
    ThalliumOptions *thallium_opts = nullptr;
    ASSERT_NE((thallium_opts = dynamic_cast<ThalliumOptions *>(opts.p->transport->transport_specific)), nullptr);
    EXPECT_EQ(thallium_opts->type_, MDHIM_TRANSPORT_THALLIUM);
    EXPECT_EQ(thallium_opts->protocol_, module);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}

TEST(mdhimConfig, prefer_mpi) {
    mdhim_options_t opts;
    ASSERT_EQ(mdhim_options_init(&opts, MPI_COMM_WORLD, false, false), MDHIM_SUCCESS);
    ASSERT_NE(opts.p, nullptr);
    ASSERT_NE(opts.p->transport, nullptr);
    ASSERT_EQ(opts.p->transport->transport_specific, nullptr);

    ConfigSequence seq;

    // MPI should be preferred over thallium,
    // even when USE_THALLIUM shows up first
    // and both are true
    std::stringstream s;
    s << USE_THALLIUM << " " << std::boolalpha << true << std::endl
      << THALLIUM_MODULE << " tcp" << std::endl
      << USE_MPI << " " << std::boolalpha << true << std::endl
      << MEMORY_ALLOC_SIZE << " " << 128 << std::endl
      << MEMORY_REGIONS << " " << 256 << std::endl
      << " NUM_LISTENERS 1" << std::endl;

    ConfigString str(s.str());
    seq.add(&str);

    ASSERT_EQ(process_config_and_fill_options(seq, &opts), MDHIM_SUCCESS);
    ASSERT_NE(opts.p->transport->transport_specific, nullptr);
    EXPECT_EQ(opts.p->transport->transport_specific->type_, MDHIM_TRANSPORT_MPI);

    EXPECT_EQ(mdhim_options_destroy(&opts), MDHIM_SUCCESS);
}
