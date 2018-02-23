#include "fill_db_opts.h"

void fill_db_opts(mdhim_options_t &opts) {
    opts.comm = MPI_COMM_WORLD;
    mdhim_options_set_db_path(&opts, "./");
    mdhim_options_set_db_name(&opts, "mdhim");
    mdhim_options_set_db_type(&opts, LEVELDB);
    mdhim_options_set_key_type(&opts, MDHIM_INT_KEY); //Key_type = 1 (int)
    mdhim_options_set_debug_level(&opts, MLOG_CRIT);
    mdhim_options_set_server_factor(&opts, 1);
}
