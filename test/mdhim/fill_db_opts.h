#ifndef _FILL_DB_OPTS_IN_TESTS_
#define _FILL_DB_OPTS_IN_TESTS_

#include "data_store.h"
#include "mdhim_options.h"
#include "mlog2.h"
#include "partitioner.h"

// quick and dirty function to fill in a mdhim_options_t
// variable with required fields
void fill_db_opts(mdhim_options_t &opts);

#endif
