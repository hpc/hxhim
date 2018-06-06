#ifndef MDHIM_CLI_BPUT
#define MDHIM_CLI_BPUT

#include <iostream>

#include "mdhim.h"
#include "../util.hpp"

/** @description Example usage and cleanup of mdhimPut */
void bput(mdhim_t *md,
          void **primary_keys, std::size_t *primary_key_lens,
          void **values, std::size_t *value_lens,
          std::size_t num_keys,
          std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
