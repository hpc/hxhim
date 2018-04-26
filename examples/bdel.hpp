#ifndef MDHIM_CLI_BDELETE
#define MDHIM_CLI_BDELETE

#include <iostream>

#include "mdhim.h"
#include "util.hpp"

/** @description Example usage and cleanup of mdhimBDelete */
void bdel(mdhim_t *md,
          void **primary_keys, std::size_t *primary_key_lens,
          std::size_t num_keys,
          std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
