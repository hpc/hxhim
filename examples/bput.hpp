#ifndef MDHIM_CLI_BPUT
#define MDHIM_CLI_BPUT

#include <iostream>

#include "mdhim.h"

/** @description Example usage and cleanup of mdhimPut */
void bput(mdhim_t *md,
          void **primary_keys, int *primary_key_lens,
          void **values, int *value_lens,
          int num_keys,
          std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
