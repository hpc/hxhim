#ifndef MDHIM_CLI_BGET
#define MDHIM_CLI_BGET

#include <iostream>

#include "mdhim.h"

/** @description Example usage and cleanup of mdhimBget */
void bget(mdhim_t *md,
          void **primary_keys, int *primary_key_lens,
          int num_keys,
          std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
