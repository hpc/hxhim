#ifndef MDHIM_CLI_BDELETE
#define MDHIM_CLI_BDELETE

#include <iostream>

#include "mdhim.h"

/** @description Example usage and cleanup of mdhimBDelete */
void bdel(mdhim_t *md,
          void **primary_keys, int *primary_key_lens,
          int num_keys,
          std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
