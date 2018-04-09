#ifndef MDHIM_CLI_PUT
#define MDHIM_CLI_PUT

#include <iostream>

#include "mdhim.h"

/** @description Example usage and cleanup of mdhimPut */
void put(mdhim_t *md,
         void *primary_key, int primary_key_len,
         void *value, int value_len,
         std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
