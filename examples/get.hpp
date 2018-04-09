#ifndef MDHIM_CLI_GET
#define MDHIM_CLI_GET

#include <iostream>

#include "mdhim.h"

/** @description Example usage and cleanup of mdhimGet */
void get(mdhim_t *md,
         void *primary_key, int primary_key_len,
         std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
