#ifndef MDHIM_CLI_DELETE
#define MDHIM_CLI_DELETE

#include <iostream>

#include "mdhim.h"

/** @description Example usage and cleanup of mdhimDelete */
void del(mdhim_t *md,
         void *primary_key, int primary_key_len,
         std::ostream &out = std::cout, std::ostream &err = std::cerr);

#endif
