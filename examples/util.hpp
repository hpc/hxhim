#ifndef MDHIM_CLI_UTILITY
#define MDHIM_CLI_UTILITY

#include <istream>
#include <list>

#include "mdhim.h"

struct UserInput {
    std::size_t fields;
    void ***data;
    std::size_t **lens;
    std::size_t num_keys;
};

/** @description Utility function for reading arbitrary number of fields          */
bool bulk_read(std::istream &s, UserInput &input, bool read_rows = true);

/** @description Utility function for cleaning up pointers allocated by bulk_read */
bool bulk_clean(UserInput &input);

/** @description Incremenet a mdhim_brm_t to the next message */
int next(mdhim_brm_t **brm);

/** @description Incremenet a mdhim_bgrm_t to the next message */
int next(mdhim_bgrm_t **bgrm);

#endif
