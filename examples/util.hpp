#ifndef MDHIM_CLI_UTILITY
#define MDHIM_CLI_UTILITY

#include <istream>

#include "mdhim.h"

/** @description Utility function for reading arbitrary number of fields          */
int bulk_read(std::istream &s, int columns, void ****data, int ***lens, int &rows, bool read_columns = true);

/** @description Utility function for cleaning up pointers allocated by bulk_read */
int bulk_clean(int columns, void ***data, int **lens, int rows);

/** @description Incremenet a mdhim_brm_t to the next message */
int next(mdhim_brm_t **brm);

/** @description Incremenet a mdhim_bgetrm_t to the next message */
int next(mdhim_bgetrm_t **bgrm);

#endif
