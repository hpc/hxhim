#ifndef MDHIM_CLI_INPUT
#define MDHIM_CLI_INPUT

#include <istream>

#include "mdhim.h"

/** @description Utility function for reading arbitrary number of fields          */
int bulk_read(std::istream &s, int columns, void ****data, int ***lens, int &rows, bool read_columns = true);

/** @description Utility function for cleaning up pointers allocated by bulk_read */
int bulk_clean(int columns, void ***data, int **lens, int rows);

#endif
