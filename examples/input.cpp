#include "input.hpp"

/**
 * bulk_read
 * read arbitrary number of fields into a table made up of data and length arrays
 * format of the data should be:
 *     rows (depends on read_rows)
 *     field_0_1 field_0_2, ..., field_0_n
 *     ...
 *     field_rows-1_1 field_rows-1_2, ..., field_rows-1_n
 *
 * @param s            an input stream containing data in the format as described above
 * @param columns      the number of columns of data there are
 * @param data         an array of columns of data; each value in a column is a pointer
 * @param lens         corresponding lengths of the data
 * @param rows         how many rows of data there are
 * @param read_rows    whether or not to read the number of rows from s; if false, sets rows to 1
 * @return MDHIM_SUCCESS or MDHIM_ERROR;
 */
int bulk_read(std::istream &s, int columns, void ****data, int ***lens, int &rows, bool read_rows) {
    // get number of rows
    if (read_rows) {
        if (!(s >> rows)) {
            return MDHIM_ERROR;
        }
    }
    else {
        rows = 1;
    }

    // allocate space
    *data = new void **[columns]();
    *lens = new int *[columns]();
    for(int f = 0; f < columns; f++) {
        (*data)[f] = new void *[rows]();
        (*lens)[f] = new int[rows]();
    }

    // read from the stream
    bool error = false;
    for(int r = 0; (r < rows) && !error; r++) {
        for(int c = 0; (c < columns) && !error; c++) {
            // read the value
            std::string value;
            if (!(s >> value)) {
                error = true;
                break;
            }

            // store the value into the table
            void **addr = (*data)[c] + r;
            *addr = ::operator new(value.size());
            memcpy(*addr, value.c_str(), value.size());

            // store the length into the table
            (*lens)[c][r] = value.size();
        }
    }

    return error?MDHIM_ERROR:MDHIM_SUCCESS;
}

/**
 * bulk_clean
 * clean up bulk_read data
 *
 * @param columns the number of columns of data there are
 * @param data    an array of columns of data; each value in a column is a pointer
 * @param lens    corresponding lengths of the data
 * @param rows    how many rows of data there are
 * @return MDHIM_SUCCESS or MDHIM_ERROR;
 */
int bulk_clean(int columns, void ***data, int **lens, int rows) {
    if (data) {
        // delete each value
        for(int r = 0; r < rows; r++) {
            for(int c = 0; c < columns; c++) {
                if (data[c]) {
                    ::operator delete(data[c][r]);
                }
            }
        }

        // delete each column
        for(int c = 0; c < columns; c++) {
            delete [] data[c];
        }

        // delete the table
        delete [] data;
    }

    if (lens) {
        for(int c = 0; c < columns; c++) {
            delete [] lens[c];
        }

        // delete the table
        delete [] lens;
    }

    return MDHIM_SUCCESS;
}
