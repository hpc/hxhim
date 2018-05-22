#include "util.hpp"

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
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int bulk_read(std::istream &s, std::size_t columns, void ****data, std::size_t ***lens, std::size_t &rows, bool read_rows) {
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
    *lens = new std::size_t *[columns]();
    for(std::size_t f = 0; f < columns; f++) {
        (*data)[f] = new void *[rows]();
        (*lens)[f] = new std::size_t[rows]();
    }

    // read from the stream
    bool error = false;
    for(std::size_t r = 0; (r < rows) && !error; r++) {
        for(std::size_t c = 0; (c < columns) && !error; c++) {
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
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int bulk_clean(std::size_t columns, void ***data, std::size_t **lens, std::size_t rows) {
    if (data) {
        // delete each value
        for(std::size_t r = 0; r < rows; r++) {
            for(std::size_t c = 0; c < columns; c++) {
                if (data[c]) {
                    ::operator delete(data[c][r]);
                }
            }
        }

        // delete each column
        for(std::size_t c = 0; c < columns; c++) {
            delete [] data[c];
        }

        // delete the table
        delete [] data;
    }

    if (lens) {
        for(std::size_t c = 0; c < columns; c++) {
            delete [] lens[c];
        }

        // delete the table
        delete [] lens;
    }

    return MDHIM_SUCCESS;
}

/**
 * next
 * Increment a mdhim_brm_t to the next message
 *
 * @param brm the current message
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int next(mdhim_brm_t **brm) {
    if (!brm) {
        return MDHIM_ERROR;
    }

    // Go to next result
    mdhim_brm_t *next = nullptr;
    if (mdhim_brm_next(*brm, &next) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    mdhim_brm_destroy(*brm);
    *brm = next;
    return MDHIM_SUCCESS;
}

/**
 * next
 * Increment a mdhim_bgrm_t to the next message
 *
 * @param bgrm the current message
 * @return MDHIM_SUCCESS or MDHIM_ERROR
 */
int next(mdhim_bgrm_t **bgrm) {
    if (!bgrm) {
        return MDHIM_ERROR;
    }

    // Go to next result
    mdhim_bgrm_t *next = nullptr;
    if (mdhim_bgrm_next(*bgrm, &next) != MDHIM_SUCCESS) {
        return MDHIM_ERROR;
    }

    mdhim_bgrm_destroy(*bgrm);
    *bgrm = next;
    return MDHIM_SUCCESS;
}
