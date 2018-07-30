#include "util.hpp"

/**
 * bulk_read
 * read arbitrary number of fields into a table made up of data and length arrays
 * format of the data should be:
 *     num_keys (depends on read_num_keys)
 *     field_0_1 field_0_2, ..., field_0_n
 *     ...
 *     field_num_keys-1_1 field_num_keys-1_2, ..., field_num_keys-1_n
 *
 * @param s              an input stream containing data in the format as described above
 * @param input          the structure for storing user input coming in from s
 * @param read_num_keys  whether or not to read the number of num_keys from s; if false, sets num_keys to 1
 * @return true, or false on error
 */
bool bulk_read(std::istream &s, UserInput &input, bool read_num_keys) {
    // get number of num_keys
    if (read_num_keys) {
        if (!(s >> input.num_keys)) {
            return false;
        }
    }
    else {
        input.num_keys = 1;
    }

    // allocate space
    input.data = new void **[input.fields]();
    input.lens = new std::size_t *[input.fields]();
    for(std::size_t f = 0; f < input.fields; f++) {
        (input.data)[f] = new void *[input.num_keys]();
        (input.lens)[f] = new std::size_t[input.num_keys]();
    }

    // read from the stream
    bool error = false;
    for(std::size_t r = 0; (r < input.num_keys) && !error; r++) {
        for(std::size_t c = 0; (c < input.fields) && !error; c++) {
            // read the value
            std::string value;
            if (!(s >> value)) {
                error = true;
                break;
            }

            // store the value into the table
            void **addr = input.data[c] + r;
            *addr = ::operator new(value.size());
            memcpy(*addr, value.c_str(), value.size());

            // store the length into the table
            input.lens[c][r] = value.size();
        }
    }

    return !error;
}

/**
 * bulk_clean
 * clean up bulk_read data
 *
 * @param input the user input to deallocate
 * @return true
 */
bool bulk_clean(UserInput &input) {
    if (input.data) {
        // delete each value
        for(std::size_t r = 0; r < input.num_keys; r++) {
            for(std::size_t c = 0; c < input.fields; c++) {
                if (input.data[c]) {
                    ::operator delete(input.data[c][r]);
                }
            }
        }

        // delete each column
        for(std::size_t c = 0; c < input.fields; c++) {
            delete [] input.data[c];
        }

        // delete the table
        delete [] input.data;
    }

    if (input.lens) {
        for(std::size_t c = 0; c < input.fields; c++) {
            delete [] input.lens[c];
        }

        // delete the table
        delete [] input.lens;
    }

    return true;
}

// /**
//  * next
//  * Increment a transport_brm_t to the next message
//  *
//  * @param brm the current message
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
//  */
// int next(transport_brm_t **brm) {
//     if (!brm) {
//         return TRANSPORT_ERROR;
//     }

//     // Go to next result
//     transport_brm_t *next = nullptr;
//     if (transport_brm_next(*brm, &next) != TRANSPORT_SUCCESS) {
//         return TRANSPORT_ERROR;
//     }

//     transport_brm_destroy(*brm);
//     *brm = next;
//     return TRANSPORT_SUCCESS;
// }

// /**
//  * next
//  * Increment a transport_bgrm_t to the next message
//  *
//  * @param bgrm the current message
//  * @return TRANSPORT_SUCCESS or TRANSPORT_ERROR
//  */
// int next(transport_bgrm_t **bgrm) {
//     if (!bgrm) {
//         return TRANSPORT_ERROR;
//     }

//     // Go to next result
//     transport_bgrm_t *next = nullptr;
//     if (transport_bgrm_next(*bgrm, &next) != TRANSPORT_SUCCESS) {
//         return TRANSPORT_ERROR;
//     }

//     transport_bgrm_destroy(*bgrm);
//     *bgrm = next;
//     return TRANSPORT_SUCCESS;
// }
