#ifndef CLI_UTILITY
#define CLI_UTILITY

#include <cstring>
#include <istream>
#include <list>

struct UserInput {
    std::size_t fields;
    void ***data;
    std::size_t **lens;
    std::size_t num_keys;
};

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
template <typename T>
bool bulk_read(std::istream &s, UserInput &input, const bool *encode, bool read_num_keys = true) {
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
            void **addr = input.data[c] + r;

            // read the value
            if (encode[c]) {
                T value;
                if (!(s >> value)) {
                    error = true;
                    break;
                }

                // store the value into the table
                *addr = ::operator new(sizeof(value));
                memcpy(*addr, &value, sizeof(value));

                // store the length into the table
                input.lens[c][r] = sizeof(value);
            }
            else {
                std::string value;
                if (!(s >> value)) {
                    error = true;
                    break;
                }

                // store the value into the table
                *addr = ::operator new(sizeof(value));
                memcpy(*addr, &value, sizeof(value));

                // store the length into the table
                input.lens[c][r] = sizeof(value);
            }
        }
    }

    return !error;
}

bool bulk_read(std::istream &s, UserInput &input, bool read_num_keys);

/** @description Utility function for cleaning up pointers allocated by bulk_read */
bool bulk_clean(UserInput &input);

// /** @description Incremenet a transport_brm_t to the next message */
// int next(transport_brm_t **brm);

// /** @description Incremenet a transport_bgrm_t to the next message */
// int next(transport_bgrm_t **bgrm);

#endif
