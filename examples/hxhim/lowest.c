#include <float.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <mpi.h>

#include "elen.h"
#include "hxhim.h"

const double MIN_DOUBLE = -100;
const double MAX_DOUBLE = 100;

const char NAME_FMT[]   = "cell-%zu";
const size_t BUF_SIZE   = 100;
const char X[]          = "x";
const char Y[]          = "y";
const char TEMP[]       = "TEMP";

typedef struct Cell {
    char name[BUF_SIZE];
    char *x;
    char *y;
    char *temp;
} Cell_t;

static void print_double_results(hxhim_return_t *results) {
    // move the internal index to the first range server
    while (hxhim_return_move_to_first_rs(results) == HXHIM_SUCCESS) {
        // iterate through each range server
        for(int valid_rs; (hxhim_return_valid_rs(results, &valid_rs) == HXHIM_SUCCESS) && (valid_rs == HXHIM_SUCCESS); hxhim_return_next_rs(results)) {
            int src = -1;
            hxhim_return_get_src(results, &src);

            // move the internal index to the beginning of the key value pairs list
            if (hxhim_return_move_to_first_spo(results) == HXHIM_SUCCESS) {
                // make sure the return value can be obtained
                int error = HXHIM_SUCCESS;
                if (hxhim_return_get_error(results, &error) == HXHIM_SUCCESS) {
                    if (error == HXHIM_SUCCESS) {
                        // iterate through each key value pair
                        for(int valid_spo; (hxhim_return_valid_spo(results, &valid_spo) == HXHIM_SUCCESS) && (valid_spo == HXHIM_SUCCESS); hxhim_return_next_spo(results)) {
                            // get the key
                            char *subject; size_t subject_len;
                            char *predicate; size_t predicate_len;
                            char *object; size_t object_len;
                            hxhim_return_get_spo(results, (void **) &subject, &subject_len, (void **) &predicate, &predicate_len, (void **) &object, &object_len);

                            // turn the predicate back into a double
                            double temp = 0;
                            elen_decode_double(predicate, predicate_len, &temp);

                            printf("GET {%.*s, %f} -> %.*s on range server %d\n", (int) subject_len, subject, temp, (int) object_len, object, src);
                        }
                    }
                    else {
                        printf("Range server %d responded with failure\n", src);
                    }
                }
            }
        }
        hxhim_return_next(results);
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL));

    // start hxhim
    hxhim_t hx;
    hxhimOpen(&hx, MPI_COMM_WORLD);

    double lowest = DBL_MAX;

    // generate some data
    const size_t num_x = 10;
    const size_t num_y = 10;
    const size_t total = num_x * num_y;
    Cell_t *cells = (Cell_t *) calloc(total, sizeof(Cell_t));

    size_t id = 0;
    for(size_t x = 0; x < num_x; x++) {
        for(size_t y = 0; y < num_y; y++) {
            Cell_t *c = &cells[x * num_x + y];

            snprintf(c->name, BUF_SIZE, NAME_FMT, id++);

            size_t x_len = 0;
            elen_encode_size_t(x, &c->x, &x_len);

            size_t y_len = 0;
            elen_encode_size_t(y, &c->y, &y_len);

            const double temp = MIN_DOUBLE + (MAX_DOUBLE - MIN_DOUBLE) * ((double) rand()) / ((double) RAND_MAX);
            size_t temp_len = 0;
            elen_encode_double(temp, 2 * sizeof(double), &c->temp, &temp_len);

            // keep track of the lowest temperature
            if (temp < lowest) {
                lowest = temp;
            }

            hxhimPut(&hx, (void *) c->name, strlen(c->name), (void *) &X,    strlen(X),    (void *) c->x,    x_len);
            hxhimPut(&hx, (void *) c->name, strlen(c->name), (void *) &Y,    strlen(Y),    (void *) c->y,    y_len);
            hxhimPut(&hx, (void *) c->name, strlen(c->name), (void *) &TEMP, strlen(TEMP), (void *) c->temp, temp_len);
        }
    }

    hxhim_return_t *flush1 = hxhimFlush(&hx);
    hxhim_return_destroy(flush1);

    char *lowest_str = NULL;
    size_t lowest_str_len = 0;
    elen_encode_double(lowest, 2 * sizeof(double), &lowest_str, &lowest_str_len);

    hxhimBGetOp(&hx, (void *) &TEMP, strlen(TEMP), (void *) lowest_str, lowest_str_len, total, GET_NEXT);

    hxhim_return_t *flush2 = hxhimFlush(&hx);
    print_double_results(flush2);
    hxhim_return_destroy(flush2);

    free(lowest_str);

    for(size_t i = 0; i < total; i++) {
        free(cells[i].x);
        free(cells[i].y);
        free(cells[i].temp);
    }
    free(cells);

    hxhimClose(&hx);
    MPI_Finalize();

    return 0;
}
