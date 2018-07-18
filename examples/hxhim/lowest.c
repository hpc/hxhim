#include <float.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "utils/elen.h"

#define BUF_SIZE 100

const double MIN_DOUBLE = -100;
const double MAX_DOUBLE = 100;

const char NAME_FMT[]   = "cell-%zu";
const char X[]          = "x";
const char Y[]          = "y";
const char TEMP[]       = "TEMP";

typedef struct Cell {
    char name[BUF_SIZE];
    char *x;
    char *y;
    char *temp;
} Cell_t;

static void print_double_results(hxhim_results_t *results) {
    if (!results) {
        return;
    }

    for(hxhim_results_goto_head(results); hxhim_results_valid(results) == HXHIM_SUCCESS; hxhim_results_goto_next(results)) {
        enum hxhim_result_type type;
        hxhim_results_type(results, &type);

        int error;
        hxhim_results_error(results, &error);

        int database;
        hxhim_results_database(results, &database);

        switch (type) {
            case HXHIM_RESULT_PUT:
                printf("PUT returned %s from database %d", (error == HXHIM_SUCCESS)?"SUCCESS":"ERROR", database);
                break;
            case HXHIM_RESULT_GET:
                printf("GET returned ");
                if (error == HXHIM_SUCCESS) {
                    void *subject;
                    size_t subject_len = 0;
                    hxhim_results_get_subject(results, &subject, &subject_len);

                    void *predicate;
                    size_t predicate_len = 0;
                    hxhim_results_get_predicate(results, &predicate, &predicate_len);

                    void *object;
                    size_t object_len = 0;
                    hxhim_results_get_object(results, &object, &object_len);

                    double temp = 0;
                    elen_decode_double(predicate, predicate_len, &temp);

                    printf("{%.*s, %f} -> %.*s",
                           (int) subject_len, (char *) subject,
                           temp,
                           (int) object_len, (char *) object);
                }
                else {
                    printf("ERROR");
                }

                printf("from database %d\n", database);
                break;
            case HXHIM_RESULT_DEL:
                printf("DEL returned %s from database %d", (error == HXHIM_SUCCESS)?"SUCCESS":"ERROR", database);
                break;
            default:
                break;
        }
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

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to read configuration");
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
        fprintf(stderr, "Failed to initialize hxhim\n");
        hxhim_options_destroy(&opts);
        return 1;
    }

    double lowest = DBL_MAX;
    double highest = -DBL_MAX;

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

            // keep track of the highest temperature
            if (temp > highest) {
                highest = temp;
            }

            hxhimPut(&hx, (void *) c->name, strlen(c->name), (void *) &X,    strlen(X),    (void *) c->x,    x_len);
            hxhimPut(&hx, (void *) c->name, strlen(c->name), (void *) &Y,    strlen(Y),    (void *) c->y,    y_len);
            hxhimPut(&hx, (void *) c->name, strlen(c->name), (void *) &TEMP, strlen(TEMP), (void *) c->temp, temp_len);
        }
    }

    hxhim_results_t *flush1 = hxhimFlush(&hx);
    hxhim_results_destroy(flush1);

    char *lowest_str = NULL;
    size_t lowest_str_len = 0;
    elen_encode_double(lowest, 2 * sizeof(double), &lowest_str, &lowest_str_len);

    char *highest_str = NULL;
    size_t highest_str_len = 0;
    elen_encode_double(highest, 2 * sizeof(double), &highest_str, &highest_str_len);

    hxhimStatFlush(&hx);
    hxhimBGetOp(&hx, (void *) &TEMP, strlen(TEMP), (void *) lowest_str, lowest_str_len, 10, HXHIM_GET_NEXT);
    hxhimBGetOp(&hx, (void *) &TEMP, strlen(TEMP), (void *) highest_str, highest_str_len, 10, HXHIM_GET_PREV);

    hxhim_results_t *flush2 = hxhimFlush(&hx);
    print_double_results(flush2);
    hxhim_results_destroy(flush2);

    free(lowest_str);
    free(highest_str);

    for(size_t i = 0; i < total; i++) {
        free(cells[i].x);
        free(cells[i].y);
        free(cells[i].temp);
    }
    free(cells);

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
