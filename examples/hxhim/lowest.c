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

const char NAME_FMT[]   = "Cell-(%d, %zu, %zu)";
const char TEMP_FMT[]   = "Temp-(%d, %zu, %zu)";

typedef struct Cell {
    char name[BUF_SIZE];
    char predicate[BUF_SIZE];
    double temp;
    char *temp_str;
} Cell_t;

void print_results(hxhim_t *hx, const int print_rank, hxhim_results_t *results) {
    HXHIM_C_RESULTS_LOOP(results) {
        if (print_rank) {
            int rank = -1;
            if (hxhimGetMPI(hx, NULL, &rank, NULL) != HXHIM_SUCCESS) {
                printf("Could not get rank\n");
            }
            else {
                printf("Rank %d ", rank);
            }
        }

        enum hxhim_op_t op;
        hxhim_result_op(results, &op);

        int status;
        hxhim_result_status(results, &status);

        int datastore;
        hxhim_result_datastore(results, &datastore);

        void *subject = NULL;
        size_t subject_len = 0;

        void *predicate = NULL;
        size_t predicate_len = 0;

        if (status == HXHIM_SUCCESS) {
            hxhim_result_subject(results, &subject, &subject_len);
            hxhim_result_predicate(results, &predicate, &predicate_len);
        }

        const double temp = elen_decode_floating_double(subject, ELEN_NEG, ELEN_POS);

        switch (op) {
            case HXHIM_PUT:
                printf("PUT          {%f, %.*s} returned %s from datastore %d\n", temp, (int) predicate_len, (char *) predicate, (status == HXHIM_SUCCESS)?"SUCCESS":"ERROR", datastore);
                break;
            case HXHIM_GETOP:
                printf("GET returned ");
                if (status == HXHIM_SUCCESS) {
                    enum hxhim_object_type_t object_type;
                    hxhim_result_object_type(results, &object_type);
                    void *object = NULL;
                    size_t object_len = 0;
                    hxhim_result_object(results, &object, &object_len);

                    printf("{%f, %.*s} -> %.*s", temp,
                           (int) predicate_len, (char *) predicate,
                           (int) object_len, (char *) object);

                }
                else {
                    printf("ERROR");
                }

                printf(" from datastore %d\n", datastore);
                break;
            default:
                printf("Bad Operation: %d\n", (int) op);
                break;
        }
    }
}

int main(int argc, char *argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (argc < 5) {
        fprintf(stderr, "Syntax: %s n_x n_y n_lowest n_highest", argv[0]);
        return 1;
    }

    size_t num_x       = 0;
    size_t num_y       = 0;
    size_t num_lowest  = 0;
    size_t num_highest = 0;

    if ((sscanf(argv[1], "%zu", &num_x)       != 1) ||
        (sscanf(argv[2], "%zu", &num_y)       != 1) ||
        (sscanf(argv[3], "%zu", &num_lowest)  != 1) ||
        (sscanf(argv[4], "%zu", &num_highest) != 1)) {
        fprintf(stderr, "Bad input\n");
        return 1;
    }

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL));

    // read the config
    hxhim_options_t opts;
    if (hxhim_default_config_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        if (rank == 0) {
            fprintf(stderr, "Failed to read configuration\n");
        }
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
        if (rank == 0) {
            fprintf(stderr, "Failed to initialize hxhim\n");
        }
        hxhim_options_destroy(&opts);
        return 1;
    }

    Cell_t *lowest = NULL;
    Cell_t *highest = NULL;

    // generate some data
    const size_t total = num_x * num_y;
    Cell_t *cells = (Cell_t *) calloc(total, sizeof(Cell_t));

    size_t id = 0;
    for(size_t x = 0; x < num_x; x++) {
        for(size_t y = 0; y < num_y; y++) {
            Cell_t *c = &cells[x * num_x + y];

            snprintf(c->name,      BUF_SIZE, NAME_FMT, rank, x, y);
            snprintf(c->predicate, BUF_SIZE, TEMP_FMT, rank, x, y);
            c->temp = MIN_DOUBLE + (MAX_DOUBLE - MIN_DOUBLE) * ((double) rand()) / ((double) RAND_MAX);
            c->temp_str = elen_encode_floating_double(c->temp, 10, ELEN_NEG, ELEN_POS);

            // keep track of the lowest temperature
            if (lowest) {
                if (c->temp < lowest->temp) {
                    lowest = c;
                }
            }
            else {
                lowest = c;
            }

            // keep track of the highest temperature
            if (highest) {
                if (c->temp > highest->temp) {
                    highest = c;
                }
            }
            else {
                highest = c;
            }

            hxhimPut(&hx,
                     c->temp_str, strlen(c->temp_str) + 1,
                     c->predicate, strlen(c->predicate) + 1,
                     HXHIM_OBJECT_TYPE_BYTE,
                     c->name, strlen(c->name) + 1);
        }
    }

    // force PUTs to finish
    hxhim_results_t *puts = hxhimFlush(&hx);
    print_results(&hx, 0, puts);
    hxhim_results_destroy(puts);

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == (rand() % size)) {
        printf("--------------------------------------------\n");
        printf("lowest %zu\n", num_lowest);
        printf("--------------------------------------------\n");
        hxhimGetOp(&hx,
                   lowest->temp_str, strlen(lowest->temp_str) + 1,
                   lowest->predicate, strlen(lowest->predicate) + 1,
                   HXHIM_OBJECT_TYPE_BYTE, num_lowest, HXHIM_GETOP_NEXT);
        hxhim_results_t *get_lowest = hxhimFlush(&hx);
        print_results(&hx, 0, get_lowest);
        hxhim_results_destroy(get_lowest);
        printf("--------------------------------------------\n");

        printf("--------------------------------------------\n");
        printf("highest %zu\n", num_highest);
        printf("--------------------------------------------\n");
        hxhimGetOp(&hx,
                   highest->temp_str, strlen(highest->temp_str) + 1,
                   highest->predicate, strlen(highest->predicate) + 1,
                   HXHIM_OBJECT_TYPE_BYTE, num_highest, HXHIM_GETOP_PREV);
        hxhim_results_t *get_highest = hxhimFlush(&hx);
        print_results(&hx, 0, get_highest);
        hxhim_results_destroy(get_highest);
        printf("--------------------------------------------\n");
    }

    for(size_t i = 0; i < total; i++) {
        free(cells[i].temp_str);
    }
    free(cells);

    hxhimClose(&hx);
    hxhim_options_destroy(&opts);

    MPI_Finalize();

    return 0;
}
