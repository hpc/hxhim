#include <float.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <mpi.h>

#include "hxhim/hxhim.h"
#include "print_results.h"

#define BUF_SIZE 100

const double MIN_DOUBLE = -100;
const double MAX_DOUBLE = 100;

const char NAME_FMT[]   = "Cell-(%d, %zu, %zu)";
const char TEMP_FMT[]   = "Temp-(%d, %zu, %zu)";

typedef struct Cell {
    char name[BUF_SIZE];
    char predicate[BUF_SIZE];
    double temp;
} Cell_t;

int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Syntax: %s n_x n_y n_lowest n_highest\n", argv[0]);
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

    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    srand(time(NULL));

    // read the config
    hxhim_options_t opts;
    if (hxhim_config_default_reader(&opts, MPI_COMM_WORLD) != HXHIM_SUCCESS) {
        if (rank == 0) {
            fprintf(stderr, "Failed to read configuration\n");
        }
        MPI_Finalize();
        return 1;
    }

    // start hxhim
    hxhim_t hx;
    if (hxhimOpen(&hx, &opts) != HXHIM_SUCCESS) {
        if (rank == 0) {
            fprintf(stderr, "Failed to initialize hxhim\n");
        }
        hxhim_options_destroy(&opts);
        MPI_Finalize();
        return 1;
    }
    hxhim_options_destroy(&opts);

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
                     c->name, strlen(c->name), HXHIM_DATA_BYTE,
                     c->predicate, strlen(c->predicate), HXHIM_DATA_BYTE,
                     &c->temp, sizeof(c->temp), HXHIM_DATA_DOUBLE,
                     HXHIM_PUT_OPS);
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
                   &lowest->temp, sizeof(lowest->temp), HXHIM_DATA_DOUBLE,
                   lowest->predicate, strlen(lowest->predicate), HXHIM_DATA_BYTE,
                   HXHIM_DATA_BYTE, num_lowest, HXHIM_GETOP_NEXT);
        hxhim_results_t *get_lowest = hxhimFlush(&hx);
        print_results(&hx, 0, get_lowest);
        hxhim_results_destroy(get_lowest);
        printf("--------------------------------------------\n");

        printf("--------------------------------------------\n");
        printf("highest %zu\n", num_highest);
        printf("--------------------------------------------\n");
        hxhimGetOp(&hx,
                   &highest->temp, sizeof(highest->temp), HXHIM_DATA_DOUBLE,
                   highest->predicate, strlen(highest->predicate), HXHIM_DATA_BYTE,
                   HXHIM_DATA_BYTE, num_highest, HXHIM_GETOP_PREV);
        hxhim_results_t *get_highest = hxhimFlush(&hx);
        print_results(&hx, 0, get_highest);
        hxhim_results_destroy(get_highest);
        printf("--------------------------------------------\n");
    }

    free(cells);

    hxhimClose(&hx);

    MPI_Finalize();

    return 0;
}
