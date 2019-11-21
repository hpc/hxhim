#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int errno;

#include "hxhim_vol.h"

#define COUNT 10

bool fill_options(MPI_Comm comm, hxhim_options_t *opts, const char * name) {
    int rank;
    MPI_Comm_rank(comm, &rank);

    return ((hxhim_options_init(opts)                                             == HXHIM_SUCCESS) &&
            (hxhim_options_set_mpi_bootstrap(opts, comm)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_debug_level(opts, MLOG_ERR)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_ratio(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_server_ratio(opts, 1)                              == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastores_per_range_server(opts, 1)               == HXHIM_SUCCESS) &&
            (hxhim_options_set_datastore_leveldb(opts, rank, name, 1)             == HXHIM_SUCCESS) &&
            (hxhim_options_set_transport_thallium(opts, "na+sm")                  == HXHIM_SUCCESS) &&
            (hxhim_options_set_hash_name(opts, "SUM_MOD_DATASTORES")              == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_alloc_size(opts, 128)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_keys_regions(opts, 64)                             == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_alloc_size(opts, 128)                      == HXHIM_SUCCESS) &&
            (hxhim_options_set_buffers_regions(opts, 12)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_ops_cache_regions(opts, 4)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_alloc_size(opts, 32 * sizeof(void *))       == HXHIM_SUCCESS) &&
            (hxhim_options_set_arrays_regions(opts, 128)                          == HXHIM_SUCCESS) &&
            (hxhim_options_set_requests_alloc_size(opts, 256)                     == HXHIM_SUCCESS) &&
            (hxhim_options_set_requests_regions(opts, 16)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_packed_alloc_size(opts, 1024)               == HXHIM_SUCCESS) &&
            (hxhim_options_set_client_packed_regions(opts, 2)                     == HXHIM_SUCCESS) &&
            (hxhim_options_set_rs_packed_alloc_size(opts, 1024)                   == HXHIM_SUCCESS) &&
            (hxhim_options_set_rs_packed_regions(opts, 2)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_responses_alloc_size(opts, 1024)                   == HXHIM_SUCCESS) &&
            (hxhim_options_set_responses_regions(opts, 2)                         == HXHIM_SUCCESS) &&
            (hxhim_options_set_result_regions(opts, 16)                           == HXHIM_SUCCESS) &&
            (hxhim_options_set_results_regions(opts, 4)                           == HXHIM_SUCCESS) &&
            (hxhim_options_set_start_async_put_at(opts, 0)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_maximum_ops_per_send(opts, 8)                      == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_first_n(opts, 10)                        == HXHIM_SUCCESS) &&
            (hxhim_options_set_histogram_bucket_gen_method(opts, "10_BUCKETS")    == HXHIM_SUCCESS) &&
            true);
}

hid_t make_fapl(struct under_info_t * under_info, hid_t vol_id) {
    /* data associated with the vol */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_vol(fapl, vol_id, under_info);
    return fapl;
}

int main(int argc, char * argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    hid_t fapl = hxhim_vol_init("/tmp/hxhim", MPI_COMM_WORLD, 1);

    hsize_t dims = COUNT;

    /* /\* write *\/ */
    /* if (rank == 0) */
    /* { */
    /*     hid_t file_id = H5Fcreate("/tmp/hxhim", H5F_ACC_TRUNC, H5P_DEFAULT, fapl); */
    /*     hid_t dataspace_id = H5Screate_simple(1, &dims, NULL); */

    /*     int write_ints[COUNT] = {0, 1, 4, 9, 16, 25, 36, 49, 64, 81}; */
    /*     hid_t write_ints_dataset_id = H5Dcreate(file_id, "/ints", H5T_NATIVE_INT, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
    /*     H5Dwrite(write_ints_dataset_id, H5T_NATIVE_INT, H5S_ALL, dataspace_id, H5P_DEFAULT, write_ints); */

    /*     double write_doubles[COUNT] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}; */
    /*     hid_t write_doubles_dataset_id = H5Dcreate(file_id, "/doubles", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
    /*     H5Dwrite(write_doubles_dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, H5P_DEFAULT, write_doubles); */

    /*     /\* noop *\/ */
    /*     hid_t group_id = H5Gcreate(file_id, "/group", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */

    /*     const char write_str[] = "ABCDEF"; */
    /*     hid_t write_str_dataset_id = H5Dcreate(group_id, "/group/string", H5T_C_S1, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
    /*     H5Dwrite(write_str_dataset_id, H5T_C_S1, H5S_ALL, dataspace_id, H5P_DEFAULT, write_str); */

    /*     H5Fflush(file_id, H5F_SCOPE_GLOBAL); */
    /*     for(hsize_t i = 0; i < dims; i++) { */
    /*         printf("     write %d\n", write_ints[i]); */
    /*     } */
    /*     for(hsize_t i = 0; i < dims; i++) { */
    /*         printf("     write %f\n", write_doubles[i]); */
    /*     } */
    /*     printf("     write %s\n", write_str); */

    /*     H5Dclose(write_str_dataset_id); */
    /*     H5Gclose(group_id); */
    /*     H5Dclose(write_doubles_dataset_id); */
    /*     H5Dclose(write_ints_dataset_id); */

    /*     H5Sclose(dataspace_id); */
    /*     H5Fclose(file_id); */

    /*     printf("\n\n"); */
    /* } */

    MPI_Barrier(MPI_COMM_WORLD);

    /* read */
    if (rank == 1)
    {
        hid_t file_id = H5Fopen("/tmp/hxhim", H5F_ACC_RDWR, fapl);
        hid_t dataspace_id = H5Screate_simple(1, &dims, NULL);

        hid_t read_ints_dataset_id = H5Dopen(file_id, "/ints", H5P_DEFAULT);
        int read_ints[COUNT] = {};
        H5Dread(read_ints_dataset_id, H5T_NATIVE_INT, H5S_ALL, dataspace_id, H5P_DEFAULT, read_ints);

        hid_t read_doubles_dataset_id = H5Dopen(file_id, "/doubles", H5P_DEFAULT);
        double read_doubles[COUNT] = {};
        H5Dread(read_doubles_dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, H5P_DEFAULT, read_doubles);

        hid_t read_str_dataset_id = H5Dopen(file_id, "/group/string", H5P_DEFAULT);
        char read_str[1024] = {};
        H5Dread(read_str_dataset_id, H5T_C_S1, H5S_ALL, dataspace_id, H5P_DEFAULT, read_str);

        H5Fflush(file_id, H5F_SCOPE_GLOBAL);
        for(hsize_t i = 0; i < COUNT; i++) {
            printf("     read %d\n", read_ints[i]);
        }
        for(hsize_t i = 0; i < COUNT; i++) {
            printf("     read %f\n", read_doubles[i]);
        }
        printf("     read %s\n", read_str);

        H5Dclose(read_str_dataset_id);
        H5Dclose(read_doubles_dataset_id);
        H5Dclose(read_ints_dataset_id);

        H5Sclose(dataspace_id);
        H5Fclose(file_id);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    hxhim_vol_finalize(fapl);

    MPI_Finalize();

    return 0;
}
