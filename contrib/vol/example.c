#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int errno;

#include "hxhim_vol.h"
#include "structs.h"

#define COUNT 10

hid_t make_fapl(struct under_info_t * under_info, hid_t vol_id) {
    /* data associated with the vol */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_vol(fapl, vol_id, &under_info);
    return fapl;
}

int main(int argc, char * argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* /\* not registered yet *\/ */
    /* printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME)); */

    hid_t vol_id = H5VLregister_connector(H5PLget_plugin_info(), H5P_DEFAULT);
    /* printf("%d %d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME), vol_id); */

    struct under_info_t under_info;
    under_info.id = vol_id;
    hid_t fapl = make_fapl(&under_info, vol_id);
    /* hid_t fapl = H5P_DEFAULT; */

    hsize_t dims = COUNT;
    /* hid_t file_id = H5Fcreate("/tmp/hxhim", H5F_ACC_TRUNC, H5P_DEFAULT, fapl); */
    hid_t file_id = H5Fopen("/tmp/hxhim", H5F_ACC_RDWR, fapl);
    hid_t dataspace_id = H5Screate_simple(1, &dims, NULL);

    /* write */
    {
        {
            /* hid_t dataset_id = H5Dcreate(file_id, "/ints", H5T_NATIVE_INT, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT); */
            hid_t dataset_id = H5Dopen(file_id, "/ints", H5P_DEFAULT);

            int write_ints[COUNT];
            for(int i = 0; i < dims; i++) {
                write_ints[i] = i * i;
            }

            /* Write the dataset */
            H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, dataspace_id, H5P_DEFAULT, write_ints);
            /* H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, write_ints); */

            for(hsize_t i = 0; i < dims; i++) {
                printf("    write %d\n", write_ints[i]);
            }

            H5Dclose(dataset_id);
        }
        {
            hid_t dataspace_id = H5Screate_simple(1, &dims, NULL);
            hid_t dataset_id = H5Dcreate(file_id, "/doubles", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

            double write_doubles[COUNT];
            for(hsize_t i = 0; i < dims; i++) {
                write_doubles[i] = i;
            }

            /* Write the dataset */
            H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, dataspace_id, H5P_DEFAULT, write_doubles);

            for(hsize_t i = 0; i < dims; i++) {
                printf("    write %f\n", write_doubles[i]);
            }

            H5Dclose(dataset_id);
        }
        {
            const char write_str[] = "ABCDEF";
            hsize_t dim = sizeof(write_str);
            hid_t dataspace_id = H5Screate_simple(1, &dim, NULL);
            hid_t dataset_id = H5Dcreate(file_id, "/string", H5T_C_S1, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

            /* Write the dataset. */
            H5Dwrite(dataset_id, H5T_C_S1, H5S_ALL, dataspace_id, H5P_DEFAULT, write_str);
            printf("    write %s\n", write_str);

            H5Dclose(dataset_id);
        }
    }

    printf("\n\n");

    /* read */
    {
        {
            hid_t dataset_id = H5Dopen(file_id, "/ints", H5P_DEFAULT);

            int read_ints[COUNT];
            H5Dread(dataset_id, H5T_NATIVE_INT, H5P_DEFAULT, dataspace_id, H5P_DEFAULT, read_ints);
            for(hsize_t i = 0; i < COUNT; i++) {
                printf("    read %d\n", read_ints[i]);
            }

            H5Dclose(dataset_id);
        }
        {
            hid_t dataset_id = H5Dopen(file_id, "/doubles", H5P_DEFAULT);

            double read_doubles[COUNT];
            H5Dread(dataset_id, H5T_NATIVE_DOUBLE, H5P_DEFAULT, dataspace_id, H5P_DEFAULT, read_doubles);
            for(hsize_t i = 0; i < COUNT; i++) {
                printf("    read %f\n", read_doubles[i]);
            }

            H5Dclose(dataset_id);
        }
        {
            hid_t dataset_id = H5Dopen(file_id, "/string", H5P_DEFAULT);

            char read_str[1024];
            H5Dread(dataset_id, H5T_C_S1, H5P_DEFAULT, dataspace_id, H5P_DEFAULT, read_str);
            printf("    read %s\n", read_str);

            H5Dclose(dataset_id);
        }
    }

    H5Sclose(dataspace_id);
    H5Fclose(file_id);
    H5Pclose(fapl);

    /* unregister hxhim */
    H5VLunregister_connector(vol_id);
    /* printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME)); */

    MPI_Finalize();

    return 0;
}
