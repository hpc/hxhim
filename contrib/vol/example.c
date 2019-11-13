#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int errno;

#include "hxhim_vol.h"
#include "structs.h"

#define COUNT 10

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

    /* data associated with the vol */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    struct under_info_t under_info;
    under_info.id = vol_id;
    H5Pset_vol(fapl, vol_id, &under_info);

    hsize_t dims = COUNT;

    /* write */
    {
        hid_t file_id = H5Fcreate("/tmp/hxhim", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
        {
            hid_t dataspace_id = H5Screate_simple(1, &dims, NULL);
            hid_t dataset_id = H5Dcreate(file_id, "/dataset", H5T_NATIVE_FLOAT, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

            float floats[COUNT];
            for(hsize_t i = 0; i < dims; i++) {
                floats[i] = i;
            }

            /* Write the dataset. */
            H5Dwrite(dataset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, floats);

            for(hsize_t i = 0; i < dims; i++) {
                printf("    %f\n", floats[i]);
            }

            H5Dclose(dataset_id);
            H5Sclose(dataspace_id);
        }
        {
            const char str[] = "ABCDEF";
            hsize_t dim = sizeof(str);
            hid_t dataspace_id = H5Screate_simple(1, &dim, NULL);
            hid_t dataset_id = H5Dcreate(file_id, "/string", H5T_C_S1, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

            /* Write the dataset. */
            H5Dwrite(dataset_id, H5T_C_S1, H5S_ALL, H5S_ALL, H5P_DEFAULT, str);

            H5Dclose(dataset_id);
            H5Sclose(dataspace_id);
        }

        H5Fclose(file_id);
    }

    /* read */
    {
        hsize_t dims1 = 2;
        hsize_t dims2 = 6;

        hid_t file_id = H5Fopen("/tmp/hxhim", 0, fapl);
        {
            hid_t dataset_id = H5Dopen(file_id, "/dataset", H5P_DEFAULT);

            float from_file[COUNT];
            H5Dread(dataset_id, H5T_NATIVE_FLOAT, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, from_file);
            for(hsize_t i = 0; i < COUNT; i++) {
                printf("    %f\n", from_file[i]);
            }

            H5Dclose(dataset_id);
        }
        {
            hid_t dataset_id = H5Dopen(file_id, "/string", H5P_DEFAULT);

            char buf[1024];
            H5Dread(dataset_id, H5T_C_S1, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT, buf);
            printf("    %s\n", buf);

            H5Dclose(dataset_id);
        }
        H5Fclose(file_id);
    }

    H5Pclose(fapl);

    /* unregister hxhim */
    H5VLunregister_connector(vol_id);
    /* printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME)); */

    MPI_Finalize();

    return 0;
}
