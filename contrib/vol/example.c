#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int errno;

#include "hxhim_vol.h"
#include "float3.h"

#define COUNT 10

int main(int argc, char * argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int size;
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* not registered yet */
    printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME));

    hid_t vol_id = H5VLregister_connector(H5PLget_plugin_info(), H5P_DEFAULT);
    printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME));

    /* data associated with the vol */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    H5Pset_vol(fapl, vol_id, NULL);

    hid_t float3_id = H5VL_hxhim_create_float3_type();

    hsize_t dims = COUNT;
    struct float3_t floats[COUNT];
    for(hsize_t i = 0; i < dims; i++) {
        floats[i].subject   = '0' + rank;
        floats[i].predicate = i;
        floats[i].object    = i * i;
    }

    hid_t file_id = H5Fcreate("/tmp/hxhim", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    hid_t dataspace_id = H5Screate_simple(1, &dims, NULL);
    hid_t dataset_id = H5Dcreate(file_id, "/dataset", float3_id, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    /* Write the dataset. */
    H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, floats);

    /* hid_t file_id = H5Fopen("dset.h5", fapl); */
    /* hid_t dataspace_id = H5Screate_simple(sizeof(dims) / sizeof(dims[0]), dims, NULL); */
    /* hid_t dataset_id = H5Dopen(file_id, "/dataset"); */

    struct float3_t from_file[10];
    H5Dread(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, from_file);

    /* for (int i = 0; i < X_DIM; i++) { */
    /*     for (int j = 0; j < Y_DIM; j++) { */
    /*         for (int k = 0; k < Z_DIM; k++) { */
    /*             printf("(%d, %d, %d) = %4f\n", i, j, k, data[i][j][k]); */
    /*         } */
    /*     } */
    /* } */

    H5Dclose(dataset_id);
    H5Sclose(dataspace_id);
    H5Fclose(file_id);
    H5Tclose(float3_id);
    H5Pclose(fapl);

    /* unregister hxhim */
    H5VLunregister_connector(vol_id);
    printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME));

    MPI_Finalize();

    return 0;
}
