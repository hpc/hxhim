#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int errno;

#include "hxhim_vol.h"
#include "float3.h"


#define X_DIM 3
#define Y_DIM 4
#define Z_DIM 5

int main() {
    /* not registered yet */
    printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME));

    hid_t vol_id = H5VLregister_connector(H5PLget_plugin_info(), H5P_DEFAULT);
    printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME));

    /* data associated with the vol */
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
    struct file_info_t under_vol;
    H5Pget_vol_id(fapl, &under_vol.id);
    H5Pget_vol_info(fapl, &under_vol.info);
    H5Pset_vol(fapl, vol_id, &under_vol);

    /* hsize_t dims[] = {X_DIM, Y_DIM, Z_DIM}; */
    /* float dset_data[X_DIM][Y_DIM][Z_DIM]; */
    /* for (int i = 0; i < X_DIM; i++) { */
    /*     for (int j = 0; j < Y_DIM; j++) { */
    /*         for (int k = 0; k < Z_DIM; k++) { */
    /*             dset_data[i][j][k] = i + j + k; */
    /*             /\* printf("(%d, %d, %d) = %4f\n", i, j, k, dset_data[i][j][k]); *\/ */
    /*         } */
    /*     } */
    /* } */

    hid_t float3_id = H5VL_hxhim_create_float3_type();

    hsize_t dims = 10;
    struct float3_t floats[10];
    for(hsize_t i = 0; i < dims; i++) {
        floats[i].subject   = 1 * i;
        floats[i].predicate = 2 * i;
        floats[i].object    = 3 * i;
    }

    hid_t file_id = H5Fcreate("dset.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
    /* hid_t dataspace_id = H5Screate_simple(sizeof(dims) / sizeof(dims[0]), dims, NULL); */
    hid_t dataspace_id = H5Screate_simple(1, &dims, NULL);
    hid_t dataset_id = H5Dcreate(file_id, "/dataset", float3_id, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    /* Write the dataset. */
    H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, floats);

    /* hid_t file_id = H5Fopen("dset.h5", fapl); */
    /* hid_t dataspace_id = H5Screate_simple(sizeof(dims) / sizeof(dims[0]), dims, NULL); */
    /* hid_t dataset_id = H5Dopen(file_id, "/dataset"); */

    /* float data[X_DIM][Y_DIM][Z_DIM]; */
    struct float3_t from_file[10];
    H5Dread(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, from_file);

    /* for (int i = 0; i < X_DIM; i++) { */
    /*     for (int j = 0; j < Y_DIM; j++) { */
    /*         for (int k = 0; k < Z_DIM; k++) { */
    /*             printf("(%d, %d, %d) = %4f\n", i, j, k, data[i][j][k]); */
    /*         } */
    /*     } */
    /* } */


    H5Tclose(float3_id);
    H5Dclose(dataset_id);
    H5Fclose(file_id);
    H5Pclose(fapl);

    /* unregister hxhim */
    H5VLunregister_connector(vol_id);
    printf("%d\n", H5VLis_connector_registered(HXHIM_VOL_CONNECTOR_NAME));

    return 0;
}
