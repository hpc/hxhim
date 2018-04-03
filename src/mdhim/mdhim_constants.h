#ifndef __MDHIM_CONSTANTS_H
#define __MDHIM_CONSTANTS_H

#ifdef __cplusplus
extern "C"
{
#endif

#define MDHIM_SUCCESS 0
#define MDHIM_ERROR -1
#define MDHIM_DB_ERROR -2

/* mdhim_options_t configuration values*/
#define MDHIM_DS_NONE 0
#define MDHIM_DS_LEVELDB 1

#define MDHIM_TRANSPORT_NONE     0
#define MDHIM_TRANSPORT_MPI      1
#define MDHIM_TRANSPORT_THALLIUM 2

/* Append option */
#define MDHIM_DB_OVERWRITE 0
#define MDHIM_DB_APPEND 1


#define SECONDARY_GLOBAL_INFO 1
#define SECONDARY_LOCAL_INFO 2

//Message Types
#define RANGESRV_WORK_MSG         1
#define RANGESRV_WORK_SIZE_MSG    2
#define RANGESRV_INFO             3
#define CLIENT_RESPONSE_MSG       4
#define CLIENT_RESPONSE_SIZE_MSG  5

//#define MAX_BULK_OPS 1000000
#define MAX_BULK_OPS 500000

//Maximum size of messages allowed
#define MDHIM_MAX_MSG_SIZE 2147483647

//#define RANGE_SERVER_FACTOR 4
#define MDHIM_MAX_SLICES 2147483647
//32 bit unsigned integer
#define MDHIM_INT_KEY 1
#define MDHIM_LONG_INT_KEY 2
#define MDHIM_FLOAT_KEY 3
#define MDHIM_DOUBLE_KEY 4
#define MDHIM_STRING_KEY 5
//An arbitrary sized key
#define MDHIM_BYTE_KEY 6

//Maximum length of a key
#define MAX_KEY_LEN 1048576

/* The exponent used for the algorithm that determines the range server

   This exponent, should cover the number of characters in our alphabet
   if 2 is raised to that power. If the exponent is 6, then, 64 characters are covered
*/
#define MDHIM_ALPHABET_EXPONENT 6

/* Storage Methods */
#define LEVELDB 1 //LEVELDB storage method
#define MYSQLDB 3
#define ROCKSDB 4 //RocksDB
/* mdhim_store_t flags */
#define MDHIM_CREATE 1 //Implies read/write
#define MDHIM_RDONLY 2
#define MDHIM_RDWR 3

/* Keys for stats database */
#define MDHIM_MAX_STAT 1
#define MDHIM_MIN_STAT 2
#define MDHIM_NUM_STAT 3

#ifdef __cplusplus
}
#endif

#endif
