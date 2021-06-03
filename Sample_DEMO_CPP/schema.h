#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <math.h>
#include <pthread.h>

#if defined(OS_MAC)
#include <libkern/OSByteOrder.h>
#define htobe64(v) OSSwapHostToBigInt64(v)
#else
#define _BSD_SOURCE             /* See feature_test_macros(7) */
#include <endian.h>
#endif

#ifdef Debugn
  #define debug_assert(v) assert(v)
  #define DEBUG(format,...) \
        printf("DEBUG THREAD: %lu, FILE: " __FILE__ ", LINE: %d: " format "\n", pthread_self(), __LINE__, ##__VA_ARGS__);
#else
  #define debug_assert(v)
  #define DEBUG(format,...)
#endif 

#define INFO(format,...) \
    printf("INFO THREAD: %lu, LINE: %d: " format "\n", pthread_self(), __LINE__, ##__VA_ARGS__);

#define ERROR(format,...) \
    printf("ERROR THREAD: %lu, LINE: %d: " format "\n", pthread_self(), __LINE__, ##__VA_ARGS__);

#define SIGN_BIT(x) \
        (((signed char*) &x)[sizeof(x) - 1] >> 7 | 1)

void get_line(char **lineptr, size_t *n, FILE *stream);

void get_line(char **lineptr, size_t *n, FILE *stream){

}

enum TYPE { Tinyint, Smallint, Int, Bigint, Decimal, Varchar, Char, Text, Datetime };
int STORAGE_BYTES[3] = {1, 2, 4};

typedef struct Column {
    char        name[10];
    bool        is_unsigned;        
    char        charset[6];         
    char        columndef[16];
    enum TYPE   type;
    uint16_t    length;
    uint8_t     precision;
    uint8_t     scale;
} Column;

typedef struct ColumnData {
    struct ColumnData   *next;
    char                *index;
    char                *mem_data;
    uint32_t            data;
} ColumnData;

typedef struct Index {
    char        name[64];
    char        col[64];
    bool        is_primary;
    bool        is_unique;
} Index;

typedef struct PrimaryKey {
    char        name[64];
    char        col[64];
    bool        is_primary;
    bool        is_unique;
} PrimaryKey;

typedef struct TableInfo {
    ColumnData  *column_data;
    Column      *columns;
    Index       *indexes;
    PrimaryKey  *pkeys;
    char        name[64];

    uint8_t     col_num;
    uint8_t     index_num;
    uint8_t     pkey_num;
    uint8_t     pkey_size;
    uint32_t    pkey_index;
    uint8_t     pkey_shift_index[32];
    uint8_t     pkey_shift[4];
} TableInfo;

// load column json from file: Parsing the following string for examle 
// {"Name":"i_id","Ordinal":1,"Unsigned":false,"CharSet":null,"ColumnDef":"int(11)","Length":null,"Precision":10,"Scale":0}
void column_json(char *line, Column *column, size_t len){
    const unsigned char *char_ptr;
    unsigned char *name_ptr;
    for (char_ptr = (const unsigned char *) line+9, name_ptr = (unsigned char *)column->name; ; ++char_ptr, ++name_ptr) {
        if (*char_ptr != '"'){
            *name_ptr = *char_ptr;
        } else {
            break;
        }
    }
    DEBUG("COLUMN_NAME: %s, ", column->name);

    char_ptr += 12;
    for (;; ++char_ptr, ++name_ptr){
        if (*char_ptr == ','){
            char_ptr += 12;
            if (*char_ptr == 'f'){
                column->is_unsigned = false;
                char_ptr += 16;
            } else {
                column->is_unsigned = true;
                char_ptr += 15;
            }
            DEBUG("is_unsigned: %d, ", column->is_unsigned);
            break;
        }
    }
    if (*char_ptr == 'n'){ // null
        char_ptr += 18;
    } else {
        strcpy(column->charset, "latin1");
        char_ptr += 22;
    }
    DEBUG("charset: %s, ", column->charset);

    DEBUG("charset: %c, ", char_ptr[0]);
    switch (char_ptr[0]){
        case 't':
            if (*(char_ptr+1) == 'i') {
                column->type = Tinyint;
            }else{
                column->type = Text;
            }
            break;
        case 'b':
            column->type = Bigint;
            break;
        case 's':
            column->type = Smallint;
            break;
        case 'i':
            column->type = Int;
            break;
        case 'd':
            if (*(char_ptr+1) == 'e') {
                column->type = Decimal;
            } else {
                column->type = Datetime;
            }
            break;
        case 'v':
            column->type = Varchar;
            break;
        case 'c':
            column->type = Char;
            break;
        default:
            ERROR("ERROR TYPE: %c", *char_ptr);
            exit(-1);
    }
    DEBUG("TYPE: %d, ", column->type);

    for (name_ptr = (unsigned char *)column->columndef; ; ++char_ptr, ++name_ptr) {
        if (*char_ptr != '"') {
            *name_ptr = *char_ptr;
        } else {
            break;
        }
    }
    char_ptr += 11;
    DEBUG("columndef: %s, ", column->columndef);

    if (*char_ptr == 'n') { // null
        column->length = 0;
        char_ptr += 17;
    } else {
        unsigned char *tmp = (unsigned char *)calloc(1, 8);
        for (name_ptr = tmp; ; ++char_ptr, ++name_ptr) {
            if (*char_ptr != ',') {
                *name_ptr = *char_ptr;
            } else {
                break;
            }
        }
        column->length = atoi((const char *)tmp);
        free(tmp);
        char_ptr += 13;
    }
    DEBUG("length: %d, ", column->length);

    // precision
    if (*char_ptr == 'n') { // null
        column->precision = 0;
        char_ptr += 13;
    } else {
        unsigned char *tmp = (unsigned char *)calloc(1, 8);
        for (name_ptr = tmp; ; ++char_ptr, ++name_ptr) {
            if (*char_ptr != ',') {
                *name_ptr = *char_ptr;
            } else {
                break;
            }
        }
        column->precision = atoi((const char *)tmp);
        free(tmp);
        char_ptr += 9;
    }
    DEBUG("precision: %d, ", column->precision);

    // scale
    if (*char_ptr == 'n'){ // null
        // column->scale = 0;
        // char_ptr += 13;
    } else {
        unsigned char *tmp = (unsigned char *)calloc(1, 8);
        for (name_ptr = tmp; ; ++char_ptr, ++name_ptr) {
            if (*char_ptr != '}'){
                *name_ptr = *char_ptr;
            } else {
                break;
            }
        }
        column->scale = atoi((const char *)tmp);
        free(tmp);
    }
    DEBUG("scale: %d\n", column->scale);
}

// load index json from file: Parsing the following string for examle
// {"IndexName":"PRIMARY","IndexCols":["i_id"],"Primary":true,"Unique":true}
void index_json(char *line, Index *index, size_t len){
    const unsigned char *char_ptr;
    unsigned char *name_ptr;
    for (char_ptr = (const unsigned char *) line+14, name_ptr = (unsigned char *)index->name; ; ++char_ptr, ++name_ptr){
        if (*char_ptr != '"'){
            *name_ptr = *char_ptr;
        }else{
            break;
        }
    }
    DEBUG("INDEX: %s, ", index->name);

    char_ptr += 16;
    for (name_ptr = (unsigned char *)index->col; ; ++char_ptr, ++name_ptr){
        if (*char_ptr != '"'){
            *name_ptr = *char_ptr;
        }else{
            break;
        }
    }
    DEBUG("col: %s, ", index->col);

    char_ptr += 13;
    if (*char_ptr == 'f'){
        index->is_primary = false;
        char_ptr += 15;
    }else{
        index->is_primary = true;
        char_ptr += 14;
    }
    DEBUG("is_primary: %d, ", index->is_primary);

    if (*char_ptr == 'f'){
        index->is_unique = false;
    }else{
        index->is_unique = true;
    }
    DEBUG("is_unique: %d \n", index->is_unique);
}

// load primary index json from file: Parsing the following string for examle
//{"IndexName":"PRIMARY","IndexCols":["i_id"],"Primary":true,"Unique":true}
void pkey_json(char *line, PrimaryKey *index, size_t len){
    const unsigned char *char_ptr;
    unsigned char *name_ptr;
    for (char_ptr = (const unsigned char *) line+14, name_ptr = (unsigned char *)index->name; ; ++char_ptr, ++name_ptr){
        if (*char_ptr != '"'){
            *name_ptr = *char_ptr;
        }else{
            break;
        }
    }
    char_ptr += 16;
    for (name_ptr = (unsigned char *)index->col; ; ++char_ptr, ++name_ptr){
        if (*char_ptr != '"'){
            *name_ptr = *char_ptr;
        }else{
            break;
        }
    }
    char_ptr += 13;
    if (*char_ptr == 'f'){
        index->is_primary = false;
        char_ptr += 15;
    } else {
        index->is_primary = true;
        char_ptr += 14;
    }
    if (*char_ptr == 'f') {
        index->is_unique = false;
    } else {
        index->is_unique = true;
    }
}
