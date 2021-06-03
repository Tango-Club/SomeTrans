#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <getopt.h>

#include <dirent.h>
#include "schema.h"

using namespace std;

//目录、文件名、表名常量
const string DATABASE_NAME = "tianchi_dts_data";                                                             // 待处理数据库名，无需修改
const string SCHEMA_FILE_DIR = "schema_info_dir";                                                            // schema文件夹，无需修改。
const string SCHEMA_FILE_NAME = "schema.info";                                                               // schema文件名，无需修改。
const string SOURCE_FILE_DIR = "source_file_dir";                                                            // 输入文件夹，无需修改。
const string SINK_FILE_DIR = "sink_file_dir";                                                                // 输出文件夹，无需修改。
const string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";                                         // 输入文件名，无需修改。
const string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";                                             // 输出文件名模板，无需修改。
const string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse";       // 待处理表集合，无需修改。



class Demo {
    public:
    string sourceDirectory;
    string sinkDirectory;
    unordered_map<string, TableInfo> table_map;

    public:
    Demo() {
        table_map["customer"]   = (TableInfo){0, 0, 0, 0, "customer", 0, 0, 0, 0, 0, 0, 0};
        table_map["district"]   = (TableInfo){0, 0, 0, 0, "district", 0, 0, 0, 0, 0, 0, 0};
        table_map["item"]       = (TableInfo){0, 0, 0, 0, "item", 0, 0, 0, 0, 0, 0, 0};
        table_map["new_orders"] = (TableInfo){0, 0, 0, 0, "new_orders", 0, 0, 0, 0, 0, 0, 0};
        table_map["order_line"] = (TableInfo){0, 0, 0, 0, "order_line", 0, 0, 0, 0, 0, 0, 0};
        table_map["orders"]     = (TableInfo){0, 0, 0, 0, "orders", 0, 0, 0, 0, 0, 0, 0};
        table_map["stock"]      = (TableInfo){0, 0, 0, 0, "stock", 0, 0, 0, 0, 0, 0, 0};
        table_map["warehouse"]  = (TableInfo){0, 0, 0, 0, "warehouse", 0, 0, 0, 0, 0, 0, 0};
    }

    void initialSchemaInfo(string path, string tables) {
        cout << "Start reading schema_info_dir/schema.info file and construct table in memory." << endl;
        string file = SCHEMA_FILE_DIR + '/' + SCHEMA_FILE_NAME;

        FILE *fp = fopen(file.c_str(), "r");
        if (NULL == fp) {
            ERROR("Fail to open the file! %s\n", file.c_str());
            exit(-1);
        }

        size_t len = 0;
        char *line = NULL;
        ssize_t read;
        while ((read = getline(&line, &len, fp)) != -1) {
            line[read - 1] = '\0';
            char table_name[64];
	    // load table name:
	    //[DATABASE] tianchi_dts_data [TABLE] ---> position:36 $table_name
            strcpy(table_name, line + 36);
            TableInfo& table = table_map.at(table_name);

            // load column num
	    char column_num[64];
            // COLUMN NUMBER ---> position:14 $column_name
            getline(&line, &len, fp);
            strcpy(column_num, line + 14);
            table.col_num = atoi(column_num);
            table.columns = (Column*) calloc(table.col_num, sizeof(Column));
	    
     	    // load columns
            for (int i = 0; i < table.col_num; i++){
                read = getline(&line, &len, fp);
                column_json(line, table.columns + i, len);
            }

	    // load index num
            getline(&line, &len, fp);
	    char index_num[64];
            strcpy(index_num, line + 13);
            table.index_num = atoi(index_num);
            DEBUG("INDEX NUM: %d\n", table.index_num);
            table.indexes = (Index*) calloc(table.index_num, sizeof(Index));
	    // load indexes
            for (int i = 0; i < table.index_num; i++){
                read = getline(&line, &len, fp);
                DEBUG("LINE: %s, %ld\n", line, read);
                index_json(line, table.indexes+i, len);
            }

            // load pk num
            getline(&line, &len, fp);
	    char pk_num[64];
            strcpy(pk_num, line + 19);
            table.pkey_num = atoi(pk_num);
            DEBUG("PKEY NUM: %d\n", table.pkey_num);
            table.pkeys = (PrimaryKey*) calloc(table.pkey_num, sizeof(PrimaryKey));

            uint8_t pkey_size[64] = {0}; 
            int pkey_index_size = 0;
	    // load pks
            for (int i = 0; i < table.pkey_num; i++){
                read = getline(&line, &len, fp);
                pkey_json(line, table.pkeys+i, len);

                for (int j = 0; j < table.col_num; j++){
                    if (strcmp(table.pkeys[i].col, table.columns[j].name) == 0){
                        table.pkey_index = table.pkey_index | (1 << j);
                        table.pkey_shift_index[j] = i;
                        pkey_size[i] = STORAGE_BYTES[table.columns[j].type];
                        pkey_index_size += pkey_size[i];
                        break;
                    }
                }

            }
            for (int i=0; i < 4; i++){
                table.pkey_size += pkey_size[i];
            }
            for (int i = 0; i < table.pkey_num; i++){
                pkey_index_size -= pkey_size[i];
                table.pkey_shift[i] = pkey_index_size;
            }
 
        }
        free(line);
        fclose(fp);
	cout << "[SUCCESS] construct table in memory." << endl;
        return;
    }


    // load source file randomly
    void loadSourceData(string path) {
        cout << "Start reading source_file_dir/tianchi_dts_source_data_* file" << endl;
        string file = SOURCE_FILE_DIR;
        DIR *dirptr = opendir(file.c_str());
        struct dirent *entry;
        while (entry = readdir(dirptr)) {
	    //name: tianchi_dts_source_data_[0-9]+
            if (entry->d_name[0] == 't'){
                string data_file = file + '/' + entry->d_name;

                struct stat statbuf;
                int ret = stat(data_file.c_str(), &statbuf);
                int file_no = atoi(entry->d_name + 24);
                INFO("filename=%s, id=%d, size=%ld", entry->d_name, file_no, statbuf.st_size);

                FILE *fp = fopen(data_file.c_str(), "r");
                if (NULL == fp) {
                    ERROR("open file failed! %s\n", data_file.c_str());
                    exit(-1);
                }

                ssize_t offset = 0;
                size_t  len = 0;
                char    *line = NULL;
                ssize_t read;
                while ((read = getline(&line, &len, fp)) != -1) {
                    char table_name[64];
                    const unsigned char *char_ptr;
                    unsigned char *name_ptr;
                    for (char_ptr = (const unsigned char *) line + 19, name_ptr = (unsigned char *)table_name; ; ++char_ptr, ++name_ptr){
                        if (*char_ptr != '\t'){
                            *name_ptr = *char_ptr;
                        }else{
                            *name_ptr = '\0';
                            break;
                        }
                    }
                    TableInfo& table = table_map.at(table_name);

                    int     line_error = 0;
                    char    *new_line = 0;
                    char *line_start_ptr = (char*)(char_ptr + 1);
                    char *line_pos_ptr = 0; 
                    char *new_line_pos_ptr = 0;
                    ColumnData *column_data = (ColumnData*)calloc(1, sizeof(ColumnData));
                    column_data->index = (char*)calloc(1, table.pkey_size);
                    for (int i=0; i < table.col_num; i++) {
                        Column column = table.columns[i];
                        switch (column.type){
                            case Tinyint:
                            case Smallint:
                            case Int:
                            case Bigint:
				{
                                char *tmp_ptr = (char *)(char_ptr + 1);
                                int is_int_error = 0;
                                while(*(++char_ptr) != '\t' && *(char_ptr) != '\n'){
                                    char tmp_char = *(char_ptr);
                                    int tmp_char_c = tmp_char - '0';
                                    if (9 < tmp_char_c || 0 > tmp_char_c){
                                        DEBUG("error Int line: %d, %s", i, line);
                                        is_int_error = 1;
                                        if (line_error){
                                            memcpy(new_line_pos_ptr, line_pos_ptr, tmp_ptr-line_pos_ptr);
                                            new_line_pos_ptr += (tmp_ptr-line_pos_ptr);
                                            *(new_line_pos_ptr) = '0';
                                            new_line_pos_ptr++;
                                            line_pos_ptr = (char*)char_ptr;
                                        } else {
                                            line_error = 1;
                                            new_line = (char*)calloc(1, read+18);
                                            memcpy(new_line, line_start_ptr, tmp_ptr-line_start_ptr);
                                            new_line_pos_ptr = new_line+(tmp_ptr-line_start_ptr);
                                            *(new_line_pos_ptr) = '0';
                                            new_line_pos_ptr++;
                                            line_pos_ptr = (char*)char_ptr;
                                        }
                                        break;
                                    }
                                }

                                if (!is_int_error){
                                    if ((1 << i) & table.pkey_index){
                                        int start_pos = (table.pkey_size - table.pkey_shift[table.pkey_shift_index[i]]-1);
                                        uint64_t val = htobe64(atoi(tmp_ptr));
                                        int store_size = STORAGE_BYTES[column.type];
                                        char *val_char = (char*)&val;
                                        for (int i=0; i < store_size; i++){
                                            memcpy(column_data->index+start_pos-i, val_char + 8 - i - 1, 1);
                                        }
                                    }
                                }

                                }
                                while(*(char_ptr) != '\t' && *(char_ptr) != '\n'){
                                    char_ptr++;
                                    line_pos_ptr++;
                                }
                                break;
                            case Decimal:
                                {
                                char tmp_string[65] = {0};
                                char *tmp_ptr = (char *)(char_ptr + 1);
                                int ii = 0;
                                while(*(++char_ptr) != '\t' && *(char_ptr) != '\n'){
                                    char tmp_char = *(char_ptr);
                                    int tmp_char_c = tmp_char - '0';
                                    if (9 < tmp_char_c || (tmp_char_c < 0 && tmp_char_c != -2 && tmp_char_c != -3)){
                                        DEBUG("error Decimal line: %d, %s", i, line);
                                        if (line_error){
                                            memcpy(new_line_pos_ptr, line_pos_ptr, tmp_ptr-line_pos_ptr);
                                            new_line_pos_ptr += (tmp_ptr-line_pos_ptr);
                                            *(new_line_pos_ptr) = '0';
                                            new_line_pos_ptr++;
                                            line_pos_ptr = (char*)char_ptr;
                                        }else{
                                            line_error = 1;
                                            new_line = (char*)calloc(1, read+18);
                                            memcpy(new_line, line_start_ptr, tmp_ptr-line_start_ptr);
                                            new_line_pos_ptr = new_line+(tmp_ptr-line_start_ptr);
                                            *(new_line_pos_ptr) = '0';
                                            new_line_pos_ptr++;
                                            line_pos_ptr = (char*)char_ptr;
                                        }
                                        while(*(char_ptr) != '\t' && *(char_ptr) != '\n'){
                                            char_ptr++;
                                            line_pos_ptr++;
                                        }
                                        goto DECIMAL_END;
                                        break;
                                    }
                                    tmp_string[ii++] = *char_ptr;
                                }
                                double val = atof(tmp_string);
                                if (column.scale){
                                    double xx = pow(10, -column.scale-4);
                                    val = val+xx*SIGN_BIT(val);
                                    sprintf(tmp_string, "%.*lf", column.scale, val);
                                }else{
                                    sprintf(tmp_string, "%.*lf", column.scale, val);
                                }
                                int count = strlen(tmp_string);
                                if (ii > count){
                                    DEBUG("error Decimal line: %d, %s", i, line);
                                    if (line_error){
                                        memcpy(new_line_pos_ptr, line_pos_ptr, ((char*)char_ptr-line_pos_ptr)-ii);
                                        new_line_pos_ptr += ((char*)char_ptr-line_pos_ptr-ii);
                                        memcpy(new_line_pos_ptr, tmp_string, count);
                                        new_line_pos_ptr += count;
                                        line_pos_ptr = (char*)char_ptr;
                                    }else{
                                        line_error = 1;
                                        new_line = (char*)calloc(1, read+18);
                                        memcpy(new_line, line_start_ptr, ((char*)char_ptr-line_start_ptr)-ii);
                                        new_line_pos_ptr = new_line+((char*)char_ptr-line_start_ptr)-ii;
                                        memcpy(new_line_pos_ptr, tmp_string, count);
                                        new_line_pos_ptr += count;
                                        line_pos_ptr = (char*)char_ptr;
                                    }
                                }
                                }
                            DECIMAL_END:
                                break;
                            case Char:
                                ++char_ptr;
                                if (*(char_ptr+column.length) != '\t' && *(char_ptr+column.length) != '\n'){
                                    DEBUG("error Char line: %d, %s", i, line);

                                    if (line_error){
                                        memcpy(new_line_pos_ptr, line_pos_ptr, (char*)char_ptr-line_pos_ptr+column.length);
                                        new_line_pos_ptr += ((char*)char_ptr-line_pos_ptr+column.length);
                                        line_pos_ptr = (char*)char_ptr+column.length;
                                    }else{
                                        line_error = 1;
                                        new_line = (char*)calloc(1, read+18);
                                        memcpy(new_line, line_start_ptr, (char*)char_ptr-line_start_ptr+column.length);
                                        new_line_pos_ptr = new_line+((char*)char_ptr-line_start_ptr+column.length);
                                        line_pos_ptr = (char*)char_ptr+column.length;
                                    }

                                    char_ptr += column.length;
                                    while(*(char_ptr) != '\t' && *(char_ptr) != '\n'){
                                        char_ptr++;
                                        line_pos_ptr++;
                                    }
                                }else{
                                    char_ptr += column.length;
                                }
                                break;
                            case Varchar:
                            case Text:
                                {
                                int count = 0;
                                char *tmp_ptr = (char *)(char_ptr + 1);
                                while(*(++char_ptr) != '\t' && *(char_ptr) != '\n'){
                                    count++;
                                }
                                if (count > column.length){
                                    DEBUG("error Varchar line: %d, %s", i, line);
                                    if (line_error){
                                        memcpy(new_line_pos_ptr, line_pos_ptr, tmp_ptr-line_pos_ptr+column.length);
                                        new_line_pos_ptr += (tmp_ptr-line_pos_ptr+column.length);
                                        line_pos_ptr = tmp_ptr+count;
                                    }else{
                                        line_error = 1;
                                        new_line = (char*)calloc(1, read+18);
                                        memcpy(new_line, line_start_ptr, tmp_ptr-line_start_ptr+column.length);
                                        new_line_pos_ptr = new_line+(tmp_ptr-line_start_ptr+column.length);
                                        line_pos_ptr = tmp_ptr+count;
                                    }
                                }
                                }
                                break;
                            case Datetime:
                                char *tmp_ptr = (char *)(char_ptr + 1);
                                while(*(++char_ptr) != '\t' && *(char_ptr) != '\n'){
                                    char tmp_char = *(char_ptr);
                                    int tmp_char_c = tmp_char - '0';
                                    if (10 < tmp_char_c || (tmp_char_c < 0 && tmp_char_c != -16 && tmp_char_c != -2 && tmp_char_c != -3)){
                                        DEBUG("error Datetime line: %d, %s", i, line);
                                        if (line_error){
                                            memcpy(new_line_pos_ptr, line_pos_ptr, tmp_ptr-line_pos_ptr);
                                            new_line_pos_ptr += (tmp_ptr-line_pos_ptr);
                                            strcpy(new_line_pos_ptr, "2020-04-01 00:00:00.0");
                                            new_line_pos_ptr += 21;
                                            line_pos_ptr = line_pos_ptr+((char*)char_ptr-line_pos_ptr);
                                        }else{
                                            line_error = 1;
                                            new_line = (char*)calloc(1, read+18);
                                            memcpy(new_line, line_start_ptr, tmp_ptr-line_start_ptr);
                                            new_line_pos_ptr = new_line+(tmp_ptr-line_start_ptr);
                                            strcpy(new_line_pos_ptr, "2020-04-01 00:00:00.0");
                                            new_line_pos_ptr += 21;
                                            line_pos_ptr = line_start_ptr+((char*)char_ptr-line_start_ptr);
                                        }
                                        while(*(char_ptr) != '\t' && *(char_ptr) != '\n'){
                                            char_ptr++;
                                            line_pos_ptr++;
                                        }
                                        break;
                                    }
                                }
                                break;
                        }

                    }
                    if (line_error){
                        char_ptr++;
                        if ((char*)char_ptr-line_pos_ptr){
                            memcpy(new_line_pos_ptr, line_pos_ptr, (char*)char_ptr-line_pos_ptr+1);
                        }
                        DEBUG("error NEW line: %s", new_line);
                        column_data->mem_data = new_line;
                        column_data->data = 1 << 31 | file_no << 27 | offset;
                    }else{
                        column_data->data = file_no << 27 | offset;
                    }

                    // sort 
                    if (!table.column_data){
                        table.column_data = (ColumnData*)calloc(1, sizeof(ColumnData));
                        table.column_data->next = column_data;
                    }else{
                        ColumnData *pre = table.column_data;
                        ColumnData *tmp = pre->next;
                        while(1){
                            int res = memcmp(tmp->index, column_data->index, table.pkey_size);
                            if (res > 0){
                                column_data->next = tmp;
                                pre->next = column_data;
                                break;
                            }else if(res == 0){ 
                                break;
                            }
                            pre = tmp;
                            tmp = tmp->next;
                            if (!tmp){
                                pre->next = column_data;
                                break;
                            }
                        }
                    }
                    offset += read;
                }
                free(line);
                fclose(fp);
            }
        }
        closedir(dirptr);
        return;
    }
    
    void sinkData(string path) {
        cout << "Sink the data." << endl;
        unordered_map<int, FILE*> fd_map;
        string sink_file_dir = SINK_FILE_DIR;
        DIR *dirptr = opendir(sink_file_dir.c_str());
        struct dirent *entry;
        while (entry=readdir(dirptr)) {
            if (entry->d_name[0] == 't'){
                string data_file = sink_file_dir + '/' + entry->d_name;

                struct stat statbuf;
                int ret = stat(data_file.c_str(), &statbuf);
                int file_no = atoi(entry->d_name + 24);

                FILE *fp = fopen(data_file.c_str(), "r");
                if (fp == NULL) {
                    ERROR("Failed to open the file! %s\n", data_file.c_str());
                    exit(-1);
                }
                fd_map.emplace(file_no, fp);
            }
        }

        mkdir(sinkDirectory.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        string file = sinkDirectory + '/' + SINK_FILE_NAME_TEMPLATE;
        INFO("Sink file template name: %s", file.c_str());
        for (auto iter : table_map) {
            string data_file = file + iter.first;
            int table_name_len = iter.first.size();
            INFO("Start sinking file name: %s", data_file.c_str());
            FILE *fp = fopen(data_file.c_str(), "wa");
            if (NULL == fp) {
                ERROR("Failed to open the file! %s\n", data_file.c_str());
                exit(-1);
            }
            TableInfo table = iter.second;
            ColumnData *next;
            if (table.column_data){
                next = table.column_data->next;
            } else {
                next = table.column_data;
            }
            while (next) {
                ColumnData *tmp = next->next;
                if (next->mem_data){
                    int len = strlen(next->mem_data);
                    if (!tmp){
                        if (*(next->mem_data+len-1) == '\n'){
                            len--;
                        }
                        fwrite(next->mem_data, len, 1, fp);
                    } else {
                        fwrite(next->mem_data, len, 1, fp);
                        if (*(next->mem_data+len-1) != '\n'){
                            char c = '\n';
                            fwrite(&c, 1, 1, fp);
                        }
                    }
                } else {
                    FILE *fd = fd_map.at((next->data << 1) >> 28);
                    long offset = (next->data & 0x7ffffff) + 19 + 1 + table_name_len;
                    fseek(fd, offset, SEEK_SET);
                    size_t len = 0;
                    char *line = NULL;
                    ssize_t read;
                    if ((read = getline(&line, &len, fd)) != -1) {
                        if (!tmp){
                            if (*(line+read-1) == '\n'){
                                read--;
                            }
                            fwrite(line, read, 1, fp);
                        } else {
                            fwrite(line, read, 1, fp);
                            if (*(line+read-1) != '\n'){
                                char c = '\n';
                                fwrite(&c, 1, 1, fp);
                            }
                        }
                    } else {
                        ERROR("Failed to sink data.");
                    }
                    free(line);
                }
                next = next->next;
            }
	    INFO("[SUCCESS] sink the file: %s", data_file.c_str());
            fclose(fp);
        }
        return;
    }
};

/**
Input: 
1. Disordered source data (in SOURCE_FILE_DIR)
2. Schema information (in SCHEMA_FILE_DIR)

Process:
    data clean: 
    1) duplicate primary key data;
    2) exceed char length data;
    3) error date time type data;
    4) error decimal type data;
    5) error data type.

    sort by pk

Output:
1. Sorted data of each table (out SINK_FILE_DIR)

**/
int main(int argc, char *argv[]) {
    Demo *demo = new Demo();

    static struct option long_options[] = {
                    {"input_dir",           required_argument, 0,  'i' },
                    {"output_dir",          required_argument, 0,  'o' },
                    {"output_db_url",       required_argument, 0,  'h' },
                    {"output_db_user",      required_argument, 0,  'u' },
                    {"output_db_passwd",    required_argument, 0,  'p' },
                    {0,                     0                , 0,   0 }
               };
    int opt_index;
    int opt;

    while (-1 != (opt = getopt_long(argc, argv, "", long_options, &opt_index))) {

        switch (opt) {
            case 'i' :
                demo->sourceDirectory = optarg;
                break;
            case 'o' :
                demo->sinkDirectory = optarg;
                break;
	    default  :
		cout << "Unrecognize options. arg: " << optarg << endl;
		break;
        }
    }
    cout << "Input sourceDirectory: " << demo->sourceDirectory << endl;
    cout << "Input sinkDirectory:" << demo->sinkDirectory << endl;

    cout << "[Start]\tload schema information." << endl;
    // load schema information.
    demo->initialSchemaInfo(SCHEMA_FILE_DIR, CHECK_TABLE_SETS);
    cout << "[End]\tload schema information." << endl;

    // load input Start file.
    cout << "[Start]\tload input Start file." << endl;
    demo->loadSourceData(SOURCE_FILE_DIR);
    cout << "[End]\tload input Start file." << endl;

    // sink to target file
    cout << "[Start]\tsink to target file." << endl;
    demo->sinkData(SINK_FILE_DIR);
    cout << "[End]\tsink to target file." << endl;

    return 0;
}
