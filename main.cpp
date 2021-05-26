#include <getopt.h>
#include <stdlib.h>

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

/**
 * @author dts，just for demo.
 * @author pxl, fuck demo.
 */
const std::string DATABASE_NAME = "tianchi_dts_data";														// 待处理数据库名，无需修改
const std::string SCHEMA_FILE_DIR = "schema_info_dir";														// schema文件夹，无需修改。
const std::string SCHEMA_FILE_NAME = "schema.info";															// schema文件名，无需修改。
const std::string SOURCE_FILE_DIR = "source_file_dir";														// 输入文件夹，无需修改。
const std::string SINK_FILE_DIR = "sink_file_dir";															// 输出文件夹，无需修改。
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";									// 输入文件名，无需修改。
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";										// 输出文件名模板，无需修改。
const std::string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse"; // 待处理表集合，无需修改。

/*
表定义类
*/
struct ColumnDefType
{ //数据类型
	std::string typeName;
	std::vector<int> args;
	ColumnDefType(const std::string &typeStr)
	{
	}
};
struct ColumnInfo
{ //列声明
	std::string name;
	int ordinal;
	bool isUnsigned;
	std::string charSet;
	ColumnDefType columnDef;
	int length;
	int precision;
	int scale;
	ColumnInfo(const rapidjson::Document &doc) : columnDef(doc["ColumnDef"].GetString())
	{
		this->name = doc["Name"].GetString();
		this->ordinal = doc["Ordinal"].GetInt();
		this->isUnsigned = doc["Unsigned"].GetBool();
		this->charSet = (doc["CharSet"].IsNull() ? "null" : doc["CharSet"].GetString());
		this->length = (doc["Length"].IsNull() ? -1 : doc["Length"].GetInt());
		this->precision = (doc["Precision"].IsNull() ? -1 : doc["Precision"].GetInt());
		this->scale = (doc["Scale"].IsNull() ? -1 : doc["Scale"].GetInt());
	}
};
struct IndexInfo
{ //索引声明
	//临时
	std::string str;
	IndexInfo(const std::string &indexStr)
	{
		this->str = indexStr;
	}
};
struct PrimeKeyInfo
{ //主键声明
	//临时
	std::string str;
	PrimeKeyInfo(const std::string &primeKeyStr)
	{
		this->str = primeKeyStr;
	}
};
struct TableInfo
{ //表声明
	std::string tableName;
	std::string fromDataBase;
	std::vector<ColumnInfo> columns;
	std::vector<IndexInfo> indexs;
	std::vector<PrimeKeyInfo> primeKeys;
	TableInfo(std::ifstream &schemaInfo)
	{
		std::string tmp;
		schemaInfo >> tmp >> this->fromDataBase >> tmp >> this->tableName;
		int columnNums = 0;
		schemaInfo >> tmp >> tmp >> columnNums;
		while (columnNums--)
		{
			std::string colStr;
			std::getline(schemaInfo, colStr);
			rapidjson::Document doc;
			doc.Parse(colStr.c_str());
			columns.emplace_back(ColumnInfo(doc));
		}
	}
};

class Demo
{
public:
	std::string sourceDirectory;
	std::string sinkDirectory;

public:
	bool initialSchemaInfo()
	{
		std::cout << "Read schema_info_dir/schema.info file and construct table in memory." << std::endl;
		std::string path = sourceDirectory + "//" + SCHEMA_FILE_DIR + "//" + SCHEMA_FILE_NAME;
		std::cout << "path: " << path << std::endl;
		std::ifstream schemaInfo(path);
		if (!schemaInfo.is_open())
		{
			std::cout << "not found: schemaInfo" << std::endl;
			return false;
		}
		return true;
	}

	bool loadSourceData(int dataNumber)
	{
		std::cout << "Read source_file_dir/tianchi_dts_source_data_* file" << std::endl;
		std::string path = sourceDirectory + "//" + SOURCE_FILE_DIR + "//" + SOURCE_FILE_NAME_TEMPLATE + std::to_string(dataNumber);
		std::cout << "path: " << path << std::endl;
		std::ifstream sourceData(path);
		if (!sourceData.is_open())
		{
			std::cout << "not found: sourceData " + std::to_string(dataNumber) << std::endl;
			return false;
		}
		return true;
	}

	void cleanData()
	{
		std::cout << "Clean and sort the source data." << std::endl;
		return;
	}

	void sinkData(std::string path)
	{
		std::cout << "Sink the data." << std::endl;
		std::cout << "path: " << path << std::endl;
		std::cout << "sinkDirectory: " << sinkDirectory << std::endl;
		return;
	}
};

/**
Input: 
1. Disordered source data (in SOURCE_FILE_DIR)
2. Schema information (in std::string)

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
int main(int argc, char *argv[])
{
	Demo *demo = new Demo();

	static struct option long_options[] = {
		{"input_dir", required_argument, 0, 'i'},
		{"output_dir", required_argument, 0, 'o'},
		{"output_db_url", required_argument, 0, 'r'},
		{"output_db_user", required_argument, 0, 'u'},
		{"output_db_passwd", required_argument, 0, 'p'},
		{0, 0, 0, 0}};
	int opt_index;
	int opt;

	while (-1 != (opt = getopt_long(argc, argv, "", long_options, &opt_index)))
	{

		switch (opt)
		{
		case 'i':
			demo->sourceDirectory = optarg;
			break;
		case 'o':
			demo->sinkDirectory = optarg;
			break;
		}
	}

	std::cout << "[Start]\tload schema information." << std::endl;
	// load schema information.
	demo->initialSchemaInfo();
	std::cout << "[End]\tload schema information." << std::endl;

	// load input Start file.
	std::cout << "[Start]\tload input Start file." << std::endl;
	int dataNumber = 1;
	while (demo->loadSourceData(dataNumber))
		dataNumber++;
	std::cout << "[End]\tload input Start file." << std::endl;

	// data clean.
	std::cout << "[Start]\tdata clean." << std::endl;
	demo->cleanData();
	std::cout << "[End]\tdata clean." << std::endl;

	// sink to target file
	std::cout << "[Start]\tsink to target file." << std::endl;
	demo->sinkData(SINK_FILE_DIR);
	std::cout << "[End]\tsink to target file." << std::endl;

	return 0;
}
