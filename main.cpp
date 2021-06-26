#include <dirent.h>
#include <getopt.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include <ext/pb_ds/priority_queue.hpp>

#include <any>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "lightweightsemaphore.h"
#include "blockingconcurrentqueue.h"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "utils.hpp"

#include "FastIO.hpp"

#include "SchemaReader.hpp"

#include "ParallelRead.hpp"

/**
 * @author dts，just for demo.
 * @author pxl, fuck demo.
 */

class Demo
{
public:
	std::string sourceDirectory;
	std::string sinkDirectory;
	std::unordered_map<std::string, TableInfo> tables;

public:
	bool initialSchemaInfo()
	{
		time_t startTime = getTime();
		std::cout << "Read schema_info_dir/schema.info file and construct table in memory." << std::endl;
		std::string path = sourceDirectory + "/" + SCHEMA_FILE_DIR + "/" + SCHEMA_FILE_NAME;
		std::cout << "path: " << path << std::endl;
		std::ifstream schemaInfo(path);
		if (!schemaInfo.is_open())
		{
			std::cout << "not found: schemaInfo" << std::endl;
			return false;
		}
		while (schemaInfo.peek() != EOF)
		{
			TableInfo table(schemaInfo);
			assert(table.tableName != "");
			tables.insert({table.tableName, table});
			std::cout << "read table:"
					  << " " << table.tableName << " success." << std::endl;
		}
		time_t endTime = getTime();
		std::cout << "initialSchemaInfo time use : " << endTime - startTime << std::endl;
		return true;
	}
	bool loadSourceData(int dataNumber)
	{
		time_t startTime = getTime();

		std::string sinkPath = sinkDirectory + "/" + SINK_FILE_DIR;
		createPath(sinkPath);
		std::vector<std::shared_ptr<std::thread>> threads;
		std::vector<std::shared_ptr<parallelReadRow::RowProducter>> producters;
		for (int i = dataNumber; i < dataNumber + readerLim; i++)
		{
			std::cout << "Read source_file_dir/tianchi_dts_source_data_* file" << std::endl;
			std::string path = sourceDirectory + "/" + SOURCE_FILE_DIR + "/" + SOURCE_FILE_NAME_TEMPLATE + std::to_string(i);
			std::cout << "path: " << path << std::endl;
			std::ifstream sourceData(path);
			if (!sourceData.is_open())
			{
				std::cout << "not found: sourceData " << i << std::endl;
				break;
			}
			producters.emplace_back(std::make_shared<parallelReadRow::RowProducter>(path));
		}
		for (auto &producter : producters)
		{
			parallelReadRow::aliveProducter++;
			threads.emplace_back(std::make_shared<std::thread>([&]()
															   { producter->loop(); }));
		}
		if (!parallelReadRow::aliveProducter)
			return false;
		std::vector<parallelReadRow::RowConsumer> consumers;
		for (int i = 1; i <= writerLim; i++)
			consumers.emplace_back(tables, sinkDirectory);

		for (auto &consumer : consumers)
			threads.emplace_back(std::make_shared<std::thread>([&]()
															   { consumer.loop(); }));

		for (auto &tableThread : threads)
			tableThread->join();
		time_t endTime = getTime();
		std::cout << "loadSourceData time use : " << endTime - startTime << std::endl;
		return true;
	}
	void clearQue()
	{
		moodycamel::BlockingConcurrentQueue<std::shared_ptr<std::string>> newQue;
		std::swap(newQue, parallelReadRow::rowQue);
	}
	void mergeData()
	{
		time_t startTime = getTime();
		clearQue();

		std::string path = sinkDirectory + "/" + SINK_FILE_DIR;
		createPath(path);

		std::vector<std::shared_ptr<std::thread>> threads;
		for (auto &table : tables)
			threads.emplace_back(std::make_shared<std::thread>([&](std::string path)
															   { table.second.finalSink(path); },
															   path));
		for (auto &tableThread : threads)
			tableThread->join();
		time_t endTime = getTime();
		std::cout << "mergeData time use : " << endTime - startTime << std::endl;
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
#include <unistd.h>
int main(int argc, char *argv[])
{
/*
    auto file = std::make_shared<fastIO::IN>("tianchi_dts_source_data_50");
    std::string str;
    freopen("outfile2","w",stdout);
    while( (str=file->readLine())!="")
    {
        if(str=="")break;
        std::cout<<str<<std::endl;
    }



    return 0;*/

    time_t startTime = getTime();
	std::shared_ptr<Demo> demo(new Demo());

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
		dataNumber += readerLim;
	std::cout << "[End]\tload input Start file." << std::endl;

	std::cout << "[Start]\tmerge data file." << std::endl;
	demo->mergeData();
	std::cout << "[End]\tmerge data file." << std::endl;

	time_t endTime = getTime();
	std::cout << "All time use : " << endTime - startTime << std::endl;
	return 0;
}
