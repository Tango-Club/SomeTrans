namespace parallelReadRow
{
	std::atomic<bool> aliveProducter;
	std::queue<std::string> rowQueue;
	moodycamel::ConcurrentQueue<std::string> rowQue(1000000);
	std::atomic<int> sinkCounter = 0;
	class RowProducter
	{
	public:
		std::string sourceDirectory;
		fastIO::IN sourceData;
		RowProducter(std::string path, std::string sourceDirectoryArg) : sourceData(path)
		{
			sourceDirectory = sourceDirectoryArg;
		}
		void product()
		{
			std::string rowStr = sourceData.readLine();
			if (sourceData.IOerror)
				return;
			while (!rowQue.try_enqueue(rowStr))
			{
				//std::cout << "try to enqueue" << std::endl;
			}
			//std::cout << "in " << rowStr << std::endl;
		}
		void loop()
		{
			aliveProducter = true;
			while (!sourceData.IOerror)
				product();
			aliveProducter = false;
		}
	};
	class RowConsumer
	{
	public:
		std::string sinkDirectory;
		std::unordered_map<std::string, TableInfo> tables;
		RowConsumer(std::unordered_map<std::string, TableInfo> tableinfo, std::string sinkDirectoryArg)
			: tables(tableinfo)
		{
			sinkDirectory = sinkDirectoryArg;
		}
		bool consume()
		{
			std::string rowStr;
			while (aliveProducter && !rowQue.try_dequeue(rowStr))
			{
			}
			if (!rowStr.length())
				return false;
			//std::cout << "out " << rowStr << std::endl;
			std::vector<std::string> vecStr;
			splitStr(rowStr, vecStr);
			auto &op = vecStr[0];
			auto &tableName = vecStr[2];
			if (op == "I")
			{
				tables.at(tableName).readRow(vecStr);
			}
			else
			{
				assert(0);
			}
			return true;
		}
		void sinkData()
		{
			std::string path = sinkDirectory + "/" + SINK_FILE_DIR + "/" + std::to_string(sinkCounter++);
			if (opendir(path.c_str()) == NULL)
				MKDIR(path.c_str());
			std::cout << "path: " << path << std::endl;
			for (auto &table : tables)
				table.second.sink(path);
		}
		void loop()
		{
			while (consume())
			{
			}
			sinkData();
		}
	};
}