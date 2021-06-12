namespace parallelReadRow
{
	std::atomic<int> aliveProducter;
	std::queue<std::string> rowQueue;
	moodycamel::ConcurrentQueue<std::string> rowQue(10000);
	std::atomic<int> sinkCounter = 0;
	class RowProducter
	{
	public:
		fastIO::IN sourceData;
		RowProducter(std::string path) : sourceData(path) {}
		void product()
		{
			std::string rowStr = sourceData.readLine();
			if (sourceData.IOerror)
				return;
			while (!rowQue.try_enqueue(rowStr))
			{
			}
		}
		void loop()
		{
			while (!sourceData.IOerror)
				product();
			aliveProducter--;
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
			while (!rowQue.try_dequeue(rowStr) && aliveProducter)
			{
			}
			if (!rowStr.length())
				return false;
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
			createPath(path);
			std::vector<std::shared_ptr<std::thread>> threads;
			for (auto &table : tables)
			{
				threads.emplace_back(std::make_shared<std::thread>([&](std::string path)
																   { table.second.sink(path); },
																   path));
			}
			for (auto &tableThread : threads)
				tableThread->join();
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