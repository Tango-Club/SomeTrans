namespace parallelReadRow
{
	std::atomic<int> aliveProducter;
	moodycamel::BlockingConcurrentQueue<std::shared_ptr<std::string>> rowQue(100000000);
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
			while (!rowQue.try_enqueue(std::make_shared<std::string>(rowStr)))
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
			std::shared_ptr<std::string> rowStr;
			while (!rowQue.wait_dequeue_timed(rowStr, std::chrono::milliseconds(10)) && aliveProducter)
			{
			}
			if (rowStr == nullptr || !rowStr->length())
				return false;
			std::vector<std::string> vecStr;
			splitStr(*rowStr, vecStr);
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
			int p = 0;
			while (consume())
			{
				p++;
				if (p % 10000 == 0)
				{
					sinkData();
				}
			}
			sinkData();
		}
	};
}