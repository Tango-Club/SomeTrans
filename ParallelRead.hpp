namespace parallelReadRow
{
	std::atomic<bool> aliveProducter;
	std::queue<std::string> rowQueue;
	std::mutex mtx;
	std::condition_variable cv;
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
			}
			cv.notify_all();
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
				std::unique_lock<std::mutex> lck(mtx);
				cv.wait(lck);
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
			if (opendir(path.c_str()) == NULL)
				MKDIR(path.c_str());
			std::vector<std::thread> threads;
			for (auto &table : tables)
			{
				threads.emplace_back([&](std::string path)
									 { table.second.sink(path); },
									 path);
			}
			for (auto &tableThread : threads)
				tableThread.join();
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