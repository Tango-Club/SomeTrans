#ifdef _WIN32
#define MKDIR(a) mkdir((a))
#else
#define MKDIR(a) mkdir((a), (S_IRWXU | S_IRWXG | S_IRWXO))
#endif
const int readerLim = 1;
const int writerLim = 10;
const std::string DATABASE_NAME = "tianchi_dts_data";														// 待处理数据库名，无需修改
const std::string SCHEMA_FILE_DIR = "schema_info_dir";														// schema文件夹，无需修改。
const std::string SCHEMA_FILE_NAME = "schema.info";															// schema文件名，无需修改。
const std::string SOURCE_FILE_DIR = "source_file_dir";														// 输入文件夹，无需修改。
const std::string SINK_FILE_DIR = "sink_file_dir";															// 输出文件夹，无需修改。
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";									// 输入文件名，无需修改。
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";										// 输出文件名模板，无需修改。
const std::string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse"; // 待处理表集合，无需修改。

time_t getTime()
{
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
}
void splitStr(const std::string &str, std::vector<std::string> &tokens)
{
	int pre = 0, now = 0;
	for (char c : str)
	{
		now++;
		if (c == '	')
		{
			tokens.emplace_back(str.substr(pre, now - pre - 1));
			pre = now;
		}
	}
	if (pre < now)
		tokens.emplace_back(str.substr(pre, now - pre));
}
bool isInteger(const std::string &s)
{
	bool sign = (s[0] == '-');
	for (size_t i = sign; i < s.length(); i++)
		if (!isdigit(s[i]))
			return false;
	return true;
}
bool isDecimal(const std::string &s)
{
	bool sign = (s[0] == '-');
	bool point = false;
	for (size_t i = sign; i < s.length(); i++)
		if (!isdigit(s[i]))
		{
			if (s[i] == '.' && point == false)
				point = true;
			else
				return false;
		}
	return true;
}
void createPath(std::string path)
{
	DIR *fp = opendir(path.c_str());
	if (fp == nullptr)
		MKDIR(path.c_str());
	else
		closedir(fp);
}