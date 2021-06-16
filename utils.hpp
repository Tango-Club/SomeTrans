#ifdef _WIN32
#define MKDIR(a) mkdir((a))
#else
#define MKDIR(a) mkdir((a), (S_IRWXU | S_IRWXG | S_IRWXO))
#endif
const int readerLim = 3;
const int writerLim = 10;
const std::string DATABASE_NAME = "tianchi_dts_data";														// 待处理数据库名，无需修改
const std::string SCHEMA_FILE_DIR = "schema_info_dir";														// schema文件夹，无需修改。
const std::string SCHEMA_FILE_NAME = "schema.info";															// schema文件名，无需修改。
const std::string SOURCE_FILE_DIR = "source_file_dir";														// 输入文件夹，无需修改。
const std::string SINK_FILE_DIR = "sink_file_dir";															// 输出文件夹，无需修改。
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";									// 输入文件名，无需修改。
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";										// 输出文件名模板，无需修改。
const std::string CHECK_TABLE_SETS = "customer,district,item,new_orders,order_line,orders,stock,warehouse"; // 待处理表集合，无需修改。

constexpr long long mul[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
							 1000000000, 10000000000LL, 100000000000LL, 1000000000000LL, 10000000000000LL,
							 100000000000000LL, 1000000000000000LL, 10000000000000000LL, 100000000000000000LL};

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

std::string dtos(double x, size_t y)
{
	std::string str;
	if (x < -1e-12)
		str += '-', x = -x;
	x += 1e-8;
	x *= mul[y];
	long long x1 = (long long)floor(x);
	if (x - floor(x) >= 0.5)
		++x1;
	long long x2 = x1 / mul[y], x3 = x1 - x2 * mul[y];
	str += std::to_string(x2);
	if (y > 0)
	{
		str += '.';
		for (size_t i = 1; i < y && x3 * mul[i] < mul[y]; str += '0', ++i)
			;
		str += std::to_string(x3);
	}
	return str;
}