/*
数据库定义
*/
enum class ValueType : int
{
	Vtinyint,
	Vsmallint,
	Vmediumint,
	Vint,
	Vbigint,
	//整型 非法整数数值
	Vfloat,
	Vdouble,
	Vdecimal,
	//浮点型 超长浮点数精度
	Vdate,
	Vtime,
	Vyear,
	Vdatetime,
	Vtimestamp,
	//时间 非法时间数据
	Vchar,
	Vvarchar,
	Vtinyblob,
	Vtinytext,
	Vblob,
	Vtext,
	Vmediumblob,
	Vmediumtext,
	Vlongblob,
	Vlongtext
	//文本 超长字符长度
};
const std::unordered_map<std::string, ValueType> TypeMP{
	{"tinyint", ValueType::Vtinyint},
	{"smallint", ValueType::Vsmallint},
	{"mediumint", ValueType::Vmediumint},
	{"int", ValueType::Vint},
	{"bigint", ValueType::Vbigint},

	{"float", ValueType::Vfloat},
	{"double", ValueType::Vdouble},
	{"decimal", ValueType::Vdecimal},

	{"date", ValueType::Vdate},
	{"time", ValueType::Vtime},
	{"year", ValueType::Vyear},
	{"datetime", ValueType::Vdatetime},
	{"timestamp", ValueType::Vtimestamp},

	{"char", ValueType::Vchar},
	{"varchar", ValueType::Vvarchar},
	{"tinyblob", ValueType::Vtinyblob},
	{"tinytext", ValueType::Vtinytext},
	{"blob", ValueType::Vblob},
	{"text", ValueType::Vtext},
	{"mediumblob", ValueType::Vmediumblob},
	{"mediumtext", ValueType::Vmediumtext},
	{"longblob", ValueType::Vlongblob},
	{"longtext", ValueType::Vlongtext}};
struct RowData
{
	std::vector<std::variant<int, long long, unsigned long long, std::string, double>> RowValue;
};
struct ColumnDefType
{ //数据类型
	std::vector<int> args;
	ValueType type;
	ColumnDefType(const std::string &typeStr)
	{
		std::string typeName;
		int pre = 0;
		for (size_t i = 0; i < typeStr.length(); i++)
		{
			if (typeStr[i] == '(')
				typeName = typeStr.substr(0, i), pre = i + 1;
			else if (typeStr[i] == ',' || typeStr[i] == ')')
			{
				this->args.emplace_back(std::stoi(typeStr.substr(pre, i - pre)));
				pre = i + 1;
			}
		}
		if (pre == 0)
			typeName = typeStr;
		this->type = TypeMP.at(typeName);
	}
};
bool isInteger(const std::string &s)
{
	bool sign = (s[0] == '-');
	for (int i = sign; i < s.length(); i++)
		if (!isdigit(s[i]))
			return false;
	return true;
}
bool isDecimal(const std::string &s)
{
	bool sign = (s[0] == '-');
	bool point = false;
	for (int i = sign; i < s.length(); i++)
		if (!isdigit(s[i]))
		{
			if (s[i] == '.' && point == false)
				point = true;
			else
				return false;
		}
	return true;
}
struct ColumnInfo
{ //列声明
	std::string name;
	int ordinal;
	bool isUnsigned;
	std::string charSet;
	ColumnDefType columnDef;
	size_t length;
	int precision;
	int scale;
	ColumnInfo(const rapidjson::Document &doc) : columnDef(doc["ColumnDef"].GetString())
	{
		this->name = doc["Name"].GetString();
		this->ordinal = doc["Ordinal"].GetInt();
		this->isUnsigned = doc["Unsigned"].GetBool();
		this->charSet = (doc["CharSet"].IsNull() ? "null" : doc["CharSet"].GetString());
		this->length = (doc["Length"].IsNull() ? 0 : doc["Length"].GetInt());
		this->precision = (doc["Precision"].IsNull() ? -1 : doc["Precision"].GetInt());
		this->scale = (doc["Scale"].IsNull() ? -1 : doc["Scale"].GetInt());
	}
	std::variant<int, long long, unsigned long long, std::string, double> readCol(std::string &data)
	{
		if (this->columnDef.type == ValueType::Vtinyint)
		{
			/*
			int
			Signed [-128,127]
			Unsigned [0,255]
			*/
			if (!isInteger(data))
				return 0;
			if (!this->isUnsigned)
			{
				int x = std::stoi(data);
				if (x > 127 || x < -128)
					return 0;
				return x;
			}
			else
			{
				if (data[0] == '-')
					return 0;
				int x = std::stoi(data);
				if (x > 255)
					return 0;
				return x;
			}
		}
		if (this->columnDef.type == ValueType::Vsmallint)
		{
			/*
			int
			Signed [-32768,32767]
			Unsigned [0,65535]
			*/
			if (!isInteger(data))
				return 0;
			if (!this->isUnsigned)
			{
				int x = std::stoi(data);
				if (x > 32767 || x < -32768)
					return 0;
				return x;
			}
			else
			{
				int x = std::stoi(data);
				if (data[0] == '-')
					return 0;
				if (x > 65535)
					return 0;
				return x;
			}
		}
		/*
		if (this->columnDef.type == ValueType::Vmediumint)
		{
			
			not appeared
			int
			Signed [-8388608,8388607]
			Unsigned [0,16777215]
			
			if (!isInteger(data))
				return 0;
			try
			{
				int x = std::stoi(data);
				if (!this->isUnsigned)
				{
					if (x < -8388608 || x > 8388607)
						return 0;
					return x;
				}
				else
				{
					if (x < 0 || x > 16777215)
						return 0;
					return x;
				}
			}
			catch (const std::exception &e)
			{
			}
			return 0;
		}
		*/
		if (this->columnDef.type == ValueType::Vint)
		{
			/*
			long long
			Signed [-2147483648,2147483647]
			Unsigned [0,4294967295]
			*/
			if (!isInteger(data))
				return 0;

			if (!this->isUnsigned)
			{
				long long x = std::stoll(data);
				if (x > 2147483647 || x < -2147483648)
					return 0;
				return x;
			}
			else
			{
				if (data[0] == '-')
					return 0;
				long long x = std::stoll(data);
				if (x > 4294967295)
					return 0;
				return x;
			}
		}
		if (this->columnDef.type == ValueType::Vbigint)
		{
			/*
			ll/ull
			Signed [-9223372036854775808,9223372036854775807]
			Unsigned [0,18446744073709551615]
			*/
			if (!isInteger(data))
				return 0;
			try
			{
				if (!this->isUnsigned)
				{
					long long x = std::stoll(data);
					return x;
				}
				else
				{
					unsigned long long x = std::stoull(data);
					return x;
				}
			}
			catch (const std::exception &)
			{
			}
			return 0;
		}
		//整型 非法整数数值
		/*
		if (this->columnDef.type == ValueType::Vfloat)
		{
			
			not appeared
			

			if (isDecimal(data))
			{
				try
				{
					float ret = std::stof(data);
					return ret;
				}
				catch (const std::exception &)
				{
				}
			}
			return 0;
		}
		if (this->columnDef.type == ValueType::Vdouble)
		{
			
			not appeared
			
			if (isDecimal(data))
			{
				try
				{
					double ret = std::stod(data);
					return ret;
				}
				catch (const std::exception &)
				{
				}
			}
			return 0;
		}
		*/
		if (this->columnDef.type == ValueType::Vdecimal)
		{
			/*
			*/
			if (isDecimal(data))
			{
				double x = std::stod(data);
				return x;
			}
			return 0;
		}
		//浮点型 超长浮点数精度
		/*
		if (this->columnDef.type == ValueType::Vdate)
		{
			
			not appeared
			 YYYY-MM-DD 
			 1000-01-01 
			
			return data;
		}
		if (this->columnDef.type == ValueType::Vtime)
		{
			
			not appeared
			 HH:MM:SS 
			-838:59:59
			
			return data;
		}
		if (this->columnDef.type == ValueType::Vyear)
		{
			
			not appeared
			 YYYY 
			 1901 
			
			return data;
		}
		*/
		if (this->columnDef.type == ValueType::Vdatetime)
		{
			/*
			 YYYY-MM-DD HH:MM:SS.xx 注意 小数点后多一个数据
			 1000-01-01 00:00:00.0
			*/

			/*std::string regex_template("\\d{4}[-]\\d{2}[-]\\d{2}[ ]\\d{2}[:]\\d{2}[:]\\d{2}[.]\\d{1,3}");
			std::regex pattern(regex_template, std::regex::icase);
			std::match_results<std::string::const_iterator> result;

			if (std::regex_match(data, result, pattern))
				return data;
            return "2020-04-01 00:00:00.0";*/
			for (auto &c : data)
			{
				if (c <= '9' && c >= '0')
					continue;
				/*
				if (c == ' ' || c == '-' || c == ':' || c == '.')
					continue;
				*/
				return "2020-04-01 00:00:00.0";
			}
			return data;
		}
		/*
		if (this->columnDef.type == ValueType::Vtimestamp)
		{
			
			not appeared
			 YYYY-MM-DD HH:MM:SS
			 19700101080001
			
			return data;
		}
		*/
		//时间 非法时间数据
		if (this->columnDef.type == ValueType::Vchar)
		{
			/*
			*/
			if (data.length() > this->length)
				data = data.substr(0, this->length);
			return data;
		}
		if (this->columnDef.type == ValueType::Vvarchar)
		{
			/*
			*/
			if (data.length() > this->length)
				data = data.substr(0, this->length);
			return data;
		}
		/*
		if (this->columnDef.type == ValueType::Vtinyblob)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		
		if (this->columnDef.type == ValueType::Vtinytext)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vblob)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		*/
		if (this->columnDef.type == ValueType::Vtext)
		{
			/*
			*/
			if (data.length() > this->length)
				data = data.substr(0, this->length);
			return data;
		}
		/*
		if (this->columnDef.type == ValueType::Vmediumblob)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vmediumtext)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vlongblob)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vlongtext)
		{
			
			not appeared
			
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		*/
		//文本 超长字符长度
		return 0;
	}
	std::variant<int, long long, unsigned long long, std::string, double> readColLow(std::string &data)
	{
		//printf("[%s]\n", data.c_str());
		if (this->columnDef.type == ValueType::Vtinyint)
		{
			/*
			int
			Signed [-128,127]
			Unsigned [0,255]
			*/
			if (!isInteger(data))
				return 0;
			if (!this->isUnsigned)
			{
				int x = std::stoi(data);
				if (x > 127 || x < -128)
					return 0;
				return x;
			}
			else
			{
				if (data[0] == '-')
					return 0;
				int x = std::stoi(data);
				if (x > 255)
					return 0;
				return x;
			}
		}
		if (this->columnDef.type == ValueType::Vsmallint)
		{
			/*
			int
			Signed [-32768,32767]
			Unsigned [0,65535]
			*/
			if (!isInteger(data))
				return 0;
			if (!this->isUnsigned)
			{
				int x = std::stoi(data);
				if (x > 32767 || x < -32768)
					return 0;
				return x;
			}
			else
			{
				int x = std::stoi(data);
				if (data[0] == '-')
					return 0;
				if (x > 65535)
					return 0;
				return x;
			}
		}
		if (this->columnDef.type == ValueType::Vint)
		{
			/*
			long long
			Signed [-2147483648,2147483647]
			Unsigned [0,4294967295]
			*/
			if (!isInteger(data))
				return 0;

			if (!this->isUnsigned)
			{
				long long x = std::stoll(data);
				if (x > 2147483647 || x < -2147483648)
					return 0;
				return x;
			}
			else
			{
				if (data[0] == '-')
					return 0;
				long long x = std::stoll(data);
				if (x > 4294967295)
					return 0;
				return x;
			}
		}
		if (this->columnDef.type == ValueType::Vbigint)
		{
			/*
			ll/ull
			Signed [-9223372036854775808,9223372036854775807]
			Unsigned [0,18446744073709551615]
			*/
			if (!isInteger(data))
				return 0;
			try
			{
				if (!this->isUnsigned)
				{
					long long x = std::stoll(data);
					return x;
				}
				else
				{
					unsigned long long x = std::stoull(data);
					return x;
				}
			}
			catch (const std::exception &)
			{
			}
			return 0;
		}
		return data;
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
	std::string indexCol;
	int index;
	PrimeKeyInfo(const rapidjson::Document &doc, const std::vector<ColumnInfo> &columns)
	{
		this->indexCol = doc["IndexCols"].GetArray()[0].GetString();
		for (size_t i = 0; i < columns.size(); i++)
			if (columns[i].name == this->indexCol)
				index = i;
	}
};
struct RowDataCmp
{
	const std::vector<PrimeKeyInfo> &primeKeys;
	RowDataCmp(const std::vector<PrimeKeyInfo> &keys) : primeKeys(keys) {}
	bool operator()(const RowData &lhs, const RowData &rhs) const
	{
		for (auto &primeKey : this->primeKeys)
		{
			if (lhs.RowValue[primeKey.index] != rhs.RowValue[primeKey.index])
				return lhs.RowValue[primeKey.index] < rhs.RowValue[primeKey.index];
		}
		return 0;
	}
};
struct RowDataEqual
{
	const std::vector<PrimeKeyInfo> &primeKeys;
	RowDataEqual(const std::vector<PrimeKeyInfo> &keys) : primeKeys(keys) {}
	bool operator()(const RowData &lhs, const RowData &rhs) const
	{
		for (auto &primeKey : this->primeKeys)
		{
			if (lhs.RowValue[primeKey.index] != rhs.RowValue[primeKey.index])
				return false;
		}
		return true;
	}
};
struct PairRowDataCmp
{
	RowDataCmp cmp;
	PairRowDataCmp(const std::vector<PrimeKeyInfo> &keys) : cmp(keys) {}
	bool operator()(const std::pair<RowData, std::shared_ptr<fastIO::IN>> &lhs, const std::pair<RowData, std::shared_ptr<fastIO::IN>> &rhs) const
	{
		return cmp(lhs.first, rhs.first);
	}
};
struct TableInfo
{ //表声明
	std::string tableName;
	std::string fromDataBase;
	std::vector<ColumnInfo> columns;
	std::vector<IndexInfo> indexs;
	std::vector<PrimeKeyInfo> primeKeys;
	std::vector<RowData> datas;
	TableInfo(std::ifstream &schemaInfo)
	{
		std::string tmp; //无用内容或空行
		schemaInfo >> tmp >> this->fromDataBase >> tmp >> this->tableName;
		int columnNums = 0;
		schemaInfo >> tmp >> tmp >> columnNums;
		std::getline(schemaInfo, tmp);
		while (columnNums--)
		{
			std::string colStr;
			std::getline(schemaInfo, colStr);
			rapidjson::Document doc;
			doc.Parse(colStr.c_str());
			columns.emplace_back(ColumnInfo(doc));
		}
		int indexNums = 0;
		schemaInfo >> tmp >> tmp >> indexNums;
		std::getline(schemaInfo, tmp);

		while (indexNums--)
		{
			std::string colStr;
			std::getline(schemaInfo, colStr);
			indexs.emplace_back(IndexInfo(colStr));
		}
		int primeKeyNums = 0;
		schemaInfo >> tmp >> tmp >> tmp >> primeKeyNums;
		std::getline(schemaInfo, tmp);
		while (primeKeyNums--)
		{
			std::string colStr;
			std::getline(schemaInfo, colStr);
			rapidjson::Document doc;
			doc.Parse(colStr.c_str());
			primeKeys.emplace_back(PrimeKeyInfo(doc, columns));
		}
	}
	void readRow(std::vector<std::string> &vecStr)
	{
		RowData rowData;
		for (size_t i = 0; i < columns.size(); i++)
			rowData.RowValue.emplace_back(columns[i].readCol(vecStr[i + 3]));
		this->datas.emplace_back(rowData);
	}
	RowData readRowLow(std::string rowStr)
	{
		std::vector<std::string> vecStr;
		splitStr(rowStr, vecStr);
		RowData rowData;
		printf("%d %d--\n", columns.size(), vecStr.size());
		//if (columns.size() != vecStr.size())
		std::cout << rowStr << std::endl;
		for (size_t i = 0; i < columns.size(); i++)
			rowData.RowValue.emplace_back(columns[i].readCol(vecStr[i]));
		return rowData;
	}
	void sortDatas()
	{
		std::sort(datas.begin(), datas.end(), RowDataCmp(primeKeys));
		datas.erase(unique(datas.begin(), datas.end(), RowDataEqual(primeKeys)), datas.end());
	}
	void sink(std::string path)
	{
		this->sortDatas();
		path += "/tianchi_dts_sink_data_" + this->tableName;
		remove(path.c_str());
		fastIO::OUT dataSink(path);
		size_t rNums = 0;
		for (auto &row : datas)
		{
			bool f = 1;
			size_t cNums = 0;
			for (auto &value : row.RowValue)
			{
				if (f)
					f = 0;
				else
					dataSink.print('	');
				if (auto pval = std::get_if<double>(&value))
					dataSink.print(*pval, this->columns[cNums].columnDef.args[1]);
				else
					std::visit([&](const auto &val)
							   { dataSink.print(val); },
							   value);
				cNums++;
			}
			rNums++;
			if (rNums != datas.size())
				dataSink.print('\n');
		}
		//std::cout << "mkdir the path file: " << path << std::endl;
	}
	void sink(RowData &row, fastIO::OUT &dataSink, bool &isFirst)
	{
		if (!isFirst)
			dataSink.print('\n');
		else
			isFirst = false;
		bool f = 1;
		size_t cNums = 0;
		for (auto &value : row.RowValue)
		{
			if (f)
				f = 0;
			else
				dataSink.print('	');
			if (auto pval = std::get_if<double>(&value))
				dataSink.print(*pval, this->columns[cNums].columnDef.args[1]);
			else
				std::visit([&](const auto &val)
						   { dataSink.print(val); },
						   value);
			cNums++;
		}
	}
	void merge(std::vector<std::string> filePaths, std::string outPath)
	{
		std::priority_queue<std::pair<RowData, std::shared_ptr<fastIO::IN>>, std::vector<std::pair<RowData, std::shared_ptr<fastIO::IN>>>,
							PairRowDataCmp>
			q{PairRowDataCmp(primeKeys)};

		for (std::string filePath : filePaths)
		{
			auto file = std::make_shared<fastIO::IN>(fastIO::IN(filePath));
			std::string rowStr = file->readLine();
			printf("[%s]-----\n", filePath.c_str());
			printf("[%s] %d\n", rowStr.c_str(), file->IOerror);
			if (rowStr == "")
				continue;
			q.push({readRowLow(rowStr), file});
		}
		fastIO::OUT outFile(outPath);
		bool isFirst = true;
		std::shared_ptr<RowData> last = nullptr;
		auto equal = RowDataEqual(primeKeys);
		int p = 0;
		while (true)
		{
			auto topRow = q.top().first;
			auto topIn = q.top().second;
			q.pop();
			if (last == nullptr || !equal(*last, topRow))
			{
				sink(topRow, outFile, isFirst);
				last = std::make_shared<RowData>(topRow);
			}
			printf("--[%d %d]\n", topIn, topIn->IOerror);
			std::string rowStr = topIn->readLine();
			printf("[%s] %d\n", rowStr.c_str(), topIn->IOerror);
			if (rowStr == "")
			{
				if (q.empty())
					break;
				continue;
			}
			topRow = readRowLow(rowStr);
			q.push({topRow, topIn});
		}
	}
	void finalSink(std::string path)
	{
		std::vector<std::string> filePaths;
		for (int i = 0; true; i++)
		{
			std::string filePath = path + "/" + std::to_string(i) + "/" + SINK_FILE_NAME_TEMPLATE + tableName;
			if (!std::ifstream(filePath).is_open())
				break;
			printf("%s\n", filePath.c_str());
			filePaths.push_back(filePath);
		}
		merge(filePaths, path + "/" + SINK_FILE_NAME_TEMPLATE + tableName);
	}
};
