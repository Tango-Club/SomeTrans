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
	std::vector<std::any> rowValue;
	RowData(int x) : rowValue(x) {}
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
	std::any readCol(const std::string &data)
	{
		if (this->columnDef.type == ValueType::Vtinyint)
		{
			if (!isInteger(data))
				return 0;
			return std::stoi(data);
		}
		else if (this->columnDef.type == ValueType::Vsmallint)
		{
			if (!isInteger(data))
				return 0;
			return std::stoi(data);
		}
		else if (this->columnDef.type == ValueType::Vint)
		{
			if (!isInteger(data))
				return 0;
			return std::stoi(data);
		}
		else if (this->columnDef.type == ValueType::Vbigint)
		{
			if (!isInteger(data))
				return 0;
			return std::stoll(data);
		}
		else if (this->columnDef.type == ValueType::Vdecimal)
		{
			if (isDecimal(data))
			{
				double x = std::stod(data);
				return dtos(x, this->columnDef.args[1]);
			}
			return 0;
		}
		else if (this->columnDef.type == ValueType::Vdatetime)
		{
			for (auto &c : data)
			{
				if (c <= '9' && c >= '0')
					continue;
				if (c == ' ' || c == '-' || c == ':' || c == '.')
					continue;
				return std::string("2020-04-01 00:00:00.0");
			}
			return data;
		}
		else if (this->columnDef.type == ValueType::Vchar)
		{
			if (data.length() > this->length)
				return data.substr(0, this->length);
			return data;
		}
		else if (this->columnDef.type == ValueType::Vvarchar)
		{
			if (data.length() > this->length)
				return data.substr(0, this->length);
			return data;
		}
		else if (this->columnDef.type == ValueType::Vtext)
		{
			if (data.length() > this->length)
				return data.substr(0, this->length);
			return data;
		}
		return 0;
	}
	std::any readColLow(const std::string &data)
	{
		if (this->columnDef.type == ValueType::Vtinyint)
			return std::stoi(data);
		else if (this->columnDef.type == ValueType::Vsmallint)
			return std::stoi(data);
		else if (this->columnDef.type == ValueType::Vint)
			return std::stoi(data);
		return data;
	}
};
struct IndexInfo
{ //索引声明
	//临时
	std::string str;
	IndexInfo(const std::string &indexStr) : str(indexStr) {}
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
			if (std::any_cast<int>(lhs.rowValue[primeKey.index]) != std::any_cast<int>(rhs.rowValue[primeKey.index]))
				return std::any_cast<int>(lhs.rowValue[primeKey.index]) < std::any_cast<int>(rhs.rowValue[primeKey.index]);
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
			if (std::any_cast<int>(lhs.rowValue[primeKey.index]) != std::any_cast<int>(rhs.rowValue[primeKey.index]))
				return false;
		}
		return true;
	}
};
struct PairRowDataCmp
{
	const std::vector<PrimeKeyInfo> &primeKeys;
	PairRowDataCmp(const std::vector<PrimeKeyInfo> &keys) : primeKeys(keys) {}
	bool operator()(const std::pair<std::shared_ptr<RowData>, std::shared_ptr<fastIO::IN>> &lhs, const std::pair<std::shared_ptr<RowData>, std::shared_ptr<fastIO::IN>> &rhs) const
	{
		for (auto &primeKey : this->primeKeys)
		{
			if (std::any_cast<int>(lhs.first->rowValue[primeKey.index]) != std::any_cast<int>(rhs.first->rowValue[primeKey.index]))
				return std::any_cast<int>(lhs.first->rowValue[primeKey.index]) > std::any_cast<int>(rhs.first->rowValue[primeKey.index]);
		}
		return 0;
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
		RowData rowData(vecStr.size() - 3);

		for (size_t i = 0; i < columns.size(); i++)
			rowData.rowValue[i] = columns[i].readCol(vecStr[i + 3]);
		this->datas.emplace_back(rowData);
	}
	RowData readRowLow(const std::string &rowStr)
	{
		std::vector<std::string> vecStr;
		splitStr(rowStr, vecStr);
		RowData rowData(vecStr.size());
		for (size_t i = 0; i < columns.size(); i++)
			rowData.rowValue[i] = columns[i].readColLow(vecStr[i]);
		return rowData;
	}
	void sortDatas()
	{
		std::sort(datas.begin(), datas.end(), RowDataCmp(primeKeys));
		//datas.erase(unique(datas.begin(), datas.end(), RowDataEqual(primeKeys)), datas.end());
	}
	void sink(const RowData &row, fastIO::OUT &dataSink, bool &isFirst)
	{
		if (!isFirst)
			dataSink.print('\n');
		else
			isFirst = false;
		bool f = 1;
		size_t cNums = 0;
		for (auto &value : row.rowValue)
		{
			if (f)
				f = 0;
			else
				dataSink.print('	');
			if (value.type() == typeid(std::string))
				dataSink.print(std::any_cast<std::string>(value));
			else if (value.type() == typeid(int))
				dataSink.print(std::any_cast<int>(value));
			else if (value.type() == typeid(long long))
				dataSink.print(std::any_cast<long long>(value));
			cNums++;
		}
	}
	void sink(std::string path)
	{
		this->sortDatas();
		path += "/tianchi_dts_sink_data_" + this->tableName;
		remove(path.c_str());
		fastIO::OUT dataSink(path);
		std::shared_ptr<RowData> last = nullptr;
		auto equal = RowDataEqual(primeKeys);
		bool isFirst = true;
		for (auto &row : datas)
		{
			if (last == nullptr || !equal(*last, row))
			{
				sink(row, dataSink, isFirst);
				last = std::make_shared<RowData>(row);
			}
		}
		datas.clear();
		datas.shrink_to_fit();
	}
	void merge(std::vector<std::string> filePaths, std::string outPath)
	{
		/*
		__gnu_pbds::priority_queue<std::pair<std::shared_ptr<RowData>, std::shared_ptr<fastIO::IN>>,
								   PairRowDataCmp, __gnu_pbds::rc_binomial_heap_tag>
			q{PairRowDataCmp(primeKeys)};
		*/
		std::priority_queue<std::pair<std::shared_ptr<RowData>, std::shared_ptr<fastIO::IN>>,
							std::vector<std::pair<std::shared_ptr<RowData>, std::shared_ptr<fastIO::IN>>>,
							PairRowDataCmp>
			q{PairRowDataCmp(primeKeys)};

		for (std::string filePath : filePaths)
		{
			auto file = std::make_shared<fastIO::IN>(filePath);
			std::string rowStr(file->readLine());
			if (rowStr == "")
				continue;
			q.push({std::make_shared<RowData>(readRowLow(rowStr)), file});
		}
		fastIO::OUT outFile(outPath);
		bool isFirst = true;
		std::shared_ptr<RowData> last = nullptr;
		auto equal = RowDataEqual(primeKeys);
		while (!q.empty())
		{
			auto topRow = q.top().first;
			auto topIn = q.top().second;
			q.pop();
			if (last == nullptr || !equal(*last, *topRow))
			{
				sink(*topRow, outFile, isFirst);
				last = topRow;
			}
			std::string rowStr(topIn->readLine());
			if (rowStr == "")
			{
				if (q.empty())
					break;
				continue;
			}
			topRow = std::make_shared<RowData>(readRowLow(rowStr));
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
			filePaths.push_back(filePath);
		}
		merge(filePaths, path + "/" + SINK_FILE_NAME_TEMPLATE + tableName);
	}
};
