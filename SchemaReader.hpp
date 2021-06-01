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
	std::vector<std::variant<int, long long, std::string>> RowValue;
};
struct ColumnDefType
{ //数据类型
	std::vector<int> args;
	ValueType type;
	ColumnDefType(const std::string &typeStr)
	{
		std::string typeName;
		int pre = 0;
		for (int i = 0; i < typeStr.length(); i++)
		{
			if (typeStr[i] == '(')
				typeName = typeStr.substr(0, i), pre = i + 1;
			else if (typeStr[i] == ',' || typeStr[i] == ')')
			{
				this->args.push_back(std::stoi(typeStr.substr(pre, i - pre)));
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
	std::variant<int, long long, std::string> readCol(std::ifstream &dataSource)
	{
		if (this->columnDef.type == ValueType::Vtinyint)
		{
			/*
			int
			Signed [-128,127]
			Unsigned [0,255]
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vsmallint)
		{
			/*
			int
			Signed [-32768,32767]
			Unsigned [0,65535]
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vmediumint)
		{
			/*
			not appeared
			int
			Signed [-8388608,8388607]
			Unsigned [0,16777215]
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vint)
		{
			/*
			int
			Signed [-2147483648,2147483647]
			Unsigned [0,4294967295]
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vbigint)
		{
			/*
			ll/ull
			Signed [-9223372036854775808,9223372036854775807]
			Unsigned [0,18446744073709551615]
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		//整型 非法整数数值
		if (this->columnDef.type == ValueType::Vfloat)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vdouble)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vdecimal)
		{
			/*
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		//浮点型 超长浮点数精度
		if (this->columnDef.type == ValueType::Vdate)
		{
			/*
			not appeared
			 YYYY-MM-DD 
			 1000-01-01 
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vtime)
		{
			/*
			not appeared
			 HH:MM:SS 
			-838:59:59
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vyear)
		{
			/*
			not appeared
			 YYYY 
			 1901 
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vdatetime)
		{
			/*
			 YYYY-MM-DD HH:MM:SS.xx 注意 小数点后多一个数据
			 1000-01-01 00:00:00.0
			 21位
			*/
			std::string data;
			char c = ' ';
			while (c == 9 || c == ' ')
			{
				dataSource.get(c);
			}
			data += c;
			for (int i = 1; i <= 3; i++)
			{
				dataSource.get(c);
				data += c;
			}
			if (data != "null")
			{
				for (int i = 1; i <= 17; i++)
				{
					dataSource.get(c);
					data += c;
				}
			}
			std::cout << data << std::endl;
			return data;
		}
		if (this->columnDef.type == ValueType::Vtimestamp)
		{
			/*
			not appeared
			 YYYY-MM-DD HH:MM:SS
			 19700101080001
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		//时间 非法时间数据
		if (this->columnDef.type == ValueType::Vchar)
		{
			/*
			*/
			std::string data;
			dataSource >> data;
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vvarchar)
		{
			/*
			*/
			std::string data;
			dataSource >> data;
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vtinyblob)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vtinytext)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vblob)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vtext)
		{
			/*
			*/
			std::string data;
			dataSource >> data;
			while (data.length() > this->length)
				data.pop_back();
			return data;
		}
		if (this->columnDef.type == ValueType::Vmediumblob)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vmediumtext)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vlongblob)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		if (this->columnDef.type == ValueType::Vlongtext)
		{
			/*
			not appeared
			*/
			std::string data;
			dataSource >> data;
			return data;
		}
		//文本 超长字符长度
		assert(0);
		return 0;
	}
};
struct IndexInfo
{ //索引声明
	//临时
	std::string str;
	IndexInfo(const std::string &indexStr)
	{
		this->str = indexStr;
		std::cout << indexStr << std::endl;
	}
};
struct PrimeKeyInfo
{ //主键声明
	//临时
	std::string indexCol;
	PrimeKeyInfo(const rapidjson::Document &doc)
	{
		this->indexCol = doc["IndexCols"].GetArray()[0].GetString();
		std::cout << this->indexCol << std::endl;
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
			std::cout << colStr << std::endl;
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
			primeKeys.emplace_back(PrimeKeyInfo(doc));
		}
	}
	void readRow(std::ifstream &dataSource)
	{
		RowData rowData;
		for (auto col : columns)
		{
			rowData.RowValue.push_back(col.readCol(dataSource));
		}
		this->datas.push_back(rowData);
	}
	void sink(std::string path)
	{
		if (access(path.c_str(), 0) == -1)
			mkdir(path.c_str());
		path += "/tianchi_dts_sink_data_" + this->tableName;
		remove(path.c_str());
		std::ofstream dataSink(path);
		for (auto row : datas)
		{
			bool f = 1;
			for (auto value : row.RowValue)
			{
				if (f)
					f = 0;
				else
					dataSink << " ";
				if (auto pval = std::get_if<int>(&value))
					dataSink << *pval;
				else if (auto pval = std::get_if<std::string>(&value))
					dataSink << *pval;
				else
					assert(0);
			}
			dataSink << std::endl;
		}
	}
};
