/*
表定义类
*/
struct ColumnDefType
{ //数据类型
	std::string typeName;
	std::vector<int> args;
	ColumnDefType(const std::string &typeStr)
	{
		int pre = 0;
		for (int i = 0; i < typeStr.length(); i++)
		{
			if (typeStr[i] == '(')
				this->typeName = typeStr.substr(0, i), pre = i + 1;
			else if (typeStr[i] == ',' || typeStr[i] == ')')
			{
				this->args.push_back(std::stoi(typeStr.substr(pre, i - pre)));
				pre = i + 1;
			}
		}
		std::cout << typeName << std::endl;
		for (auto x : args)
			std::cout << x << " ";
		std::cout << std::endl;
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
};
