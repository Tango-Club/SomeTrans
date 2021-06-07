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