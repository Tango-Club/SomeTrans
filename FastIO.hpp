namespace fastIO
{
	using ll = long long;
	using ull = unsigned long long;
	constexpr int BUF_SIZE = 1000000;
	class IN
	{
	public:
		char buf[BUF_SIZE + 5];
		char *p1;
		char *pend;
		bool open_success = false;
		bool IOerror = false;
		FILE *fp;
		IN(std::string path)
		{
			fp = fopen(path.c_str(), "r");
			p1 = buf + BUF_SIZE;
			pend = buf + BUF_SIZE;
			open_success = (fp == NULL ? false : true);
			if (!open_success)
				printf("[%s]\n", path.c_str());
			assert(open_success);
			IOerror = false;
		}
		IN(const IN &) = delete;
		IN &operator=(const IN &) = delete;
		~IN()
		{
			if (open_success)
				fclose(fp);
		}
		//fread->read
		inline char nc()
		{
			if (p1 == pend)
			{
				p1 = buf;
				pend = buf + fread(buf, 1, BUF_SIZE, fp);
				if (p1 == pend)
				{
					IOerror = true;
					return -1;
				}
			}
			return *p1++;
		}
		inline bool isEnd(char ch) { return ch == '\n' || ch == '\r'; }
		inline std::string readLine()
		{
			std::string s;
			char ch = nc();
			if (IOerror)
				return s;
			for (; !isEnd(ch) && !IOerror; ch = nc())
				s.push_back(ch);
			return s;
		}
	};
	//fwrite->write

	struct Ostream_fwrite
	{
		char s[15], *s1;
		char buf[BUF_SIZE + 5];
		char *p1, *pend;
		FILE *fp;
		Ostream_fwrite()
		{
			p1 = buf;
			pend = buf + BUF_SIZE;
		}
		void out(char ch)
		{
			if (p1 >= pend)
			{
				fwrite(buf, 1, BUF_SIZE, fp);
				p1 = buf;
			}
			*p1++ = ch;
		}
		void print(int x)
		{
			s1 = s;
			if (!x)
				*s1++ = '0';
			if (x < 0)
				out('-'), x = -x;
			while (x)
				*s1++ = x % 10 + '0', x /= 10;
			while (s1-- != s)
				out(*s1);
		}
		void print(unsigned int x)
		{
			s1 = s;
			if (!x)
				*s1++ = '0';
			while (x)
				*s1++ = x % 10 + '0', x /= 10;
			while (s1-- != s)
				out(*s1);
		}
		void print(ll x)
		{
			s1 = s;
			if (!x)
				*s1++ = '0';
			if (x < 0)
				out('-'), x = -x;
			while (x)
				*s1++ = x % 10 + '0', x /= 10;
			while (s1-- != s)
				out(*s1);
		}
		void print(ull x)
		{
			s1 = s;
			if (!x)
				*s1++ = '0';
			while (x)
				*s1++ = x % 10 + '0', x /= 10;
			while (s1-- != s)
				out(*s1);
		}
		void print(double x, size_t y)
		{
			if (x < -1e-12)
				out('-'), x = -x;
			x += 1e-8;
			x *= mul[y];
			ll x1 = (ll)floor(x);
			if (x - floor(x) >= 0.5)
				++x1;
			ll x2 = x1 / mul[y], x3 = x1 - x2 * mul[y];
			print(x2);
			if (y > 0)
			{
				out('.');
				for (size_t i = 1; i < y && x3 * mul[i] < mul[y]; out('0'), ++i)
					;
				print(x3);
			}
		}
		void print(char *s)
		{
			while (*s)
				out(*s++);
		}
		void flush()
		{
			if (p1 != buf)
			{
				fwrite(buf, 1, p1 - buf, fp);
				p1 = buf;
			}
		}
		~Ostream_fwrite()
		{
			flush();
		}
		Ostream_fwrite(const Ostream_fwrite &) = delete;
		Ostream_fwrite &operator=(const Ostream_fwrite &) = delete;
	};
	class OUT
	{
	public:
		bool open_success = false;
		Ostream_fwrite Ostream;
		FILE *fp;
		OUT(std::string path)
		{
			fp = fopen(path.c_str(), "w");
			Ostream.fp = fp;
			open_success = (fp == NULL ? false : true);
			assert(open_success);
		}
		OUT(const OUT &) = delete;
		OUT &operator=(const OUT &) = delete;
		~OUT()
		{
			Ostream.flush();
			if (open_success)
				fclose(fp);
		}
		inline void print(int x) { Ostream.print(x); }
		inline void print(char x) { Ostream.out(x); }
		inline void print(ll x) { Ostream.print(x); }
		inline void print(ull x) { Ostream.print(x); }

		inline void print(double x, int y) { Ostream.print(x, y); }
		inline void print(double x) { Ostream.print(x, 0); }

		inline void print(char *s) { Ostream.print(s); }
		inline void print(const std::string &s)
		{
			for (auto &c : s)
				print(c);
		}
	};
};
