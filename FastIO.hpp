namespace fastIO
{
	const int BUF_SIZE = 20000000;
	const int OUT_SIZE = 20000000;
	using ll = long long;
	using ull = unsigned long long;
	bool open_success = false;
	class IN
	{
	public:
		FILE *fp;
		IN(std::string path)
		{
			fp = fopen(path.c_str(), "r");
			open_success = (fp == NULL ? false : true);
		}
		~IN()
		{
			if (open_success)
				fclose(fp);
		}
		//fread->read
		bool IOerror = 0;
		inline char nc()
		{
			static char buf[BUF_SIZE + 5], *p1 = buf + BUF_SIZE, *pend = buf + BUF_SIZE;
			if (p1 == pend)
			{
				p1 = buf;
				pend = buf + fread(buf, 1, BUF_SIZE, fp);
				if (pend == p1)
				{
					IOerror = 1;
					return -1;
				}
			}
			return *p1++;
		}
		inline bool isEnd(char ch) { return ch == '\r' || ch == '\n'; }
		inline void readLine(std::string &s)
		{
			s.clear();
			char ch = nc();
			if (IOerror)
				return;
			for (; !isEnd(ch) && !IOerror; ch = nc())
				s.push_back(ch);
		}
	};
	//fwrite->write

	struct Ostream_fwrite
	{
		char *buf, *p1, *pend;
		FILE *fp;
		Ostream_fwrite()
		{
			buf = new char[BUF_SIZE];
			p1 = buf;
			pend = buf + BUF_SIZE;
		}
		void out(char ch)
		{
			if (p1 == pend)
			{
				fwrite(buf, 1, BUF_SIZE, fp);
				p1 = buf;
			}
			*p1++ = ch;
		}
		void print(int x)
		{
			static char s[15], *s1;
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
			static char s[15], *s1;
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
			static char s[25], *s1;
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
			static char s[25], *s1;
			s1 = s;
			if (!x)
				*s1++ = '0';
			while (x)
				*s1++ = x % 10 + '0', x /= 10;
			while (s1-- != s)
				out(*s1);
		}
		void print(double x, int y)
		{
			static ll mul[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000,
							   1000000000, 10000000000LL, 100000000000LL, 1000000000000LL, 10000000000000LL,
							   100000000000000LL, 1000000000000000LL, 10000000000000000LL, 100000000000000000LL};
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
		~Ostream_fwrite() { flush(); }
	};
	class OUT
	{
	public:
		Ostream_fwrite Ostream;
		FILE *fp;
		OUT(std::string path)
		{
			fp = fopen(path.c_str(), "w");
			Ostream.fp = fp;
			open_success = (fp == NULL ? false : true);
		}
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
		inline void print(double x) { Ostream.print(x, 2); }

		inline void print(char *s) { Ostream.print(s); }
		inline void print(const std::string& s)
		{
			for (auto &c : s)
				print(c);
		}
	};
};
