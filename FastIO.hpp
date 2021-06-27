namespace fastIO
{
    using ll = long long;
    using ull = unsigned long long;
    constexpr int BUF_SIZE = 4096*32;
    class IN
    {
    public:
        char buf[BUF_SIZE + 5];
        char *p1;
        char *pend;
        bool open_success = false;
        bool IOerror = false;
        int fd;
        char *start;
        struct stat sb;
        unsigned long long read_size=BUF_SIZE;
        unsigned long long read_pos=0;
        std::string path;
        IN(std::string path)
        {
          //  puts("start 1");
            this->path=path;
            fd = open(path.c_str(), O_RDONLY);
            fstat(fd, &sb);
            p1 = buf + BUF_SIZE;
            pend = buf + BUF_SIZE;
            IOerror = false;
       //     puts("start 2");
        }
        IN(const IN &) = delete;
        IN &operator=(const IN &) = delete;
        ~IN()
        {
            close(fd);
        }
        //fread->read
        inline char nc()
        {
          //  puts("start3 ");
            if (p1 == pend)
            {
            //    puts("start    4 ");
                if(read_pos>=sb.st_size){
                    IOerror = true;
                    return -1;
                }
                char  *mmap_get=(char *)mmap(nullptr, read_size, PROT_READ, MAP_PRIVATE, fd, read_pos);
                if(mmap_get == MAP_FAILED){
                    printf("error : [%s]\n", path.c_str());
                    open_success=false;
                    IOerror = true;
                    return -1;
                }
                unsigned long long max_read=std::min(read_size,sb.st_size-read_pos);

         //       printf("ok: %llu %llu\n",sb.st_size, read_pos);
                memcpy(buf,(char *)mmap_get,max_read);
                munmap(mmap_get, read_size);
                read_pos += max_read;
                p1 = buf;
                pend = buf + max_read;
            //    puts(std::to_string(max_read).c_str());
                if (p1 == pend)
                {
                    IOerror = true;
                    return -1;
                }
            }
       //     system("free > ./debug.txt 2&>1");
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
        int fd;
        Ostream_fwrite()
        {

            p1 = buf;
            pend = buf + BUF_SIZE;
        }
        void out(char ch)
        {
           // std::cerr<<ch;
            if (p1 >= pend)
            {
                //fwrite(buf, 1, BUF_SIZE, fp);
                write(fd,buf,BUF_SIZE);
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
                //fwrite(buf, 1, p1 - buf, fp);
                //write()
                write(fd,buf,p1 - buf);
                p1 = buf;
            }
        }
        ~Ostream_fwrite()
        {
            flush();
            close(fd);
        }
        Ostream_fwrite(const Ostream_fwrite &) = delete;
        Ostream_fwrite &operator=(const Ostream_fwrite &) = delete;
    };
    class OUT
    {
    public:
        Ostream_fwrite Ostream;
        OUT(std::string path)
        {
            //fp = fopen(path.c_str(), "w");
            Ostream.fd = open("./1", O_RDWR | O_CREAT ,0644);
        }
        OUT(const OUT &) = delete;
        OUT &operator=(const OUT &) = delete;
        ~OUT()
        {
            Ostream.flush();
            //fclose(fp);

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