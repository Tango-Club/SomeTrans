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

    struct OUT
    {
        char s[15], *s1;
        char buf[BUF_SIZE + 5];
        char *p1, *pend;
        FILE *fp;
        std::string path;
        int fd;
        unsigned long long write_pos;
        OUT(std::string path)
        {
            p1 = buf;
            pend = buf + BUF_SIZE;
            this->path=path;
            fd = open("./1", O_RDWR | O_CREAT | O_TRUNC,S_IRWXU);
            write_pos=0;
        }
        void out(char ch)
        {
            if (p1 >= pend)
            {
                char * start = (char *) mmap(nullptr, BUF_SIZE, PROT_WRITE, MAP_SHARED, fd, (long)write_pos);
                if (start == MAP_FAILED) /* 判断是否映射成功 */
                {
                    printf("error : [%s]\n", path.c_str());
                    return ;
                }
                write(fd, buf, BUF_SIZE);
                munmap(start, BUF_SIZE);
                p1 = buf;
                write_pos+=BUF_SIZE;
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
               /* fwrite(buf, 1, p1 - buf, fp);
                p1 = buf;*/
                char * start = (char *) mmap(nullptr, BUF_SIZE, PROT_WRITE, MAP_SHARED, fd, (long)write_pos);
                if (start == MAP_FAILED) /* 判断是否映射成功 */
                {
                    printf("error : [%s]\n", path.c_str());
                    return ;
                }
                write(fd, buf,  p1 - buf);
                munmap(start, BUF_SIZE);
            }
        }
        inline void print(const std::string &s)
        {
            for (auto &c : s)
                print(c);
        }
        ~OUT()
        {
            flush();
            close(fd);
        }
        OUT(const OUT &) = delete;
        OUT &operator=(const OUT &) = delete;
    };
};