namespace parallelRead
{
    class RowProducter
    {
        fastIO::IN sourceData;
        std::queue<std::string>&rowQueue;
        RowProducter (std::string path,std::queue<std::string>&q):sourceData(path),rowQueue(q){}
        void product()
        {

        }
    }
}