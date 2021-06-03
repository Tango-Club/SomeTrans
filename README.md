# Project SomeTrans
### Todolist
- [x] shell入口 
- [ ] 数据库声明读入
  - [X] 表结构
  - [ ] 索引
  - [X] 主键
- [ ] 行数据读入整合
  - [X] 内存存储
  - [ ] fread替换文件流
  - [ ] 多线程 
- [ ] UK/PK去重排序
  - [X] 内存存储
  - [ ] 文件merge 
- [X] 非法数据清洗
- [X] 数据写入目标文件
### Notes
* 硬件
	* 选手迁移程序评测环境中存在两块PMEM内存供使用，以fsdax方式开放给选手。挂载目录为/input和/output目录，其中/input目录为只读，/output目录为可读写。待迁移数据会存储在/input目录下，整理好的数据或是临时数据可存储于/output目录下，而/output同时也是初赛中的输出数据目录。
	* 初、复赛选手可使用的CPU资源为16C，DRAM为8G，存储资源一块已挂载到/output目录的，大小为512GB。
	* 由于容器本身的架构限制，使用virtio-pmem方式使用PMEM设备。而这一方式会和真实PMEM设备有所不同：即对于pmem_map_file、pmem_is_pmem等函数，会显示其为fake pmem地址空间，但在读写性能上，却和真实PMEM设备一致。同时，相对于真实PMEM需要使用pmem\_persist保证持久化，在容器内需要使用pmem_msync进行持久化。
* 去重
    * 初赛中对于source_file_dir中不同源文件可能存在相同主键的值，这里保证主键相同的话数据也是相同的，即主键相同视为重复数据，选手对结果需要进行去重。
* 排序
	* 部分表会出现复合主键的情况，排序的次序与schema.info里主键列出现的顺序相同，并且按照ASC出现。
* 非法数据
    * 非法整数数值。如定义为int的列值出现了非法字符，我们统一将其处理为"0"值；比如表item表，i_price字段类型为decimal(5,2)，出现了SQ字符串，则认为非法列值；
    * 超长浮点数精度。如定义为decimal(3, 2)的列值中出现了小数点后3位，我们对其进行4舍5入；比如order_line表，ol_amount字段出现值5.346，则需要清洗为5.35；
    * 非法时间数据。如定义为datetime的列值中出现了非法的日期，我们将其统一成"2020-04-01 00:00:00.0"；非法时间数据一般包括大小写字母，比如：2021-04-1a 15:41:55.0;
    * 超长字符长度。如定义为varchar(16)的列值出现了17个字符，此时我们按照此列的最大长度对列值截断（注意不考虑"\0"因素)。比如warehouse表w_state字段类型为char(2)，出现了"aaaaaaa"长度值，则需要截断处理"aa";
* 调试
    * 为了便于选手调试程序，我们开放了和评测环境一致的Docker Image供选手测试，使用如下命令运行镜像：
      * docker pull rv2001/tianchi-contest:public1.0
      * docker run -it rv2001/tianchi-contest:public1.0 /bin/bash
    * 数据采用TPC-C标准生成 https://github.com/Percona-Lab/tpcc-mysql