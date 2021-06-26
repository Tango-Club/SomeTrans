#!/bin/bash
#
# Author: dts
# Date: 2021/05/13
#
echo "init"
echo "echo0: $0"
echo "echo1: $1"
echo "echo2: $2"
echo "echo3: $3"
echo "echo4: $4"
APP_HOME=main.cpp
OUT_NAME=main
#g++-11 $APP_HOME -std=c++20 -o $OUT_NAME  -O2 -pthread
echo "nohup ./$OUT_NAME $* & "
echo "start $0 $1 $2 $3 $4 $@"

#screen -ls
./$OUT_NAME $@

#
#for((i=1; i<=10000; i+=1))
#do
#runs=`ps -aux | grep ./main | wc -l`
#printf "run: ${runs}.\n"
#date
#echo 'USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND'
#ps -aux | grep  './main --input_dir /input --output_dir'
#echo '----------------------------------------------------------------------------'
#free
#if [ ${runs} -lt 2 ]; then
#    printf "stop\n"
#    break;  q
#else
#    sleep 10
#fi
#done
#
#echo "end"
#
#
#
