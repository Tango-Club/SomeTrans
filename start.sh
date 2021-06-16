#!/bin/sh
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
echo "start $0 $1 $2 $3 $4 $@"
nohup ./$OUT_NAME $@ & > ./lala.out

for((i=0;i<100;i++))
do
sleep 1
echo 'USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND'
ps -aux | grep  ./main
free
done

cat ./lala.out
echo "end"