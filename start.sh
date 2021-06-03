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
g++ $APP_HOME -std=c++17 -o $OUT_NAME  -O2
echo "start $0 $1 $2 $3 $4"
./$OUT_NAME $@
echo "end"
