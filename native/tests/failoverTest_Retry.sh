#!/bin/sh
usage="Usage: unitTest_3.sh timeduration scriptPWD locator-ip-address locator-port"

timeduration="$1"
locatorIpAddress="$3"
locatorPort="$4"
scriptDir="$2"

timeduration=${timeduration#"["}
timeduration=${timeduration%"]"}
scriptDir=${scriptDir#"["}
scriptDir=${scriptDir%"]"}

_snappyNativeDir="$(dirname "$scriptDir")"
distDir=${_snappyNativeDir}/dist

THRIFT_VERSION=1.0.0-2
BOOST_VERSION=1.65.1

thrftLibPath=${distDir}/thrift-${THRIFT_VERSION}/lin64/lib
bostLibPath=${distDir}/boost-${BOOST_VERSION}/lin64/lib
snappClientLibPath=${_snappyNativeDir}/build-artifacts/lin/snappyclient/lin64/lib/debug

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${thrftLibPath}:${bostLibPath}:${snappClientLibPath}:${_snappyNativeDir}/Debug#!/bin/sh

export LD_LIBRARY_PATH

headerLoc=${_snappyNativeDir}/src/snappyclient/headers
boostHeadeLoc=${_snappyNativeDir}/dist/boost-${BOOST_VERSION}/include
cd ${scriptDir}
g++ -std=c++11 -I"${headerLoc}" -I"${boostHeadeLoc}" -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MFunitTest_3.d -MTunitTest_3.o -o unitTest_3.o unitTest_3.cpp

g++ -L${bostLibPath} -L${thrftLibPath} -L${snappClientLibPath} -L${_snappyNativeDir}/Debug -rdynamic -o unitTest_3  unitTest_3.o -lcrypto -lodbc -lpthread -lssl -lsnappydata-native -lboost_chrono -lboost_date_time -lboost_filesystem -lboost_log -lboost_log_setup -lboost_system -lboost_thread -lthrift -lpthread -lrt -lgmp

chmod 777 unitTest_3 unitTest_3.d

./unitTest_3 $locatorIpAddress $locatorPort $timeduration

rm unitTest_3 unitTest_3.d unitTest_3.o
