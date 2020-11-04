#!/bin/bash

set -e

JAVA_HOME=/home/wsargent/.jabba/jdk/graalvm-ce-java11@20.1.0
PATH=$JAVA_HOME/bin:$PATH

echo "> building jar"

./gradlew clean shadow
mkdir -p build/graal

echo "> compiling binary"
native-image --no-server \
  -H:Path=./build/graal \
  -H:Name=blacklite-reader \
  -cp ./build/libs/blacklite-reader-all.jar
