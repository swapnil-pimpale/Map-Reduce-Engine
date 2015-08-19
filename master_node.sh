#!/bin/sh

killall -9 java
rm -rf /tmp/sr0/*
mkdir -p /tmp/sr0
java -jar framework.jar master &
