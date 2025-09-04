# Folly

This directory contains codes that runs the `ConcurrentSkipList.h` class in Facebook's Folly library.

## Preparations

First, clone the Facebook Folly library: `https://github.com/facebook/folly/tree/main/folly`.

Folly the compilation instructions and compile Folly.

Note this process is complicated. If you have trouble downloading FastFloat, try clone and compile it manually `git clone https://github.com/fastfloat/fast_float.git`.

After completing this, there should be a build dir in your Folly folder.

## Compilation

Compilation command: `g++-11 -I <path to your folly dir> -I <path to glog dir> -I <path to folly's build dir> -I <path to fmt's include dir>  ycsb.cpp -o ycsb -lfolly -lpthread -lglog -ldl -lfmt -O3 -march=native -DLATENCY=1`.

Example command: `g++-11 -I /home/brian/folly/ -I /home/brian/glog/src -I /home/brian/folly/_build/ -I /home/brian/fmt/include/   ycsb.cpp -o ycsb -lfolly -lpthread -lglog -ldl -lfmt -O3 -march=native -DLATENCY=1`.

## Execution

Execution command: `./ycsb <path to ycsb data dir> <workload> <thread number> <output file>`.
