# bskiplist

getting the code: clone it and get submodules

git clone git@github.com:itshelenxu/bskiplist.git
cd bskiplist
git submodule init
git submodule update

---
building the code: using g++-11
export CXX=g++-11

to build in parallel
make basic

to run with lock timing
make ENABLE_TRACE_TIMER=1 CYCLE_ITMER=1 basic

to build with debug
make DEBUG=1 basic

---
to run microbenchmarks

./basic --microbenchmark_baseline --write_csv

see also run_tests.py
