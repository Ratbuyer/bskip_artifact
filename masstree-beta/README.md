# Masstree

## Compilation

First, modify the data file path in `ycsb.cc`.

To compile, run `make`.

## Execution

execution command: `./mttest --test=rw1 --workload=<ycsb workload 0=A/1=B/2=C/4=E> --method=0 -j<thread number>`.

Example command: `./mttest --test=rw1 --workload=0 --method=0 -j128`.

This will print the througput on the terminal. To get latency, run `python3 script.py`.
