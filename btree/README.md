# B-Tree, forked from https://github.com/wheatman/BP-Tree

## Compilation:

To compile the code for throughput, run `make`. To compile the code for both throughput and latency, run `make LATENCY=1`

## Execution

After compiling, run `./ycsb <path to ycsb files> <workload> <thread number> <output file>`

Example command: `./ycsb /home/eddy/repo/ycsb/ a 16 out.txt`

The througput or/and latency will be printed in the terminal
