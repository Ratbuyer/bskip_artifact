# java.util.concurrent.ConcurrentSkipListMap

## Compilation

To compile, run `./build.sh`, this will produce two binaries, one for througput and one for latency.

## Execution

For througput, run `java YCSB_CSLM <YCSB load file> <YCSB run file> <thread number> <output file>`.

Example command: `numactl -i all java YCSB_CSLM ../datasets/uniform/loade_unif_int.dat ../datasets/uniform/txnse_unif_int.dat 128 out.txt`.

To get latencies, replace YCSB_CSLM with YCSB_CSLM_lat.
