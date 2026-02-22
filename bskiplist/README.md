# bskiplist
---
to compile test binary:

`make test DEBUG=1` and run `./test`

to compile ycsb driver:

`make ycsb` and run `./ycsb <path to load file> <path to run file> <# of threads> <# of keys>`

For example, ./ycsb /home/loada_unif_int.dat /home/txnsa_unif_int.dat 64 100000000
