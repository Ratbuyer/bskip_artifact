numactl -N 1 -m 1 java YCSB_CSLM ../datasets/custom/uniform/insert_100/loada_unif_int.dat ../datasets/custom/uniform/insert_100/txnsa_unif_int.dat 64 out.txt

numactl -i all java YCSB_CSLM ../datasets/custom/uniform/insert_100/loada_unif_int.dat ../datasets/custom/uniform/insert_100/txnsa_unif_int.dat 128 out.txt
