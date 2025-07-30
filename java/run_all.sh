numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/uniform/loada_unif_int.dat /home/eddy/datasets/uniform/txnsa_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/uniform/loadb_unif_int.dat /home/eddy/datasets/uniform/txnsb_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/uniform/loadc_unif_int.dat /home/eddy/datasets/uniform/txnsc_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/uniform/loade_unif_int.dat /home/eddy/datasets/uniform/txnse_unif_int.dat 128 aaaa

numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/uniform/loada_unif_int.dat /home/eddy/datasets/uniform/txnsa_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/uniform/loadb_unif_int.dat /home/eddy/datasets/uniform/txnsb_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/uniform/loadc_unif_int.dat /home/eddy/datasets/uniform/txnsc_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/uniform/loade_unif_int.dat /home/eddy/datasets/uniform/txnse_unif_int.dat 128 aaaa



numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/zipfian/loada_unif_int.dat /home/eddy/datasets/zipfian/txnsa_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/zipfian/loadb_unif_int.dat /home/eddy/datasets/zipfian/txnsb_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/zipfian/loadc_unif_int.dat /home/eddy/datasets/zipfian/txnsc_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM /home/eddy/datasets/zipfian/loade_unif_int.dat /home/eddy/datasets/zipfian/txnse_unif_int.dat 128 aaaa

numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/zipfian/loada_unif_int.dat /home/eddy/datasets/zipfian/txnsa_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/zipfian/loadb_unif_int.dat /home/eddy/datasets/zipfian/txnsb_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/zipfian/loadc_unif_int.dat /home/eddy/datasets/zipfian/txnsc_unif_int.dat 128 aaaa
numactl --cpunodebind=0,1 --membind=0,1 java YCSB_CSLM_lat /home/eddy/datasets/zipfian/loade_unif_int.dat /home/eddy/datasets/zipfian/txnse_unif_int.dat 128 aaaa
