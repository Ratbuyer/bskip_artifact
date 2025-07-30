numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/uniform/ c 32 out.txt
numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/uniform/ c 16 out.txt
numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/uniform/ c 8 out.txt
numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/uniform/ c 4 out.txt
numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/uniform/ c 2 out.txt
