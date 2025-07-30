distribution=("uniform" "zipfian")

for i in "${distribution[@]}"
do
	numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ a 64 out.txt
	numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ b 64 out.txt
	numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ c 64 out.txt
	numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ e 64 out.txt
done
