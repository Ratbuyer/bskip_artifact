percentages=(1 5 10 25 75 100)
distribution=("uniform")
dir="/home/eddy/bskiplist/results/brian"

for i in "${distribution[@]}"
do
	numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ c 64 out.txt > ${dir}/${i}_insert_0_log.txt
    numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/custom/$i/insert_100/ a 64 out.txt > ${dir}/${i}_insert_100_log.txt
done
