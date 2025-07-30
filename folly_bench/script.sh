percentages=(1 5 10 25 75 100)
distribution=(0 1)
threads=(1 2 4 8 16)
dir="/home/eddy/bskiplist/results/brian"

for t in "${threads[@]}"
do

for i in "${distribution[@]}"
do

                echo $t $i
                LD_LIBRARY_PATH=/home/brian/glog/build/:$LD_LIBRARY_PATH numactl -N 1 -m 1  ./ycsb ../../eddy/datasets/$i/ a $t out.txt
		#LD_LIBRARY_PATH=/home/brian/glog/build/:$LD_LIBRARY_PATH numactl -N 1 -m 1  ./ycsb ../../eddy/datasets/$i/ b $t out.txt
		LD_LIBRARY_PATH=/home/brian/glog/build/:$LD_LIBRARY_PATH numactl -N 1 -m 1  ./ycsb ../../eddy/datasets/$i/ c $t out.txt
		LD_LIBRARY_PATH=/home/brian/glog/build/:$LD_LIBRARY_PATH numactl -N 1 -m 1  ./ycsb /home/eddy/datasets/custom/$i/insert_100/ a $t out.txt
        done
done
