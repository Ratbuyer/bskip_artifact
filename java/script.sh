percentages=(1 5 10 25 75 100)
distribution=("uniform" "zipfian")
threads=(2 1)
dir="/home/eddy/bskiplist/results/brian"

numactl -i all java YCSB_CSLM_lat ../datasets/uniform/loade_unif_int.dat ../datasets/uniform/txnse_unif_int.dat 128 out.txt

numactl -i all java YCSB_CSLM_lat ../datasets/zipfian/loade_unif_int.dat ../datasets/zipfian/txnse_unif_int.dat 128 out.txt

for t in "${threads[@]}"
do

for i in "${distribution[@]}"
do

                echo $t $i
                #numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/custom/$i/insert_100/ a $t out.txt
                #numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/custom/$i/insert_0/ a $t out.txt

                #numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ a $t  out.txt
                #numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ b $t  out.txt
                #numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ c $t  out.txt
                #numactl -N 1 -m 1 ./ycsb /home/eddy/datasets/$i/ e $t  out.txt
		
		numactl -N 1 -m 1 java YCSB_CSLM ../datasets/$i/loada_unif_int.dat ../datasets/$i/txnsa_unif_int.dat $t out.txt
		numactl -N 1 -m 1 java YCSB_CSLM ../datasets/$i/loadc_unif_int.dat ../datasets/$i/txnsc_unif_int.dat $t out.txt
		
		numactl -N 1 -m 1 java YCSB_CSLM ../datasets/custom/$i/insert_100/loada_unif_int.dat ../datasets/custom/$i/insert_100/txnsa_unif_int.dat $t out.txt
        done
done
