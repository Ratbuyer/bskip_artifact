build:
	g++-11 -I/home/eddy/repo/folly \
   	-I/home/eddy/repo/folly/build \
   	ycsb.cpp \
   	-o ycsb \
   	-L/home/eddy/repo/folly/build \
   	-lfolly \
   	-lpthread -lglog -ldl -lfmt -lgflags \
   	-lboost_filesystem -lboost_program_options -lboost_system \
   	-ldouble-conversion \
   	-O3 -march=native -DLATENCY=1

clean:
	rm -f output.txt ycsb
