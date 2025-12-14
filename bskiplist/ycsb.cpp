#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <stdlib.h>
#include <sys/time.h>
#include <thread>
#include <vector>

#include <sys/time.h>

#include "include/BSkipList.hpp"
#include "helpers.h"
#include "include/tools.h"
#include <ParallelTools/parallel.h>

using namespace std;

using Key = uint64_t;
using TID = uint64_t;

enum {
	OP_INSERT,
	OP_UPDATE,
	OP_READ,
	OP_SCAN,
	OP_SCAN_END,
	OP_DELETE,
};

static uint64_t LOAD_SIZE = 1000000;
static uint64_t RUN_SIZE = 1000000;

struct ThreadArgs {
	std::function<void(int, int)> func;
	int start;
	int end;
};

void *threadFunction(void *arg) {
	ThreadArgs *args = static_cast<ThreadArgs *>(arg);
	args->func(args->start, args->end);
	pthread_exit(NULL);
}

template <typename F>
inline void parallel_for(int numThreads, size_t start, size_t end, F f) {
	pthread_t threads[numThreads];
	ThreadArgs threadArgs[numThreads];
	int per_thread = (end - start) / numThreads;

	// Create the threads and start executing the lambda function
	for (int i = 0; i < numThreads; i++) {
		threadArgs[i].func = [&f](int arg1, int arg2) {
			for (int k = arg1; k < arg2; k++) {
				f(k);
			}
		};

		threadArgs[i].start = start + (i * per_thread);
		if (i == numThreads - 1) {
			threadArgs[i].end = end;
		} else {
			threadArgs[i].end = start + ((i + 1) * per_thread);
		}
		int result =
			pthread_create(&threads[i], NULL, threadFunction, &threadArgs[i]);

		if (result != 0) {
			std::cerr << "Failed to create thread " << i << std::endl;
			exit(-1);
		}
	}

	// Wait for the threads to finish
	for (int i = 0; i < numThreads; i++) {
		pthread_join(threads[i], NULL);
	}
}

double findMedian(vector<double> &vec) {
	size_t size = vec.size();
	if (size == 0) {
		return 0;
	}
	sort(vec.begin(), vec.end());
	if (size % 2 == 0) {
		return (vec[size / 2 - 1] + vec[size / 2]) / 2;
	} else {
		return vec[size / 2];
	}
}

template<int node_size = 1024, float p_scale = 0.5>
void ycsb_load_run_randint(std::string init_file, std::string txn_file,
						   int num_thread, std::vector<uint64_t> &init_keys,
						   std::vector<uint64_t> &keys,
						   std::vector<uint64_t> &range_end,
						   std::vector<int> &ranges, std::vector<int> &ops) {

	printf("loading with file: %s\n", init_file.c_str());
	printf("running with file: %s\n", txn_file.c_str());

	std::ifstream infile_load(init_file);

	std::string op;
	uint64_t key;
	uint64_t rend;
	int range;

	std::string insert("INSERT");
	std::string update("UPDATE");
	std::string read("READ");
	std::string scan("SCAN");
	std::string scanend("SCANEND");

	uint64_t count = 0;
	while ((count < LOAD_SIZE) && infile_load.good()) {
		infile_load >> op >> key;
		if (op.compare(insert) != 0) {
			std::cout << "READING LOAD FILE FAIL!\n";
			return;
		}
		init_keys.push_back(key);
		count++;
	}

	fprintf(stderr, "Loaded %ld keys\n", count);

	std::ifstream infile_txn(txn_file);

	count = 0;
	while ((count < RUN_SIZE) && infile_txn.good()) {
		infile_txn >> op >> key;
		if (op.compare(insert) == 0) {
			ops.push_back(OP_INSERT);
			keys.push_back(key);
			ranges.push_back(1);
			range_end.push_back(1);
		} else if (op.compare(update) == 0) {
			ops.push_back(OP_UPDATE);
			keys.push_back(key);
			ranges.push_back(1);
			range_end.push_back(1);
		} else if (op.compare(read) == 0) {
			ops.push_back(OP_READ);
			keys.push_back(key);
			ranges.push_back(1);
			range_end.push_back(1);
		} else if (op.compare(scan) == 0) {
			infile_txn >> range;
			ops.push_back(OP_SCAN);
			keys.push_back(key);
			ranges.push_back(range);
			range_end.push_back(1);
		} else if (op.compare(scanend) == 0) {
			infile_txn >> rend;
			ops.push_back(OP_SCAN_END);
			keys.push_back(key);
			range_end.push_back(rend);
			ranges.push_back(1);
		} else {
			std::cout << "UNRECOGNIZED CMD!\n";
			return;
		}
		count++;
	}

	std::atomic<int> range_complete, range_incomplete;
	range_complete.store(0);
	range_incomplete.store(0);

	fprintf(stderr, "Loaded %ld more keys\n", count);

	std::this_thread::sleep_for(std::chrono::nanoseconds(3000000000));

	fprintf(stderr, "Slept\n");

	printf("bskiplist\n");

	std::vector<double> load_tpts;
	std::vector<double> run_tpts;

	int constexpr p = node_size / (sizeof(Key) + sizeof(TID));
	int constexpr promotion_rate = static_cast<int>(p * p_scale);
	using parallel_traits = BSkip_traits<true, p, promotion_rate, Key, TID>;


#if LATENCY
	constexpr int batch_size = 10;
	ThreadSafeVector<uint64_t> load_latencies;
	ThreadSafeVector<uint64_t> latencies;
#endif

	for (int k = 0; k < 6; k++) {
		BSkip<parallel_traits> concurrent_map;
		{
			auto starttime = get_usecs();
			
			#if LATENCY
			parallel_for(num_thread, 0, LOAD_SIZE / batch_size, [&](const uint64_t &i) {
				auto load_start = std::chrono::high_resolution_clock::now();
				for (int j = 0; j < batch_size; j++) {
					concurrent_map.insert({init_keys[i * 10 + j], init_keys[i * 10 + j]});
				}
				auto load_end = std::chrono::high_resolution_clock::now();
				if (k == 3) load_latencies.push_back(
					std::chrono::duration_cast<std::chrono::nanoseconds>(load_end - load_start)
						.count() /
					batch_size);
			});
			#else
			parallel_for(num_thread, 0, LOAD_SIZE, [&](const uint64_t &i) {
				concurrent_map.insert({init_keys[i], init_keys[i]});
			});
			#endif
			
			auto end = get_usecs();
			auto duration =
				end -
				starttime; // std::chrono::duration_cast<std::chrono::microseconds>(
						   // std::chrono::system_clock::now() - starttime);
			if (k != 0)
				load_tpts.push_back(((double)LOAD_SIZE) / duration);

			printf("\tLoad took %lu us, throughput = %f ops/us\n", duration,
				   ((double)LOAD_SIZE) / duration);

			// printf("Throughput: load, %f ,ops/us and time %ld in us\n",
			// (LOAD_SIZE * 1.0) / duration.count(), duration.count());
		}
		{
			// Run
			auto starttime = std::chrono::system_clock::now();
			concurrent_map.clear_stats();

#if LATENCY
			parallel_for(
				num_thread, 0, RUN_SIZE / batch_size, [&](const uint64_t &i) {
					// benchmark loops of 10
					auto start = std::chrono::high_resolution_clock::now();

					for (int j = 0; j < batch_size; j++) {

						const int index = i * batch_size + j;

						if (ops[index] == OP_INSERT) {
							concurrent_map.insert({keys[index], keys[index]});
						} else if (ops[index] == OP_READ) {
							concurrent_map.value(keys[index]);
						} else if (ops[index] == OP_SCAN) {
							uint64_t sum = 0;
							concurrent_map.map_range_length(
								keys[index], ranges[index],
								[&sum](auto key1, auto value) {
									key1 += std::get<0>(value);
									sum += std::get<0>(value);
								});
						}

						else if (ops[index] == OP_SCAN_END) {
							uint64_t sum = 0;
							concurrent_map.map_range(
								keys[index], range_end[index],
								[&sum](auto key1, auto value) {
									key1 += std::get<0>(value);
									std::get<0>(value);
								});
						} else if (ops[index] == OP_UPDATE) {
							std::cout << "NOT SUPPORTED CMD!\n";
							exit(0);
						}
					}

					auto end = std::chrono::high_resolution_clock::now();

					if (k == 3) latencies.push_back(
						std::chrono::duration_cast<std::chrono::nanoseconds>(
							end - start)
							.count() / batch_size);
				});

#else

			parallel_for(num_thread, 0, RUN_SIZE, [&](const uint64_t &i) {
				if (ops[i] == OP_INSERT) {
					concurrent_map.insert({keys[i], keys[i]});
                    // concurrent_map.delete_key(keys[i]);
				} else if (ops[i] == OP_READ) {
					concurrent_map.value(keys[i]);
				} else if (ops[i] == OP_SCAN) {
					uint64_t sum = 0;
					concurrent_map.map_range_length(
						keys[i], ranges[i], [&sum](auto key1, auto value) {
							key1 += std::get<0>(value);
							sum += std::get<0>(value);
						});
				} else if (ops[i] == OP_SCAN_END) {
					uint64_t sum = 0;
					concurrent_map.map_range(keys[i], range_end[i],
											 [&sum](auto key1, auto value) {
												 key1 += std::get<0>(value);
												 sum += std::get<0>(value);
											 });

				} else if (ops[i] == OP_UPDATE) {
					std::cout << "NOT SUPPORTED CMD!\n";
					exit(0);
				}
			});

#endif

			auto duration =
				std::chrono::duration_cast<std::chrono::microseconds>(
					std::chrono::system_clock::now() - starttime);

			if (k != 0)
				run_tpts.push_back((RUN_SIZE * 1.0) / duration.count());


			printf("\tRun, throughput: %f ,ops/us\n",
				   (RUN_SIZE * 1.0) / duration.count());
			
#if STATS
			concurrent_map.get_size_stats();
#endif
		}
	}
#if LATENCY
	load_latencies.print_percentiles();
	latencies.print_percentiles();
	// size_t pos = init_file.find_last_of("/");
	// std::string filename = (pos == std::string::npos) ? init_file : init_file.substr(pos + 1);
	// latencies.save_to_csv(output_file);
#endif
	printf("\tMedian Load throughput: %f ,ops/us\n", findMedian(load_tpts));
	printf("\tMedian Run throughput: %f ,ops/us\n", findMedian(run_tpts));

	printf("\n\n");
}

int main(int argc, char **argv) {
	if (argc != 4) {
		std::cout << "Usage: ./ycsb <path to ycsb files> <ycsb workload> [key "
					 "<# of threads>\n";
        std::cout << "Example: ./ycsb ../ycsb_data/uniform/ a 24\n";
		return 1;
	}

    std::cout << "Please make sure the partition counter in ParallelTools is equal to # of threads\n";

	string file_dir = argv[1];

	string load_file = file_dir;
	string index_file = file_dir;

	if (strcmp(argv[2], "a") == 0) {
		load_file += "loada_unif_int.dat";
		index_file += "txnsa_unif_int.dat";
	} else if (strcmp(argv[2], "b") == 0) {
		load_file += "loadb_unif_int.dat";
		index_file += "txnsb_unif_int.dat";
	} else if (strcmp(argv[2], "c") == 0) {
		load_file += "loadc_unif_int.dat";
		index_file += "txnsc_unif_int.dat";
	} else if (strcmp(argv[2], "d") == 0) {
		load_file += "loadd_unif_int.dat";
		index_file += "txnsd_unif_int.dat";
	} else if (strcmp(argv[2], "e") == 0) {
		load_file += "loade_unif_int.dat";
		index_file += "txnse_unif_int.dat";
	} else if (strcmp(argv[2], "x") == 0) {
		load_file += "loadx_unif_int.dat";
		index_file += "txnsx_unif_int.dat";
	} else if (strcmp(argv[2], "y") == 0) {
		load_file += "loady_unif_int.dat";
		index_file += "txnsy_unif_int.dat";
	} else {
		fprintf(stderr, "Unknown workload: %s\n", argv[2]);
		exit(1);
	}

	int num_thread = atoi(argv[3]);

	std::vector<uint64_t> init_keys;
	std::vector<uint64_t> keys;
	std::vector<uint64_t> ranges_end;
	std::vector<int> ranges;
	std::vector<int> ops;

	init_keys.reserve(LOAD_SIZE);
	keys.reserve(RUN_SIZE);
	ranges_end.reserve(RUN_SIZE);
	ranges.reserve(RUN_SIZE);
	ops.reserve(RUN_SIZE);

	memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
	memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
	memset(&ranges_end[0], 0x00, RUN_SIZE * sizeof(uint64_t));
	memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
	memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

	ycsb_load_run_randint<2048, 0.5f>(load_file, index_file, num_thread, init_keys, keys, ranges_end, ranges, ops);

	return 0;
}
