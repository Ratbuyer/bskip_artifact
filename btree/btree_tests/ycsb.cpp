#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <random>
#include <stdlib.h>
#include <sys/time.h>
#include <vector>
// #include "tbb/tbb.h"
#include "container/btree_map.hpp"
#include <container/btree_set.hpp>
#include <functional>
#include <thread>

#include "tools.hpp"

static long get_usecs() {
	struct timeval st;
	gettimeofday(&st, NULL);
	return st.tv_sec * 1000000 + st.tv_usec;
}

using namespace std;

using Key = uint64_t;

using TID = uint64_t;

// index types
enum {
	TYPE_BTREE,
	TYPE_ART,
	TYPE_HOT,
	TYPE_BWTREE,
	TYPE_MASSTREE,
	TYPE_CLHT,
	TYPE_FASTFAIR,
	TYPE_LEVELHASH,
	TYPE_CCEH,
	TYPE_WOART,
};

enum {
	OP_INSERT,
	OP_UPDATE,
	OP_READ,
	OP_SCAN,
	OP_SCAN_END,
	OP_DELETE,
};

enum {
	WORKLOAD_A,
	WORKLOAD_B,
	WORKLOAD_C,
	WORKLOAD_D,
	WORKLOAD_E,
	WORKLOAD_X,
	WORKLOAD_Y,
};

enum {
	RANDINT_KEY,
	STRING_KEY,
};

enum {
	UNIFORM,
	ZIPFIAN,
};

namespace Dummy {
inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void clflush(char *data, int len, bool front, bool back) {
	if (front)
		mfence();
	volatile char *ptr = (char *)((unsigned long)data & ~(64 - 1));
	for (; ptr < data + len; ptr += 64) {
#ifdef CLFLUSH
		asm volatile("clflush %0" : "+m"(*(volatile char *)ptr));
#elif CLFLUSH_OPT
		asm volatile(".byte 0x66; clflush %0" : "+m"(*(volatile char *)(ptr)));
#elif CLWB
		asm volatile(".byte 0x66; xsaveopt %0" : "+m"(*(volatile char *)(ptr)));
#endif
	}
	if (back)
		mfence();
}
} // namespace Dummy

static uint64_t LOAD_SIZE = 100000000;
static uint64_t RUN_SIZE = 100000000;

void loadKey(TID tid, Key &key) { return; }

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

template <class T>
std::vector<T> create_random_data(size_t n, size_t max_val,
								  std::seed_seq &seed) {

	std::mt19937_64 eng(seed); // a source of random data

	std::uniform_int_distribution<T> dist(0, max_val);
	std::vector<T> v(n);

	generate(begin(v), end(v), bind(dist, eng));
	return v;
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

void ycsb_load_run_randint(std::string init_file, std::string txn_file,
						   int num_thread, std::vector<uint64_t> &init_keys,
						   std::vector<uint64_t> &keys,
						   std::vector<uint64_t> &range_end,
						   std::vector<int> &ranges, std::vector<int> &ops,
                           std::string output) {

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

	std::vector<double> load_tpts;
	std::vector<double> run_tpts;

#if LATENCY
	constexpr int batch_size = 10;
	ThreadSafeVector<uint64_t> load_latencies;
	ThreadSafeVector<uint64_t> latencies;
#endif

	for (int k = 0; k < 6; k++) {
		tlx::btree_map<uint64_t, uint64_t, std::less<uint64_t>,
					   tlx::btree_default_traits<uint64_t, uint64_t>,
					   std::allocator<uint64_t>, true>
			concurrent_map;
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

			concurrent_map.clear_stats();

			auto starttime = std::chrono::system_clock::now();

#if LATENCY
			parallel_for(
				num_thread, 0, RUN_SIZE / batch_size, [&](const uint64_t &i) {
					auto start = std::chrono::system_clock::now();

					for (int j = 0; j < batch_size; j++) {

						const int index = i * batch_size + j;

						if (ops[index] == OP_INSERT) {
							concurrent_map.insert({keys[index], keys[index]});
						} else if (ops[index] == OP_READ) {
							concurrent_map.value(keys[index]);
						} else if (ops[index] == OP_SCAN) {
							uint64_t start = keys[index];
							uint64_t key_sum = 0, val_sum = 0;
#if LEAFDS
							concurrent_map.map_range_length(
								keys[index], ranges[index],
								[&key_sum, &val_sum]([[maybe_unused]] auto key,
													 auto val) {
									key_sum += key;
									val_sum += val;
								});
#else
								concurrent_map.map_range_length(
									keys[index], ranges[index],
									[&key_sum,
									 &val_sum]([[maybe_unused]] auto el) {
										key_sum += el.first;
										val_sum += el.second;
									});
#endif
						}

						else if (ops[index] == OP_SCAN_END) {
							uint64_t key_sum = 0, val_sum = 0;
#if LEAFDS
							concurrent_map.map_range(
								keys[index], range_end[index],
								[&key_sum, &val_sum]([[maybe_unused]] auto key,
													 auto val) {
									key_sum += key;
									val_sum += val;
#else
								concurrent_map.map_range(
									keys[index], range_end[index],
									[&key_sum,
									 &val_sum]([[maybe_unused]] auto el) {
										key_sum += el.first;
										val_sum += el.second;
#endif
								});
						} else if (ops[index] == OP_UPDATE) {
							std::cout << "NOT SUPPORTED CMD!\n";
							exit(0);
						}
					}

					auto end = std::chrono::high_resolution_clock::now();
					
					if (k == 3)
					latencies.push_back(
						std::chrono::duration_cast<std::chrono::nanoseconds>(
							end - start)
							.count() / batch_size);
				});

#else

			parallel_for(num_thread, 0, RUN_SIZE, [&](const uint64_t &i) {
				if (ops[i] == OP_INSERT) {
					concurrent_map.insert({keys[i], keys[i]});
				} else if (ops[i] == OP_READ) {
					concurrent_map.value(keys[i]);
				} else if (ops[i] == OP_SCAN) {
					uint64_t start = keys[i];
					uint64_t key_sum = 0, val_sum = 0;
#if LEAFDS
					concurrent_map.map_range_length(
						keys[i], ranges[i],
						[&key_sum, &val_sum]([[maybe_unused]] auto key,
											 auto val) {
							key_sum += key;
							val_sum += val;
						});
#else
						concurrent_map.map_range_length(
							keys[i], ranges[i],
							[&key_sum, &val_sum]([[maybe_unused]] auto el) {
								key_sum += el.first;
								val_sum += el.second;
							});
#endif
				} else if (ops[i] == OP_SCAN_END) {
					uint64_t key_sum = 0, val_sum = 0;
#if LEAFDS
					concurrent_map.map_range(
						keys[i], range_end[i],
						[&key_sum, &val_sum]([[maybe_unused]] auto key,
											 auto val) {
							key_sum += key;
							val_sum += val;
#else
						concurrent_map.map_range(
							keys[i], range_end[i],
							[&key_sum, &val_sum]([[maybe_unused]] auto el) {
								key_sum += el.first;
								val_sum += el.second;
#endif
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

		}
	}
#if LATENCY
	load_latencies.print_percentiles();
	latencies.print_percentiles();
	
	// latencies.save_to_csv(output);
#endif

	printf("\tMedian Load throughput: %f ,ops/us\n", findMedian(load_tpts));
	printf("\tMedian Run throughput: %f ,ops/us\n", findMedian(run_tpts));
	printf("\n\n");
}

int main(int argc, char **argv) {
	if (argc != 5) {
		std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [key "
					 "distribution] [access pattern] [number of threads]\n";
		std::cout << "1. index type: art hot bwtree masstree clht\n";
		std::cout << "               fastfair levelhash cceh woart\n";
		std::cout << "2. ycsb workload type: a, b, c, e\n";
		std::cout << "3. key distribution: randint, string\n";
		std::cout << "4. access pattern: uniform, zipfian\n";
		std::cout << "5. number of threads (integer)\n";
		return 1;
	}

	string file_dir = argv[1];

	string load_file = file_dir;
	string index_file = file_dir;

	int workload;
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
	string output = argv[4];

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

	ycsb_load_run_randint(load_file, index_file, num_thread, init_keys, keys,
							 ranges_end, ranges, ops, output);

	return 0;
}
