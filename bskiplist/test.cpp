#define DEBUG_PRINT 0
#define STATS 0

#include "ParallelTools/reducer.h"
#include "cxxopts.hpp"
#include <ParallelTools/parallel.h>
#include <algorithm>
#include <assert.h>
#include <functional>
#include <random>
#include <set>
#include <sys/time.h>
#include <vector>

#include "bskip.h"
#include "timers.hpp"

#define NUM_THREADS 16

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

template <typename F> inline void parallel_for(size_t start, size_t end, F f) {
	const int numThreads = NUM_THREADS;
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

template <class T>
std::vector<T> create_random_data_in_parallel(size_t n, size_t max_val) {
	std::vector<T> v(n);
	uint64_t per_worker = (n / ParallelTools::getWorkers()) + 1;

	// Generate a unique base seed using time and randomness
	std::random_device rd;
	uint64_t base_seed =
		std::chrono::steady_clock::now().time_since_epoch().count() ^ rd();

	ParallelTools::parallel_for(
		0, ParallelTools::getWorkers(), [&](uint64_t i) {
			uint64_t start = i * per_worker;
			uint64_t end = (i + 1) * per_worker;
			if (end > n) {
				end = n;
			}
			if ((int)i == ParallelTools::getWorkers() - 1) {
				end = n;
			}

			// Each thread gets a unique seed by combining base_seed and worker
			// ID
			std::mt19937_64 eng(base_seed + i);
			std::uniform_int_distribution<uint64_t> dist(0, max_val);

			for (size_t j = start; j < end; j++) {
				v[j] = dist(eng);
			}
		});

	return v;
}

template <class T, size_t node_size_in_bytes>
void test_parallel_correctness(uint64_t max_size, int range_query_length) {

	if (max_size > std::numeric_limits<T>::max()) {
		max_size = std::numeric_limits<T>::max();
	}

	std::vector<T> keys = create_random_data_in_parallel<T>(
		max_size, std::numeric_limits<T>::max());
	std::vector<T> values = create_random_data_in_parallel<T>(
		max_size, std::numeric_limits<T>::max());

	std::vector<T> sorted_keys = keys;
	std::sort(sorted_keys.begin(), sorted_keys.end());

	std::vector<T> sorted_values = values;

	const int node_size = node_size_in_bytes / (2 * sizeof(T));

	const int p = node_size;

	using parallel_traits = BSkip_traits<true, node_size, p, T, T>;

	BSkip<parallel_traits> s;

	// round one insert same key and value
	parallel_for(0, max_size, [&](uint64_t i) {
		s.insert({keys[i], keys[i]});
	});

	// round two override previous insert with different value
	parallel_for(0, max_size, [&](uint64_t i) {
		s.insert({keys[i], values[i]});
	});

	// check if all keys exist
	parallel_for(0, max_size, [&](uint64_t i) {
		tbassert(s.exists(keys[i]), "i = %lu\n", i);
	});

	// check if all values are correct
	parallel_for(0, max_size, [&](uint64_t i) {
		tbassert(std::get<0>(s.value(keys[i])) == values[i], "i = %lu\n", i);
	});

	std::random_device rd;
	std::mt19937 gen(rd());
	std::uniform_int_distribution<> distrib(0, max_size - 100);
	int random_start_index = distrib(gen);

	printf("random_start_index = %d\n", random_start_index);

	// check fixed length range query, 100 runs
	parallel_for(random_start_index, 100 + random_start_index, [&](uint64_t i) {
		unsigned long long int sum = 0;
		T start = keys[i];
		auto it = std::find(sorted_keys.begin(), sorted_keys.end(), start);
		if (it == sorted_keys.end()) {
			tbassert(false, "Key not found in sorted_keys\n");
		}
		int start_index = it - sorted_keys.begin();
		for (int k = 0; k < range_query_length; k++) {
			T key = sorted_keys[start_index + k];
			auto it = std::find(keys.begin(), keys.end(), key);
			if (it == keys.end()) {
				tbassert(false, "Key not found in keys\n");
			}
			int value_index = it - keys.begin();
			sum += values[value_index];
		}
		unsigned long long int lambda_sum = 0;
		s.map_range_length(keys[i], range_query_length, [&lambda_sum](auto key, auto val) {
			lambda_sum += val;
		});
		tbassert(sum == lambda_sum,
				 "i = %d, range_query_length = %d, sum = %llu, lambda_sum = %llu\n", i, range_query_length, sum,
				 lambda_sum);
		// printf("i = %d, range_query_length = %d, sum = %llu, lambda_sum = %llu\n", i, range_query_length, sum,
		// 	   lambda_sum);
	});

	// s.get_size_stats();

	s.validate_structure();
}

int main(int argc, char **argv) {
	
	// make sure to compile this file with DEBUG=1 for assertions to work
	test_parallel_correctness<uint64_t, 1024>(1000000, 100);

	printf("success\n");
}
