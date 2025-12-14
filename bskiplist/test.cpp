#define DEBUG_PRINT 0
#define STATS 0

#include <ParallelTools/reducer.h>
#include <ParallelTools/parallel.h>
#include <algorithm>
#include <assert.h>
#include <functional>
#include <random>
#include <set>
#include <sys/time.h>
#include <vector>
#include <chrono>

#include "include/BSkipList.hpp"

#define NUM_THREADS 8

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
inline void parallel_for(size_t start, size_t end, F f) {
    const int numThreads = NUM_THREADS;
    pthread_t threads[numThreads];
    ThreadArgs threadArgs[numThreads];

    for (int i = 0; i < numThreads; i++) {
        // func(start_index, end_index_total)
        threadArgs[i].func = [&f, end, numThreads](int first, int end_total) {
            // first = starting index for this thread
            // end_total = global end (same for all threads)
            for (size_t k = static_cast<size_t>(first);
                 k < static_cast<size_t>(end_total);
                 k += static_cast<size_t>(numThreads)) {
                f(k);
            }
        };

        // Each thread starts at a different offset, but shares the same global end
        threadArgs[i].start = static_cast<int>(start) + i;
        threadArgs[i].end   = static_cast<int>(end);

        int result = pthread_create(&threads[i], nullptr, threadFunction, &threadArgs[i]);
        if (result != 0) {
            std::cerr << "Failed to create thread " << i << std::endl;
            std::exit(-1);
        }
    }

    // Wait for the threads to finish
    for (int i = 0; i < numThreads; i++) {
        pthread_join(threads[i], nullptr);
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
			std::uniform_int_distribution<uint64_t> dist(1, max_val);

			for (size_t j = start; j < end; j++) {
				v[j] = dist(eng);
			}
		});

	return v;
}

bool flip_coin(double chance, uint64_t seed) {
    thread_local std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(rng) < chance;
}

template <class T, size_t node_size_in_bytes>
void test_parallel_correctness(uint64_t max_size, int range_query_length) {

	if (max_size > std::numeric_limits<T>::max()) {
		max_size = std::numeric_limits<T>::max() - 1;
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
			auto it2 = std::find(keys.begin(), keys.end(), key);
			if (it2 == keys.end()) {
				tbassert(false, "Key not found in keys\n");
			}
			int value_index = it2 - keys.begin();
			sum += values[value_index];
		}
		unsigned long long int lambda_sum = 0;
		s.map_range_length(keys[i], range_query_length, [&lambda_sum](auto key, auto val) {
			lambda_sum += std::get<0>(val);
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

template <class T, size_t node_size_in_bytes>
void test_parallel_correctness(uint64_t max_size) {
    if (max_size > std::numeric_limits<T>::max()) {
        max_size = std::numeric_limits<T>::max() - 1;
    }

    std::vector<T> keys = create_random_data_in_parallel<T>(
        max_size, std::numeric_limits<T>::max());
    std::vector<T> values = create_random_data_in_parallel<T>(
        max_size, std::numeric_limits<T>::max());

    // array of index of keys to delete
    std::vector<T> delete_keys;
    std::vector<T> exists_keys;

    for (size_t i = max_size / 2; i < max_size; ++i) {
        if (flip_coin(.7, 3849)) {
            delete_keys.push_back(i - (max_size / 2));
        } else {
            exists_keys.push_back(i);
        }
    }

    const int node_size = node_size_in_bytes / (2 * sizeof(T));

    const int p = node_size;

    using parallel_traits = BSkip_traits<true, node_size, p, T, T>;

    BSkip<parallel_traits> s;

    // round one insert first half keys
    parallel_for(0, max_size / 2, [&](uint64_t i) {
        s.insert({keys[i], values[i]});
    });

    s.count_all();

    // do some timing
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // in parallel, insert and delete the other half
    uint64_t run_size = std::max(delete_keys.size(), exists_keys.size());
	parallel_for(0, run_size, [&](uint64_t i) {
        if (i < delete_keys.size()) {
            s.delete_key(keys[delete_keys[i]]);
        }
        if (i < exists_keys.size()) {
            s.insert({keys[exists_keys[i]], values[exists_keys[i]]});
        }
	});
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                        begin)
                    .count();
    printf("total time: %f seconds\n", dur * 1.0 / 1000000);

    // check if the deleted keys do not exist
    parallel_for(0, delete_keys.size(), [&](uint64_t i) {
        tbassert(s.exists(keys[delete_keys[i]]) == false, "i = %lu\n", i);
    });

    printf("deleted keys do not exist\n");

    // check if everything else exist
    parallel_for(0, exists_keys.size(), [&](uint64_t i) {
    auto [v] = s.value(keys[exists_keys[i]]);
    tbassert(v == values[exists_keys[i]], "i = %lu\n", i);
    });

    printf("all keys values match\n");

    s.validate_structure();

    // delete the rest of the keys

    s.count_all();
}

template <class T, size_t node_size_in_bytes>
void test_serial_correctness(uint64_t max_size) {
    if (max_size > std::numeric_limits<T>::max()) {
        max_size = std::numeric_limits<T>::max() - 1;
    }

    std::vector<T> keys = create_random_data_in_parallel<T>(
        max_size, std::numeric_limits<T>::max());
    std::vector<T> values = create_random_data_in_parallel<T>(
        max_size, std::numeric_limits<T>::max());

    const int node_size = node_size_in_bytes / (2 * sizeof(T));

    const int p = node_size;

    using serial_traits = BSkip_traits<false, node_size, p, T, T>;

    BSkip<serial_traits> s;

    // round one insert keys
    for (uint64_t i = 0; i < max_size; i++) {
        s.insert({keys[i], values[i]});
    }

    // do some timing
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    // delete first half
    for (uint64_t i = 0; i < max_size / 2; i++) {
        s.delete_key(keys[i]);
    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                        begin)
                    .count();
    printf("deletion of %lu keys took %f seconds\n", max_size / 2,
           dur * 1.0 / 1000000);

    s.count_all();

    // check if first half keys do not exist
    for (uint64_t i = 0; i < max_size / 2; i++) {
        tbassert(!s.exists(keys[i]), "i = %lu\n", i);
    }

    // check if second half keys exist
    for (uint64_t i = max_size / 2; i < max_size;
            i++) {
            tbassert(std::get<0>(s.value(keys[i])) == values[i], "i = %lu\n", i);
        }

    s.validate_structure();

    // delete the rest of the keys
    for (uint64_t i = max_size / 2; i < max_size; i++) {
        s.delete_key(keys[i]);
    }

    s.count_all();

    s.validate_structure();
}

void test_small() {
    const int node_size_in_bytes = 256;
    const int node_size = node_size_in_bytes / (2 * sizeof(uint64_t));
    const int p = node_size;
    using serial_traits = BSkip_traits<false, node_size, p, uint64_t, uint64_t>;

    BSkip<serial_traits> s;

    for (int i = 1; i < 10000; i++) {
        s.insert({i, i * 10});
    }

    // delete some keys
    for (int i = 1; i < 100; i++ ) {
        s.delete_key(i);
    }

    s.count_all();

    // check existence
    for (int i = 1; i < 100; i++) {
        tbassert(!s.exists(i), "i = %d\n", i);
    }
}

void test_small_parallel() {
    const int node_size_in_bytes = 1024;
    const int node_size = node_size_in_bytes / (2 * sizeof(uint64_t));
    const int p = node_size;
    using serial_traits = BSkip_traits<true, node_size, p, uint64_t, uint64_t>;

    BSkip<serial_traits> s;

    int total = 10000;

    parallel_for (1, total, [&](int i) {
        s.insert({i, i * 10});
    });

    int deletes = 80;

    // delete some keys
    parallel_for (1, deletes, [&](int i) {
        // for (int j = 0; j < 80; j++) {
        //     s.delete_key(j);
        // }
        s.delete_key(i);
        s.insert({i, i});
    });

    s.count_all();

    // check existence
    // parallel_for (1, deletes, [&](int i) {
    //     tbassert(!s.exists(i), "i = %d\n", i);
    // });

    // check existence of the rest
    parallel_for (deletes, total, [&](int i) {
        (void) i;
        tbassert(std::get<0>(s.value(i)) == (uint64_t) i * 10, "i = %d, value = %lu\n", i, std::get<0>(s.value(i)));
    });

    s.validate_structure();
}

void test() {
    const int node_size_in_bytes = 1024;
    const int node_size = node_size_in_bytes / (2 * sizeof(uint64_t));
    const int p = node_size;
    using serial_traits = BSkip_traits<false, node_size, p, uint64_t, uint64_t>;

    BSkip<serial_traits> s;

    int total = 10000;

    for (int i = 1; i < total; i++) {
        s.insert({i, i * 10});
    }

    s.validate_structure();

    int deletes = 8000;

    // delete some keys
    for (int i = 1; i < deletes; i++) {
        // for (int j = 0; j < 80; j++) {
        //     s.delete_key(j);
        // }
        s.delete_key(i);
        s.insert({i, i});
    }

    s.count_all();
}

int main() {
    // make sure to compile this file with DEBUG=1 for assertions to work

    // test delete + insert
	test_parallel_correctness<uint64_t, 256>(1000000);

    // test insert, value, and range query
    test_parallel_correctness<uint64_t, 1024>(1000000, 100);

    // test_small_parallel();

    // test();

	printf("success\n");
}
