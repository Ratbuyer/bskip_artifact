#define DEBUG_PRINT 0
#define STATS 0

#include "ParallelTools/reducer.h"
#include <assert.h>
#include <random>
#include <vector>
#include <algorithm>
#include <functional>
#include <set>
#include <sys/time.h>
#include <ParallelTools/parallel.h>
#include "cxxopts.hpp"

#include "timers.hpp"
#include "bskip.h"

#if CILK != 1
#define cilk_for for
#endif

#define NUM_THREADS 128

struct ThreadArgs {
    std::function<void(int, int)> func;
    int start;
    int end;
};


void* threadFunction(void* arg) {
    ThreadArgs* args = static_cast<ThreadArgs*>(arg);
    args->func(args->start, args->end);
    pthread_exit(NULL);
}

template <typename F> inline void parallel_for(size_t start, size_t end, F f) {
    const int numThreads = NUM_THREADS;
    pthread_t threads[numThreads];
    ThreadArgs threadArgs[numThreads];
    int per_thread = (end - start)/numThreads;

    // Create the threads and start executing the lambda function
    for (int i = 0; i < numThreads; i++) {
        threadArgs[i].func = [&f](int arg1, int arg2) {
            for (int k = arg1 ; k < arg2; k++) {
                f(k);
            }
        };

        threadArgs[i].start = start + (i * per_thread);
        if (i == numThreads - 1) {
          threadArgs[i].end = end;
        } else {
          threadArgs[i].end = start + ((i+1) * per_thread);
        }
        int result = pthread_create(&threads[i], NULL, threadFunction, &threadArgs[i]);

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
std::vector<T> create_random_data_in_parallel(size_t n, size_t max_val,
                                                   uint64_t seed_add = 0) {

  std::vector<T> v(n);
  uint64_t per_worker = (n / ParallelTools::getWorkers()) + 1;
  ParallelTools::parallel_for(0, ParallelTools::getWorkers(), [&](uint64_t i) {
    uint64_t start = i * per_worker;
    uint64_t end = (i + 1) * per_worker;
    if (end > n) {
      end = n;
    }
    if ((int)i == ParallelTools::getWorkers() - 1) {
      end = n;
    }
    std::random_device rd;
    std::mt19937_64 eng(i + seed_add); // a source of random data

    std::uniform_int_distribution<uint64_t> dist(0, max_val);
    for (size_t j = start; j < end; j++) {
      v[j] = dist(eng);
    }
  });
  return v;
}

// if ENABLE_TRACE_TIMER is on, it computes latencies
// if it is off, i have been using it for scalability numbers
template <class T, size_t MAX_KEYS, int p> 
void test_latencies(uint64_t max_size, bool write_csv) {
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());

  uint32_t seed_offset = 100;
  std::vector<T> data_to_add = create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max(), seed_offset);

  printf("done generating the data\n");
#if ENABLE_TRACE_TIMER == 1
  std::vector<uint64_t> insert_times(max_size);
  std::vector<uint64_t> find_times(max_size);
  std::vector<uint64_t> insert_50;
  std::vector<uint64_t> insert_90;
  std::vector<uint64_t> insert_99;
  std::vector<uint64_t> insert_999;
  std::vector<uint64_t> insert_max;
  std::vector<uint64_t> find_50;
  std::vector<uint64_t> find_90;
  std::vector<uint64_t> find_99;
  std::vector<uint64_t> find_999;
  std::vector<uint64_t> find_max;
#endif
  uint64_t start, end;
  std::vector<uint64_t> total_insert_times;
  std::vector<uint64_t> total_find_times;

  // concurrent, B, p, K, V?
  using traits = BSkip_traits<false, MAX_KEYS, p, uint64_t>;

  int num_trials = 5;
  for (int j = 0; j <= num_trials; j++) {

    BSkip<traits> s_concurrent;
    start = get_usecs();
    // ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
    parallel_for(0, max_size, [&](uint64_t i) {
#if ENABLE_TRACE_TIMER == 1
      uint64_t start_time, end_time;
      start_time = get_usecs();
#endif
      s_concurrent.insert(data_to_add[i]);

#if ENABLE_TRACE_TIMER == 1
      end_time = get_usecs();
      insert_times[i] = end_time - start_time;
#endif
    });
    end = get_usecs();
    if (j > 0) { total_insert_times.push_back(end - start); }
#if ENABLE_TRACE_TIMER == 1
    std::sort(insert_times.begin(), insert_times.end());
    if (j > 0) {
      insert_50.push_back(insert_times[insert_times.size() / 2]);
      insert_90.push_back(insert_times[insert_times.size() * 9 / 10]);
      insert_99.push_back(insert_times[insert_times.size() * 99 / 100]);
      insert_999.push_back(insert_times[insert_times.size() * 999 / 1000]);
      insert_max.push_back(insert_times[insert_times.size() - 1]);
    }
#endif
    // TIME POINT FINDS
    std::vector<bool> found_count(max_size);
    start = get_usecs();
    ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
#if ENABLE_TRACE_TIMER
      uint64_t start_time, end_time;
      start_time = get_usecs();
#endif
      found_count[i] = s_concurrent.exists(data[i]);
#if ENABLE_TRACE_TIMER == 1
      end_time = get_usecs();
      find_times[i] = end_time - start_time;
#endif
    });
    end = get_usecs();
    if (j > 0) { total_find_times.push_back(end - start); }

    int count_found = 0;
    for (uint32_t i = 0; i < max_size; i++) {
      if (found_count[i] == false) { 
        assert(s_concurrent.exists(data[i]));
      }
      count_found += (found_count[i] == true);
    }
    printf("\tDone finding %lu elts, count = %d \n", max_size, count_found);
#if ENABLE_TRACE_TIMER == 1
    std::sort(find_times.begin(), find_times.end());
    if (j > 0) {
      find_50.push_back(find_times[find_times.size() / 2]);
      find_90.push_back(find_times[find_times.size() * 9 / 10]);
      find_99.push_back(find_times[find_times.size() * 99 / 100]);
      find_999.push_back(find_times[find_times.size() * 999 / 1000]);
      find_max.push_back(find_times[find_times.size() - 1]);
    }
#endif
  }
#if ENABLE_TRACE_TIMER == 1
  std::sort(insert_50.begin(), insert_50.end());
  std::sort(insert_90.begin(), insert_90.end());
  std::sort(insert_99.begin(), insert_99.end());
  std::sort(insert_999.begin(), insert_999.end());
  std::sort(insert_max.begin(), insert_max.end());
  std::sort(find_50.begin(), find_50.end());
  std::sort(find_90.begin(), find_90.end());
  std::sort(find_99.begin(), find_99.end());
  std::sort(find_999.begin(), find_999.end());
  std::sort(find_max.begin(), find_max.end());
  printf("insert percentiles: 50th %lu, 90th %lu, 99th %lu, 99.9th %lu, max %lu\n", insert_50[num_trials / 2], insert_90[num_trials / 2], insert_99[num_trials / 2], insert_999[num_trials / 2], insert_max[num_trials / 2]);

  printf("find percentiles: 50th %lu, 90th %lu, 99th %lu, 99.9th %lu, max %lu\n", find_50[num_trials / 2], find_90[num_trials / 2], find_99[num_trials / 2], find_999[num_trials / 2], find_max[num_trials / 2]);
#endif

  std::sort(total_insert_times.begin(), total_insert_times.end());
  std::sort(total_find_times.begin(), total_find_times.end());
  printf("total insert time = %lu, find time = %lu\n", total_insert_times[num_trials / 2], total_find_times[num_trials / 2]);
}

/*
template <class T, size_t MAX_KEYS> void test_ordered_insert(uint64_t max_size, int p) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  uint64_t start, end;
  
  BSkip<traits> s(p);
  
  start = get_usecs();
  for (uint32_t i = 1; i < max_size; i++) {
    s.insert(i);
  }
  end = get_usecs();
  printf("\ninsertion,\t %lu,", end - start);

  start = get_usecs();
  for (uint32_t i = 1; i < max_size; i++) {
    auto node = s.find(i);
    if (node == NULL) {
      printf("\ncouldn't find key %lu in skip at index %u\n", i, i);
      exit(0);
    }
  }
  end = get_usecs();
  printf("\nfind all,\t %lu,", end - start);

  start = get_usecs();
  uint64_t sum = s.sum();
  end = get_usecs();

  printf("\nsum_time, %lu, sum_total, %lu\n", end - start, sum);
	printf("\n");
}

template <class T, size_t MAX_KEYS>
void test_unordered_insert_then_find(uint64_t max_size, int p) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());
  printf("finished creating data\n");

  uint64_t start, end, concurrent_insert_time;

  uint32_t num_trials = 5;
  std::vector<uint64_t> insert_times;
  std::vector<uint64_t> sum_times;
  std::vector<uint64_t> find_times;

  BSkip<T, MAX_KEYS, false> s(p);
  // insert all elts serially
  for(uint32_t i = 0; i < max_size; i++) {
    s.insert(data[i]);
  }
  // serial find
  std::vector<int> found_count_serial(max_size);
  start = get_usecs();
  for (uint32_t i = 0; i < max_size; i++) {
    found_count_serial[i] = s.exists(data[i]);
  }
  end = get_usecs();
  int num_found = 0;
  for (auto e : found_count_serial) { 
    num_found += e;
  }
  printf("\nfind all serial,\t %lu, num_found = %d\n", end - start, num_found);

  // do parallel find trials
  for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
#if ENABLE_TRACE_TIMER
    ParallelTools::Reducer_sum<uint64_t> total_insert;
    ParallelTools::Reducer_sum<uint64_t> total_lock;
#endif

    BSkip<T, MAX_KEYS, true> s_concurrent(p);
    // insert all elts concurrently
    start = get_usecs();
    ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
      timer insert_timer("insert_timer");
      insert_timer.start();
      auto result = s_concurrent.insert(data[i]);
      insert_timer.stop();
#if ENABLE_TRACE_TIMER
      total_insert.add(insert_timer.get_elapsed_time());
      total_lock.add(result);
#endif
    });
    end = get_usecs();

#if ENABLE_TRACE_TIMER
    printf("total concurrent insert time = %lu, locking time = %lu, percent time = %f\n", total_insert.get(), total_lock.get(), ((double)total_lock.get() /(double) total_insert.get())*100);
#endif
    concurrent_insert_time = end - start;
    if (trial > 0) {
      insert_times.push_back(concurrent_insert_time);
    }

      // parallel find
    std::vector<int> found_count_parallel(max_size);
    start = get_usecs();
    ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
      found_count_parallel[i] = s_concurrent.exists(data[i]);
    });
    
    end = get_usecs();
    uint64_t parallel_find_time = end - start;
    for(auto e : found_count_parallel) { num_found += e; }
    printf("\ttrial %u, found count = %d\n", trial, num_found);
    assert(num_found == max_size);
    if (trial > 0) {
      find_times.push_back(parallel_find_time);
    }

#if DEBUG
    start = get_usecs();
    uint64_t psum = s_concurrent.psum();
    end = get_usecs();

    printf("\tpsum_time, %lu, psum_total, %lu\n", end - start, psum);
    uint64_t psum_time = end - start;
    if (trial > 0) {
      sum_times.push_back(psum_time);
    }
      std::set<T> inserted_data;
      for(uint64_t i = 0; i < max_size; i++) {
        inserted_data.insert(data[i]);
      }

      uint64_t correct_sum = 0;
  #if DEBUG_PRINT
      printf("*** CORRECT SET ***\n");
  #endif
      for(auto e : inserted_data) {

  #if DEBUG_PRINT
        printf("%lu\n", e);
  #endif
        correct_sum += e;
      }

      tbassert(correct_sum == psum, "got sum %lu, should be %lu\n", psum, correct_sum);
    
  #endif
  }
  std::sort(insert_times.begin(), insert_times.end());
  std::sort(find_times.begin(), find_times.end());
  concurrent_insert_time = insert_times[num_trials / 2];
  auto concurrent_find_time = find_times[num_trials / 2];
  printf("concurrent insert time = %lu, find time = %lu\n", concurrent_insert_time, concurrent_find_time);
}

template <class T, size_t MAX_KEYS>
void test_unordered_insert_and_find(uint64_t max_size, int p) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());
  printf("finished creating data\n");

  uint64_t start, end, concurrent_insert_time;

  uint32_t seed_offset = 100;
  std::vector<T> data_to_add = create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max(), seed_offset);

  uint32_t num_trials = 5;
  std::vector<uint64_t> insert_times;
  std::vector<uint64_t> find_times;

  for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
#if ENABLE_TRACE_TIMER
    ParallelTools::Reducer_sum<uint64_t> total_insert;
    ParallelTools::Reducer_sum<uint64_t> total_lock;
#endif

    BSkip<T, MAX_KEYS, true> s_concurrent(p);
    // insert all elts concurrently
    start = get_usecs();
    ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
      timer insert_timer("insert_timer");
      insert_timer.start();
      auto result = s_concurrent.insert(data[i]);
      insert_timer.stop();
#if ENABLE_TRACE_TIMER
      total_insert.add(insert_timer.get_elapsed_time());
      total_lock.add(result);
#endif
    });
    end = get_usecs();

#if ENABLE_TRACE_TIMER
    printf("total concurrent insert time = %lu, locking time = %lu, percent time = %f\n", total_insert.get(), total_lock.get(), ((double)total_lock.get() /(double) total_insert.get())*100);
#endif
    concurrent_insert_time = end - start;
    // printf("\n\tconcurrent insertion time %lu\n", concurrent_insert_time);
    if (trial > 0) {
      insert_times.push_back(concurrent_insert_time);
    }

    // parallel find
    std::vector<int> found_count_parallel(max_size);
    uint32_t percent_finds = 5;
    start = get_usecs();
    ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
      if (i % 100 < percent_finds) {
        auto node = s_concurrent.find(data[i]);
        found_count_parallel[i] = node ? 1 : 0;     
        if (node == nullptr) {
          printf("\ncouldn't find key %lu in skip at index %lu\n", data[i], i);
          exit(0);
        }
      } else {
        s_concurrent.insert(data_to_add[i]);
      }
    });
    
    end = get_usecs();
    uint64_t parallel_find_time = end - start;
    int num_found = 0;
    for(auto e : found_count_parallel) { num_found += e; }
    printf("\ttrial %u, found count = %d\n", trial, num_found);
    assert(num_found == max_size);
    if (trial > 0) {
      find_times.push_back(parallel_find_time);
    }
  }
  std::sort(insert_times.begin(), insert_times.end());
  std::sort(find_times.begin(), find_times.end());
  concurrent_insert_time = insert_times[num_trials / 2];
  auto concurrent_find_time = find_times[num_trials / 2];
  printf("concurrent insert time = %lu, mixed time = %lu\n", concurrent_insert_time, concurrent_find_time);
}
*/

template <class T, size_t node_size_in_bytes>
void test_serial_correctness(uint64_t max_size) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  // for debugging
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());
  printf("finished creating data, max size %lu\n", max_size);

  // num keys that can fit into the node
  const int node_size = node_size_in_bytes / (2*sizeof(T));
  // const int p = (int)(sqrt(node_size)); 
	const int p = node_size;

  printf("node size in bytes %lu, max keys %lu, p = %d\n", node_size_in_bytes, node_size, p);
  uint64_t start, end, concurrent_insert_time, serial_insert_time;
  //test with val
  using serial_traits = BSkip_traits<false, node_size, p, T, T>;

  uint64_t ref_sum = 0;
  std::set<T> inserted_data;

  // serial inserts
  BSkip<serial_traits> s;
  for (uint32_t i = 0; i < max_size; i++) {
		// printf("inserting %lu\n", data[i]);
    s.insert({data[i], data[i]});
    inserted_data.insert(data[i]);
    assert(s.exists(data[i]));
		// printf("found %lu\n", data[i]);
  }


  for(auto e : inserted_data) {
	  ref_sum += e;
  }
  auto sum = s.sum();
	auto val_sum = s.sum_vals();

  tbassert(sum == ref_sum, "got key sum %lu, should be %lu\n", sum, ref_sum);
  tbassert(val_sum == ref_sum, "got val sum %lu, should be %lu\n", val_sum, ref_sum);
}

template <class T, size_t node_size_in_bytes>
void test_parallel_correctness(uint64_t max_size) {

	if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }

  // for debugging
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());
  printf("finished creating data, max size %lu\n", max_size);

  // num keys that can fit into the node
  const int node_size = node_size_in_bytes / (2*sizeof(T));
  // const int p = (int)(sqrt(node_size));
  const int p = node_size;

  printf("max keys %lu, p = %d\n", node_size, p);
  uint64_t start, end, concurrent_insert_time, serial_insert_time;
  //test with val
  using parallel_traits = BSkip_traits<true, node_size, p, T, T>;

  uint64_t ref_sum = 0;
  std::set<T> inserted_data;

  BSkip<parallel_traits> s;
  printf("in parallel, insert %lu elts\n", max_size);
  parallel_for(0, max_size, [&](uint64_t i) {
    s.insert({data[i], data[i]});
  });
  parallel_for(0, max_size, [&](uint64_t i) {
    assert(s.exists(data[i]));
  });

	printf("inserting into serial baseline\n");
  for (uint32_t i = 0; i < max_size; i++) {
    inserted_data.insert(data[i]);
  }

  for(auto e : inserted_data) {
	  ref_sum += e;
  }
  auto sum = s.sum();
  auto val_sum = s.sum_vals();

  tbassert(sum == ref_sum, "got key sum %lu, should be %lu\n", sum, ref_sum);
  tbassert(val_sum == ref_sum, "got val sum %lu, should be %lu\n", val_sum, ref_sum);
}

// run microbenchmarks
// first add n (untimed), then start timer and add another n items
// find for k
// k short ranges, k long ranges (
template <class T, size_t node_size_in_bytes>
void test_concurrent_microbenchmarks_map(uint64_t max_size, uint64_t NUM_QUERIES, std::seed_seq &seed, bool write_csv) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  // for debugging
  std::vector<T> data =
      create_random_data_in_parallel<T>(2 * max_size, std::numeric_limits<T>::max());
  printf("finished creating data, max size %lu\n", max_size);

  // num elts that can fit into the node
  const int node_size = node_size_in_bytes / (2*sizeof(T));
  // const int p = (int)(sqrt(node_size)); 
  const int p = (int)((node_size)); 

  uint64_t start_time, end_time;
  std::vector<uint64_t> insert_times;
  std::vector<uint64_t> find_times;
  std::vector<uint64_t> sorted_range_query_times_by_size;
  std::vector<uint64_t> unsorted_range_query_times_by_size;

  printf("max keys %lu, p = %d\n", node_size, p);
  uint64_t start, end, concurrent_insert_time, serial_insert_time;
  using serial_traits = BSkip_traits<false, node_size, p, T>;

#if RUN_SERIAL
  // serial inserts
  BSkip<serial_traits> s;
  start = get_usecs();
  for (uint32_t i = 0; i < max_size; i++) {
    s.insert(data[i]);
  }
  end = get_usecs();
  serial_insert_time = end - start;
  printf("\nserial insertion time %lu \n", end - start);
#endif

  using parallel_traits = BSkip_traits<true, node_size, p, T, T>;
  uint32_t num_trials = 5;

  for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
#if ENABLE_TRACE_TIMER
    ParallelTools::Reducer_sum<uint64_t> total_insert;
    ParallelTools::Reducer_sum<uint64_t> total_lock;
#endif

    BSkip<parallel_traits> s_concurrent;
    timer insert_timer("insert_timer");

		printf("start add first half\n");
		// add first half without timing
    parallel_for(0, max_size, [&](uint64_t i) {
      auto result = s_concurrent.insert({data[i], 2*data[i]});
#if ENABLE_TRACE_TIMER
      total_insert.add(insert_timer.get_elapsed_time());
      total_lock.add(result);
#endif
    });
		printf("finish add first half \n");
		printf("start add second half\n");
    start = get_usecs();
		// add second half with timing
    insert_timer.start();
    parallel_for(max_size, 2*max_size, [&](uint64_t i) {
      auto result = s_concurrent.insert({data[i], 2*data[i]});
#if ENABLE_TRACE_TIMER
      total_insert.add(insert_timer.get_elapsed_time());
      total_lock.add(result);
#endif
    });
    insert_timer.stop();
    end = get_usecs();

#if ENABLE_TRACE_TIMER
    printf("total concurrent insert time = %lu, locking time = %lu, percent time = %f\n", total_insert.get(), total_lock.get(), ((double)total_lock.get() /(double) total_insert.get())*100);
#endif
    concurrent_insert_time = end - start;
    printf("finish add second half in %lu\n", concurrent_insert_time);
		if (trial > 0) {
      insert_times.push_back(concurrent_insert_time);
    }    

    std::seed_seq query_seed{1};
    std::seed_seq query_seed_2{2};

    std::vector<uint64_t> range_query_start_idxs =
        create_random_data<uint64_t>(NUM_QUERIES, data.size() - 1, query_seed);

    // TIME POINT QUERIES
    std::vector<bool> found_count(NUM_QUERIES);
    start_time = get_usecs();
    parallel_for(0, NUM_QUERIES, [&](const uint32_t &i) {
      found_count[i] = s_concurrent.exists(data[range_query_start_idxs[i]]);
    });
    end_time = get_usecs();
    
		if (trial > 0) { find_times.push_back(end_time - start_time); }
    int count_found = 0;
    for (auto e : found_count) {
      count_found += e ? 1 : 0;
    }

    printf("\tDone finding %lu elts in %lu, count = %d \n",NUM_QUERIES, end_time - start_time, count_found);
	}

	// TODO: implement map range
  /*
  std::vector<uint32_t> num_query_sizes{100, 100000};

	// TIME RANGE QUERIES FOR ALL LENGTHS
	for (size_t query_size_i = 0; query_size_i < num_query_sizes.size(); query_size_i++) {
		uint64_t MAX_QUERY_SIZE = num_query_sizes[query_size_i];
		std::vector<uint64_t> range_query_lengths =
		create_random_data<uint64_t>(NUM_QUERIES, MAX_QUERY_SIZE - 1, query_seed_2);

				// sorted query first to get end key
				std::vector<T> concurrent_range_query_length_maxs(NUM_QUERIES);
				std::vector<uint64_t> concurrent_range_query_length_counts(NUM_QUERIES);
				std::vector<T> concurrent_range_query_length_key_sums(NUM_QUERIES);
				std::vector<T> concurrent_range_query_length_val_sums(NUM_QUERIES);

    parallel_for(0, NUM_QUERIES, [&](const uint32_t &i) {
        T start;
	start = data[range_query_start_idxs[i]];
*/
	std::sort(insert_times.begin(), insert_times.end());
	std::sort(find_times.begin(), find_times.end());

	auto med_insert_time = insert_times[insert_times.size() / 2];
	auto med_find_time = find_times[find_times.size() / 2];
    if(write_csv){
	    std::ofstream outfile; 
	    outfile.open("insert_finds.csv", std::ios_base::app);
	    outfile << node_size_in_bytes << ", " << p << ", " <<  max_size << ", " << med_insert_time << ", " << NUM_QUERIES << ", " << med_find_time << "\n";
	    outfile.close();
  	}

  printf("concurrent insert time %lu\n", concurrent_insert_time);
}

template <class T, size_t node_size_in_bytes>
void test_unordered_insert(uint64_t max_size,  bool write_csv) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  // for debugging
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());
  printf("finished creating data, max size %lu\n", max_size);

  // num keys that can fit into the node
  const int node_size = node_size_in_bytes / sizeof(T);
  const int p = (int)(sqrt(node_size)); 

  printf("max keys %lu, p = %d\n", node_size, p);
  uint64_t start, end, concurrent_insert_time, serial_insert_time;
  using serial_traits = BSkip_traits<false, node_size, p, T>;

#if RUN_SERIAL
  // serial inserts
  BSkip<serial_traits> s;
  start = get_usecs();
  for (uint32_t i = 0; i < max_size; i++) {
    s.insert(data[i]);
  }
  end = get_usecs();
  serial_insert_time = end - start;
  printf("\nserial insertion time %lu \n", end - start);
#endif

  using parallel_traits = BSkip_traits<true, node_size, p, T>;
  uint32_t num_trials = 5;
  std::vector<uint64_t> insert_times;
  std::vector<uint64_t> sum_times;

  for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
#if ENABLE_TRACE_TIMER
    ParallelTools::Reducer_sum<uint64_t> total_insert;
    ParallelTools::Reducer_sum<uint64_t> total_lock;
#endif

    BSkip<parallel_traits> s_concurrent;
    start = get_usecs();
    timer insert_timer("insert_timer");
    insert_timer.start();

    parallel_for(0, max_size, [&](uint64_t i) {
    // ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
      auto result = s_concurrent.insert(data[i]);
#if ENABLE_TRACE_TIMER
      total_insert.add(insert_timer.get_elapsed_time());
      total_lock.add(result);
#endif
    });

    insert_timer.stop();
    end = get_usecs();

#if ENABLE_TRACE_TIMER
    printf("total concurrent insert time = %lu, locking time = %lu, percent time = %f\n", total_insert.get(), total_lock.get(), ((double)total_lock.get() /(double) total_insert.get())*100);
#endif
    concurrent_insert_time = end - start;
    // printf("\n\tconcurrent insertion time %lu\n", concurrent_insert_time);
    if (trial > 0) {
      insert_times.push_back(concurrent_insert_time);
    }    

    start = get_usecs();
    uint64_t concurrent_sum = s_concurrent.sum();
    end = get_usecs();
   
    printf("\tconcurrent sum time = %lu, concurrent_sum_total = %lu\n", end - start, concurrent_sum);

    start = get_usecs();
    uint64_t psum = s_concurrent.psum();
    end = get_usecs();

    printf("\tpsum_time, %lu, psum_total, %lu\n", end - start, psum);
    uint64_t psum_time = end - start;
    if (trial > 0) {
      sum_times.push_back(psum_time);
    }
  #if DEBUG
      std::set<T> inserted_data;
      for(uint64_t i = 0; i < max_size; i++) {
        inserted_data.insert(data[i]);
      }

      uint64_t correct_sum = 0;
  #if DEBUG_PRINT
      printf("*** CORRECT SET ***\n");
  #endif
      for(auto e : inserted_data) {

  #if DEBUG_PRINT
        printf("%lu\n", e);
  #endif
        correct_sum += e;
      }

      tbassert(correct_sum == psum, "got sum %lu, should be %lu\n", psum, correct_sum);
  #endif
  }
  std::sort(insert_times.begin(), insert_times.end());
  concurrent_insert_time = insert_times[num_trials / 2];

  if(write_csv){
    std::ofstream outfile; 
    outfile.open("insert_finds.csv", std::ios_base::app);
    outfile << "plain, " << node_size_in_bytes << ", " << p << ", " <<  max_size << ", " << concurrent_insert_time << "\n";
    outfile.close();
  }

  printf("concurrent insert time %lu\n", concurrent_insert_time);
}

/*
// start x add y all uniform
template <class T, size_t MAX_KEYS>
void test_unordered_insert_from_base(uint64_t max_size, int p) {
  printf("*** ADD FROM BASE WITH %lu ELEMENTS ***\n", max_size);

  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  uint32_t seed_offset = 100;
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());


  uint64_t start, end, concurrent_insert_time;

  std::vector<T> data_to_add = create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max(), seed_offset);

  printf("finished creating data\n");
#if RUN_SERIAL
  // add all sizes up until max_size
  for(uint32_t num_to_add = 1; num_to_add <= max_size; num_to_add *= 10) {
    // first start with serial version
    BSkip<T, MAX_KEYS, false> s(p);
    start = get_usecs();
    for (uint32_t i = 0; i < max_size; i++) {
      s.insert(data[i]);
    }
    end = get_usecs();
    printf("\nserially inserted %lu elts in %lu \n", max_size, end - start);
    printf("\tadd %u other elts\n", num_to_add);
    
    
    // add other stuff
    start = get_usecs();
    for(uint32_t i = 0; i < num_to_add; i++) {
      s.insert(data_to_add[i]);
    }
    end = get_usecs();
    printf("num to add serial %u in time %lu\n", num_to_add, end - start);
  }
#endif

  // do 5 trials of parallel version
  uint32_t num_trials = 5;
  for(uint32_t num_to_add = 1; num_to_add <= max_size; num_to_add *= 10) {
    std::vector<uint64_t> insert_times;
    std::vector<uint64_t> sum_times;

#if ENABLE_TRACE_TIMER
    std::vector<uint64_t> read_lock_counts;
    std::vector<uint64_t> write_lock_counts;
    std::vector<uint64_t> leaf_lock_counts;
#endif
    for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
      BSkip<T, MAX_KEYS, true> s_concurrent(p);

      start = get_usecs();
      ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
        s_concurrent.insert(data[i]);
      });
      end = get_usecs();

      printf("\n\nconcurrently added %lu elts in %lu\n", max_size, end - start);
#if ENABLE_TRACE_TIMER
      s_concurrent.get_lock_counts();
      s_concurrent.reset_lock_counts(); 
#endif
      
      printf("\tconcurrent add %u other elts\n", num_to_add);
      start = get_usecs();
      ParallelTools::parallel_for(0, num_to_add, [&](uint64_t i) {
        s_concurrent.insert(data_to_add[i]);
      });
      end = get_usecs();

      concurrent_insert_time = end - start;
      printf("\tadded extra elts in %lu\n", end - start);
      if (trial > 0) {
        insert_times.push_back(concurrent_insert_time);
#if ENABLE_TRACE_TIMER
        read_lock_counts.push_back( s_concurrent.get_read_lock_count() );
        write_lock_counts.push_back( s_concurrent.get_write_lock_count() );
        leaf_lock_counts.push_back( s_concurrent.get_leaf_lock_count() );
#endif
       }    

      start = get_usecs();
      uint64_t psum = s_concurrent.psum();
      end = get_usecs();

      printf("\tpsum_time, %lu, psum_total, %lu\n", end - start, psum);
      uint64_t psum_time = end - start;
      if (trial > 0) {
        sum_times.push_back(psum_time);
      }
    #if DEBUG
        std::set<T> inserted_data;
        for(uint64_t i = 0; i < max_size; i++) {
          inserted_data.insert(data[i]);
        }

        uint64_t correct_sum = 0;
    #if DEBUG_PRINT
        printf("*** CORRECT SET ***\n");
    #endif
        for(auto e : inserted_data) {

    #if DEBUG_PRINT
          printf("%lu\n", e);
    #endif
          correct_sum += e;
        }

        tbassert(correct_sum == psum, "got sum %lu, should be %lu\n", psum, correct_sum);
    #endif
    }
    std::sort(insert_times.begin(), insert_times.end());
    concurrent_insert_time = insert_times[num_trials / 2];
    printf("num to add concurrent %u in time %lu\n", num_to_add, concurrent_insert_time);

#if ENABLE_TRACE_TIMER
    std::sort(read_lock_counts.begin(), read_lock_counts.end());
    std::sort(write_lock_counts.begin(), write_lock_counts.end());
    std::sort(leaf_lock_counts.begin(), leaf_lock_counts.end());
    auto read_lock_count = read_lock_counts[num_trials / 2];
    auto write_lock_count = write_lock_counts[num_trials / 2];
    auto leaf_lock_count = leaf_lock_counts[num_trials / 2];
    printf("median read lock count = %lu, write lock count = %lu, leaf lock count = %lu\n", read_lock_count, write_lock_count, leaf_lock_count);
#endif
  }
}

// start x add y
// start = unif, add = zipf
template <class T, size_t MAX_KEYS>
void test_unordered_insert_from_base_zipf(uint64_t max_size, int p, char* filename) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  // for debugging
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, 1UL << 32); //std::numeric_limits<T>::max());

  printf("finished creating data\n");
  uint64_t start, end, concurrent_insert_time;

  printf("start reading file\n");
  // read in file
  std::vector<T> data_to_add;
  std::ifstream input_file(filename);

  // read in data from file
  uint32_t temp;
  for( std::string line; getline( input_file, line ); ) {
    std::istringstream iss(line);
    if(!(iss >> temp)) { break; }
    data_to_add.push_back(temp);
  }
  std::cout << filename << std::endl;
  printf("finished reading file, %lu elts\n", data_to_add.size());

  // add all sizes up until max_size
  for(uint32_t num_to_add = 1; num_to_add < max_size; num_to_add *= 10) {
    // first start with serial version
    BSkip<T, MAX_KEYS, false> s(p);
    start = get_usecs();
    for (uint32_t i = 0; i < max_size; i++) {
      s.insert(data[i]);
    }
    end = get_usecs();
    printf("\nserially inserted %lu elts in %lu \n", max_size, end - start);
    printf("\tadd %u other elts\n", num_to_add);
    
    // add other stuff
    assert(num_to_add <= data_to_add.size());
    start = get_usecs();
    for(uint32_t i = 0; i < num_to_add; i++) {
      s.insert(data_to_add[i]);
    }
    end = get_usecs();
    printf("num to add serial %u in time %lu\n", num_to_add, end - start);
  }

  // do 5 trials of parallel version
  uint32_t num_trials = 5;
  for(uint32_t num_to_add = 1; num_to_add < max_size; num_to_add *= 10) {
    std::vector<uint64_t> insert_times;
    std::vector<uint64_t> sum_times;
    for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
      BSkip<T, MAX_KEYS, true> s_concurrent(p);

      start = get_usecs();
      ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
        s_concurrent.insert(data[i]);
      });
      end = get_usecs();

      printf("\n\nconcurrently added %lu elts in %lu\n", max_size, end - start);
      printf("\tconcurrent add %u other elts\n", num_to_add);
      start = get_usecs();
      ParallelTools::parallel_for(0, num_to_add, [&](uint64_t i) {
        s_concurrent.insert(data_to_add[i]);
      });
      end = get_usecs();

      concurrent_insert_time = end - start;
      printf("\tadded extra elts in %lu\n", end - start);
      if (trial > 0) {
        insert_times.push_back(concurrent_insert_time);
       }    
 
      start = get_usecs();
      uint64_t concurrent_sum = s_concurrent.sum();
      end = get_usecs();
     
      printf("\tconcurrent sum time = %lu, concurrent_sum_total = %lu\n", end - start, concurrent_sum);

      start = get_usecs();
      uint64_t psum = s_concurrent.psum();
      end = get_usecs();

      printf("\tpsum_time, %lu, psum_total, %lu\n", end - start, psum);
      uint64_t psum_time = end - start;
      if (trial > 0) {
        sum_times.push_back(psum_time);
      }
    #if DEBUG
        std::set<T> inserted_data;
        for(uint64_t i = 0; i < max_size; i++) {
          inserted_data.insert(data[i]);
        }

        uint64_t correct_sum = 0;
    #if DEBUG_PRINT
        printf("*** CORRECT SET ***\n");
    #endif
        for(auto e : inserted_data) {

    #if DEBUG_PRINT
          printf("%lu\n", e);
    #endif
          correct_sum += e;
        }

        tbassert(correct_sum == psum, "got sum %lu, should be %lu\n", psum, correct_sum);
      
    #endif
    }
    std::sort(insert_times.begin(), insert_times.end());
    concurrent_insert_time = insert_times[num_trials / 2];
    printf("num to add concurrent %u in time %lu\n", num_to_add, concurrent_insert_time);
  }
}

// start x add y
// start = unif, add = 1..n
template <class T, size_t MAX_KEYS>
void test_unordered_insert_from_base_start(uint64_t max_size, int p) {
  if (max_size > std::numeric_limits<T>::max()) {
    max_size = std::numeric_limits<T>::max();
  }
  uint32_t seed_offset = 100;
  // for debugging
  std::vector<T> data =
      create_random_data_in_parallel<T>(max_size, std::numeric_limits<T>::max());

  printf("finished creating data\n");
  uint64_t start, end, concurrent_insert_time;

  // read in file
  std::vector<T> data_to_add;

  for(uint64_t i = 1; i <= max_size; i++) {
    data_to_add.push_back(i);
  }

  auto rng = std::default_random_engine {};
  std::shuffle(std::begin(data_to_add), std::end(data_to_add), rng);

#if RUN_SERIAL
  uint64_t serial_insert_time;
  // add all sizes up until max_size
  for(uint32_t num_to_add = 1; num_to_add <= max_size; num_to_add *= 10) {
    // first start with serial version
    BSkip<T, MAX_KEYS, false> s(p);
    start = get_usecs();
    for (uint32_t i = 0; i < max_size; i++) {
      s.insert(data[i]);
    }
    end = get_usecs();
    serial_insert_time = end - start;
    printf("\nserially inserted %lu elts in %lu \n", max_size, end - start);
    printf("\tadd %u other elts\n", num_to_add);
    
    // add other stuff
    assert(num_to_add <= data_to_add.size());
    start = get_usecs();
    for(uint32_t i = 0; i < num_to_add; i++) {
      s.insert(data_to_add[i]);
    }
    end = get_usecs();
    printf("num to add serial %u in time %lu\n", num_to_add, end - start);
  }
#endif

  // do 5 trials of parallel version
  uint32_t num_trials = 5;
  for(uint32_t num_to_add = 1; num_to_add <= max_size; num_to_add *= 10) {
    std::vector<uint64_t> insert_times;
    std::vector<uint64_t> sum_times;

#if ENABLE_TRACE_TIMER
    std::vector<uint64_t> read_lock_counts;
    std::vector<uint64_t> write_lock_counts;
    std::vector<uint64_t> leaf_lock_counts;
#endif

    for(uint32_t trial = 0; trial < num_trials + 1; trial++) {
      BSkip<T, MAX_KEYS, true> s_concurrent(p);

      start = get_usecs();
      ParallelTools::parallel_for(0, max_size, [&](uint64_t i) {
        s_concurrent.insert(data[i]);
      });
      end = get_usecs();

      printf("\n\nconcurrently added %lu elts in %lu\n", max_size, end - start);
      printf("\tconcurrent add %u other elts\n", num_to_add);
#if ENABLE_TRACE_TIMER
      s_concurrent.get_lock_counts();
      s_concurrent.reset_lock_counts(); 
#endif

      start = get_usecs();

      ParallelTools::parallel_for(0, num_to_add, [&](uint64_t i) {
        s_concurrent.insert(data_to_add[i]);
      });
      end = get_usecs();

      concurrent_insert_time = end - start;
      printf("\tadded extra elts in %lu\n", end - start);
      if (trial > 0) {
        insert_times.push_back(concurrent_insert_time);
#if ENABLE_TRACE_TIMER
        read_lock_counts.push_back( s_concurrent.get_read_lock_count() );
        write_lock_counts.push_back( s_concurrent.get_write_lock_count() );
        leaf_lock_counts.push_back( s_concurrent.get_leaf_lock_count() );
#endif
      }

      start = get_usecs();
      uint64_t psum = s_concurrent.psum();
      end = get_usecs();

      printf("\tpsum_time, %lu, psum_total, %lu\n", end - start, psum);
      uint64_t psum_time = end - start;
      if (trial > 0) {
        sum_times.push_back(psum_time);
      }
    #if DEBUG
        std::set<T> inserted_data;
        for(uint64_t i = 0; i < max_size; i++) {
          inserted_data.insert(data[i]);
        }

        uint64_t correct_sum = 0;
    #if DEBUG_PRINT
        printf("*** CORRECT SET ***\n");
    #endif
        for(auto e : inserted_data) {

    #if DEBUG_PRINT
          printf("%lu\n", e);
    #endif
          correct_sum += e;
        }

        tbassert(correct_sum == psum, "got sum %lu, should be %lu\n", psum, correct_sum);
    #endif
    }
    std::sort(insert_times.begin(), insert_times.end());
    concurrent_insert_time = insert_times[num_trials / 2];
    printf("num to add concurrent %u in time %lu\n", num_to_add, concurrent_insert_time);
#if ENABLE_TRACE_TIMER
    std::sort(read_lock_counts.begin(), read_lock_counts.end());
    std::sort(write_lock_counts.begin(), write_lock_counts.end());
    std::sort(leaf_lock_counts.begin(), leaf_lock_counts.end());
    auto read_lock_count = read_lock_counts[num_trials / 2];
    auto write_lock_count = write_lock_counts[num_trials / 2];
    auto leaf_lock_count = leaf_lock_counts[num_trials / 2];
    printf("median read lock count = %lu, write lock count = %lu, leaf lock count = %lu\n", read_lock_count, write_lock_count, leaf_lock_count);
#endif
  }
}
*/


int main(int argc, char** argv) {
  cxxopts::Options options("BtreeTester",
                           "allows testing different attributes of the btree");

  options.positional_help("Help Text");

  // clang-format off
  options.add_options()
    ("trials", "num trials", cxxopts::value<int>()->default_value( "5"))
    ("num_inserts", "number of values to insert", cxxopts::value<int>()->default_value( "1000000"))
    ("num_queries", "number of queries for query tests", cxxopts::value<int>()->default_value( "10000"))
    ("query_size", "query size for cache test", cxxopts::value<int>()->default_value( "100"))
    ("write_csv", "whether to write timings to disk")
    ("node_size", "setting of B", cxxopts::value<int>()->default_value( "1024"))
    ("microbenchmark_baseline", "run baseline 1024 byte bskip microbenchmark with [trials] [num_inserts] [num_queries] [write_csv]")
    ("test_latencies", "run 1024 byte bskip inserts with measuring latencies [trials] [num_inserts] [write_csv]")
    ("serial_correctness", "run 1024 byte bskip inserts serially with finds after each one")
    ("parallel_correctness", "run 1024 byte bskip inserts serially with finds after each one")
    ("microbenchmark_allsize", "run all size bskip microbenchmark with [trials] [num_inserts] [num_queries] [write_csv]");

  std::seed_seq seed{0};
  auto result = options.parse(argc, argv);
  uint32_t trials = result["trials"].as<int>();
  uint32_t num_inserts = result["num_inserts"].as<int>();
  uint32_t num_queries = result["num_queries"].as<int>();
  uint32_t query_size = result["query_size"].as<int>();
  uint32_t write_csv = result["write_csv"].as<bool>();

  std::ofstream outfile;
  outfile.open("insert_finds.csv", std::ios_base::app);
  outfile << "bytes per node, p, num_inserted, insert_time, num_finds, find_time, \n";
  outfile.close();
  outfile.open("range_queries.csv", std::ios_base::app);
  outfile << "tree_type, internal bytes, leaf bytes, num_inserted,num_range_queries, max_query_size,  unsorted_query_time, sorted_query_time, \n";
  outfile.close();

  if (result["test_latencies"].as<bool>()) {
		const int node_size = 1024;
		const int p = (int)(sqrt(node_size)); 
    test_latencies<uint64_t, node_size, p>(num_inserts, write_csv);
  }

  if (result["microbenchmark_baseline"].as<bool>()) {
		const int node_size_in_bytes = 1024; // in bytes		
		test_concurrent_microbenchmarks_map<uint64_t, node_size_in_bytes>(num_inserts, num_queries, seed, write_csv);
		// void test_concurrent_microbenchmarks_map(uint64_t max_size, uint64_t NUM_QUERIES, std::seed_seq &seed, bool write_csv)
    // test_unordered_insert<uint64_t, node_size_in_bytes>(num_inserts, write_csv);
  }

	if (result["microbenchmark_allsize"].as<bool>()) {
 		// const int size_64 = 64; // in bytes		
 		// test_concurrent_microbenchmarks_map<uint64_t, size_64>(num_inserts, num_queries, seed, write_csv);

 		// const int size_128 = 128; // in bytes		
 		// test_concurrent_microbenchmarks_map<uint64_t, size_128>(num_inserts, num_queries, seed, write_csv);

 		const int size_256 = 256; // in bytes		
 		test_concurrent_microbenchmarks_map<uint64_t, size_256>(num_inserts, num_queries, seed, write_csv);

		const int size_512 = 512; // in bytes		
		test_concurrent_microbenchmarks_map<uint64_t, size_512>(num_inserts, num_queries, seed, write_csv);

		const int size_1024 = 1024; // in bytes		
		test_concurrent_microbenchmarks_map<uint64_t, size_1024>(num_inserts, num_queries, seed, write_csv);

		const int size_2048 = 2048; // in bytes		
		test_concurrent_microbenchmarks_map<uint64_t, size_2048>(num_inserts, num_queries, seed, write_csv);

		const int size_4096 = 4096; // in bytes		
		test_concurrent_microbenchmarks_map<uint64_t, size_4096>(num_inserts, num_queries, seed, write_csv);

		// const int size_8192 = 8192; // in bytes		
		// test_concurrent_microbenchmarks_map<uint64_t, size_8192>(num_inserts, num_queries, seed, write_csv);

		// void test_concurrent_microbenchmarks_map(uint64_t max_size, uint64_t NUM_QUERIES, std::seed_seq &seed, bool write_csv)
    // test_unordered_insert<uint64_t, node_size_in_bytes>(num_inserts, write_csv);
  }


  test_serial_correctness<uint64_t, 256>(1024);
  test_parallel_correctness<uint64_t, 256>(1024);
  

  if (result["serial_correctness"].as<bool>()) {
 		const int size_256 = 256; // in bytes		
    test_serial_correctness<uint64_t, size_256>(num_inserts);

		const int size_512 = 512; // in bytes		
    test_serial_correctness<uint64_t, size_512>(num_inserts);

		const int size_1024 = 1024; // in bytes		
    test_serial_correctness<uint64_t, size_1024>(num_inserts);

		const int size_2048 = 2048; // in bytes		
    test_serial_correctness<uint64_t, size_2048>(num_inserts);

		const int size_4096 = 4096; // in bytes		
    test_serial_correctness<uint64_t, size_4096>(num_inserts);

		const int size_8192 = 8192; // in bytes		
    test_serial_correctness<uint64_t, size_8192>(num_inserts);

  }

  if (result["parallel_correctness"].as<bool>()) {
 		const int size_256 = 256; // in bytes		
    test_parallel_correctness<uint64_t, size_256>(num_inserts);

		const int size_512 = 512; // in bytes		
    test_parallel_correctness<uint64_t, size_512>(num_inserts);

		const int size_1024 = 1024; // in bytes		
    test_parallel_correctness<uint64_t, size_1024>(num_inserts);

		const int size_2048 = 2048; // in bytes		
    test_parallel_correctness<uint64_t, size_2048>(num_inserts);

		const int size_4096 = 4096; // in bytes		
    test_parallel_correctness<uint64_t, size_4096>(num_inserts);

		const int size_8192 = 8192; // in bytes		
    test_parallel_correctness<uint64_t, size_8192>(num_inserts);
  }
  // test_unordered_insert_then_find<uint64_t, p_128>(n, s * p_128);
  // test_unordered_insert_and_find<uint64_t, p_128>(n, s * p_128);

	// test_unordered_insert_from_base<uint64_t, p_128>(n, p_128 * s);
	// test_unordered_insert_from_base_start<uint64_t, p_128>(n, p_128 * s);
  // test_unordered_insert_from_base_zipf<uint64_t, p_128>(n, p_128 * s, filename);

  /*
	printf("\nexp size 256\n");
	constexpr uint32_t p_256 = 1 << 8;
	test_unordered_insert<uint64_t, p_256>(n, seed, p_256 * s);
	
  printf("\nexp size 512\n");
	constexpr uint32_t p_512 = 1 << 9;
	test_unordered_insert<uint64_t, p_512>(n, seed, p_512 * s);

	printf("\nexp size 1024\n");
	constexpr uint32_t p_1024 = 1 << 10;
	test_unordered_insert<uint64_t, p_1024>(n, p_1024 * s);

	printf("\nexp size 2048\n");
	constexpr uint32_t p_2048 = 1 << 11;
	test_unordered_insert<uint64_t, p_2048>(n, seed, p_2048 * s);
	
	printf("\nexp size 4096\n");
	constexpr uint32_t p_4096 = 1 << 12;
	test_unordered_insert<uint64_t, p_4096 >(n, seed, p_4096 * s);
	
	printf("\nexp size 8192\n");
	constexpr uint32_t p_8192 = 1 << 13;
	test_unordered_insert<uint64_t, p_8192>(n, seed, p_8192 * s);
*/
}
