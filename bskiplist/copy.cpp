#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include <sys/time.h>
#include <thread>
#include <cstdint>

#include <ParallelTools/parallel.h>
#include "cxxopts.hpp"
#include "timers.hpp"
#include "bskip.h"

using namespace std;

using Key = uint64_t;

using TID = uint64_t;

// index types
enum
{
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

enum
{
  OP_INSERT,
  OP_UPDATE,
  OP_READ,
  OP_SCAN,
  OP_SCAN_END,
  OP_DELETE,
};

enum
{
  WORKLOAD_A,
  WORKLOAD_B,
  WORKLOAD_C,
  WORKLOAD_D,
  WORKLOAD_E,
  WORKLOAD_X,
  WORKLOAD_Y,
};

enum
{
  RANDINT_KEY,
  STRING_KEY,
};

enum
{
  UNIFORM,
  ZIPFIAN,
};

static uint64_t LOAD_SIZE = 100000000;
static uint64_t RUN_SIZE = 100000000;

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

void ycsb_load_run_string(int index_type, int wl, int kt, int ap, int num_thread,
                          std::vector<Key *> &init_keys,
                          std::vector<Key *> &keys,
                          std::vector<int> &ranges,
                          std::vector<int> &ops)
{
}

// static long get_usecs() {
//   struct timeval st;
//   gettimeofday(&st, NULL);
//   return st.tv_sec * 1000000 + st.tv_usec;
// }

double findMedian(vector<double>& vec) {
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

template <typename F> inline void parallel_for(int numThreads, size_t start, size_t end, F f) {
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
                                  std::seed_seq &seed)
{

  std::mt19937_64 eng(seed); // a source of random data

  std::uniform_int_distribution<T> dist(0, max_val);
  std::vector<T> v(n);

  generate(begin(v), end(v), bind(dist, eng));
  return v;
}

void ycsb_load_run_randint(int index_type, int wl, int kt, int ap, int num_thread,
                           std::vector<uint64_t> &init_keys,
                           std::vector<uint64_t> &keys,
                           std::vector<uint64_t> &range_end,
                           std::vector<int> &ranges,
                           std::vector<int> &ops)
{
  std::string init_file;
  std::string txn_file;

  std::string uniform_dir = "/home/eddy/uniform";
  std::string zipfian_dir = "/home/eddy/zipfian";

  if (ap == UNIFORM)
  {
    if (kt == RANDINT_KEY && wl == WORKLOAD_A)
    {
      init_file = uniform_dir + "/loada_unif_int.dat";
      txn_file = uniform_dir + "/txnsa_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_B)
    {
      init_file = uniform_dir + "/loadb_unif_int.dat";
      txn_file = uniform_dir + "/txnsb_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_C)
    {
      init_file = uniform_dir + "/loadc_unif_int.dat";
      txn_file = uniform_dir + "/txnsc_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_D)
    {
      init_file = uniform_dir + "/loadd_unif_int.dat";
      txn_file = uniform_dir + "/txnsd_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_E)
    {
      init_file = uniform_dir + "/loade_unif_int.dat";
      txn_file = uniform_dir + "/txnse_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_X)
    {
      init_file = uniform_dir + "/loadx_unif_int.dat";
      txn_file = uniform_dir + "/txnsx_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_Y)
    {
      init_file = uniform_dir + "/loady_unif_int.dat";
      txn_file = uniform_dir + "/txnsy_unif_int.dat";
    }
  }
  else
  {
    if (kt == RANDINT_KEY && wl == WORKLOAD_A)
    {
      init_file = zipfian_dir + "/loada_unif_int.dat";
      txn_file = zipfian_dir + "/txnsa_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_B)
    {
      init_file = zipfian_dir + "/loadb_unif_int.dat";
      txn_file = zipfian_dir + "/txnsb_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_C)
    {
      init_file = zipfian_dir + "/loadc_unif_int.dat";
      txn_file = zipfian_dir + "/txnsc_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_D)
    {
      init_file = zipfian_dir + "/loadd_unif_int.dat";
      txn_file = zipfian_dir + "/txnsd_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_E)
    {
      init_file = zipfian_dir + "/loade_unif_int.dat";
      txn_file = zipfian_dir + "/txnse_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_X)
    {
      init_file = zipfian_dir + "/loadx_unif_int.dat";
      txn_file = zipfian_dir + "/txnsx_unif_int.dat";
    }
    else if (kt == RANDINT_KEY && wl == WORKLOAD_Y)
    {
      init_file = zipfian_dir + "/loady_unif_int.dat";
      txn_file = zipfian_dir + "/txnsy_unif_int.dat";
    }
  }

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

  int count = 0;
  while ((count < LOAD_SIZE) && infile_load.good())
  {
    infile_load >> op >> key;
    if (op.compare(insert) != 0)
    {
      std::cout << "READING LOAD FILE FAIL!\n";
      return;
    }
    init_keys.push_back(key);
    count++;
  }

  fprintf(stderr, "Loaded %d keys\n", count);

  std::ifstream infile_txn(txn_file);

  count = 0;
  while ((count < RUN_SIZE) && infile_txn.good())
  {
    infile_txn >> op >> key;
    if (op.compare(insert) == 0)
    {
      ops.push_back(OP_INSERT);
      keys.push_back(key);
      ranges.push_back(1);
      range_end.push_back(1);
    }
    else if (op.compare(update) == 0)
    {
      ops.push_back(OP_UPDATE);
      keys.push_back(key);
      ranges.push_back(1);
      range_end.push_back(1);
    }
    else if (op.compare(read) == 0)
    {
      ops.push_back(OP_READ);
      keys.push_back(key);
      ranges.push_back(1);
      range_end.push_back(1);
    }
    else if (op.compare(scan) == 0)
    {
      infile_txn >> range;
      ops.push_back(OP_SCAN);
      keys.push_back(key);
      ranges.push_back(range);
      range_end.push_back(1);
    }
    else if (op.compare(scanend) == 0)
    {
      infile_txn >> rend;
      ops.push_back(OP_SCAN_END);
      keys.push_back(key);
      range_end.push_back(rend);
      ranges.push_back(1);
    }
    else
    {
      std::cout << "UNRECOGNIZED CMD!\n";
      return;
    }
    count++;
  }

  std::atomic<int> range_complete, range_incomplete;
  range_complete.store(0);
  range_incomplete.store(0);

  fprintf(stderr, "Loaded %d more keys\n", count);

  std::this_thread::sleep_for(std::chrono::nanoseconds(3000000000));

  fprintf(stderr, "Slept\n");

  if (index_type == TYPE_BTREE)
  {
    std::vector<double> load_tpts;
    std::vector<double> run_tpts;

    int constexpr node_size = 1024;
    int constexpr p = node_size / (sizeof(Key) + sizeof(TID));

    using parallel_traits = BSkip_traits<true, p, p / 2, Key, TID>;

    for (int k = 0; k < 6; k++)
    {
      std::vector<uint64_t> query_results_keys(RUN_SIZE);
      std::vector<uint64_t> query_results_vals(RUN_SIZE);
      BSkip<parallel_traits> concurrent_map;
      {
        // Load
        auto starttime = get_usecs(); // std::chrono::system_clock::now();
        parallel_for(num_thread, 0, LOAD_SIZE, [&](const uint64_t &i)
                     { concurrent_map.insert({init_keys[i], init_keys[i]}); });
        auto end = get_usecs();
        auto duration = end - starttime;
        if(k!=0) load_tpts.push_back(((double)LOAD_SIZE)/duration);
        printf("\tLoad took %lu us, throughput = %f ops/us\n", duration, ((double)LOAD_SIZE) / duration);
        // printf("Throughput: load, %f ,ops/us and time %ld in us\n", (LOAD_SIZE * 1.0) / duration.count(), duration.count());
      }
      {
        // Run
        constexpr bool latency = false;

        ThreadSafeVector<uint64_t> insert_latencies;
        ThreadSafeVector<uint64_t> read_latencies;
        ThreadSafeVector<uint64_t> map_length_latencies;
        ThreadSafeVector<uint64_t> map_range_latencies;

        auto starttime = std::chrono::system_clock::now();
        parallel_for(num_thread, 0, RUN_SIZE, [&](const uint64_t &i)
                    {
                    if (ops[i] == OP_INSERT) {
                        if constexpr(latency) {
                            auto start = std::chrono::high_resolution_clock::now();
                            concurrent_map.insert({keys[i], keys[i]});
                            auto end = std::chrono::high_resolution_clock::now();
                            insert_latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
                        } else
                            concurrent_map.insert({keys[i], keys[i]});
                    } else if (ops[i] == OP_READ) {
                        if constexpr(latency) {
                            auto start = std::chrono::high_resolution_clock::now();
                            concurrent_map.find(keys[i]);
                            auto end = std::chrono::high_resolution_clock::now();
                            read_latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
                        } else
                            concurrent_map.value(keys[i]);
                    } else if (ops[i] == OP_SCAN) {
			            uint64_t sum = 0;

						if constexpr(latency) {
    						auto start = std::chrono::high_resolution_clock::now();
                            concurrent_map.map_range_length(keys[i], ranges[i], [&sum](auto key, auto value) {
                                sum += value;
                            });
                            auto end = std::chrono::high_resolution_clock::now();
                            map_length_latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
                        } else
                            concurrent_map.map_range_length(keys[i], ranges[i], [&sum](auto key, auto value) {
                                sum += value;
                            });

                    } else if (ops[i] == OP_SCAN_END) {
                        uint64_t sum = 0;

                        if constexpr(latency) {
                        auto start = std::chrono::high_resolution_clock::now();
                        concurrent_map.map_range(keys[i], range_end[i], [&sum](auto key, auto value) {
                            sum += value;
                        });
                        auto end = std::chrono::high_resolution_clock::now();
                        map_range_latencies.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
                        } else
                            concurrent_map.map_range(keys[i], range_end[i], [&sum](auto key, auto value) {
                                sum += value;
                            });
                    } else if (ops[i] == OP_UPDATE) {
                        std::cout << "NOT SUPPORTED CMD!\n";
                        exit(0);
                    } });

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
        if(k!=0) run_tpts.push_back((RUN_SIZE * 1.0) / duration.count());

        if constexpr(!latency) {
            printf("\tRun, throughput: %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }

        // latency
        if constexpr(latency) {
            insert_latencies.print_percentile(90);
            read_latencies.print_percentile(90);
            map_length_latencies.print_percentile(90);
            map_range_latencies.print_percentile(90);
        }
      }
    }

    printf("\tMedian Load throughput: %f ,ops/us\n", findMedian(load_tpts));
    printf("\tMedian Run throughput: %f ,ops/us\n", findMedian(run_tpts));
  }
}

int main(int argc, char **argv)
{
  if (argc != 6)
  {
    std::cout << "Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads]\n";
    std::cout << "1. index type: art hot bwtree masstree clht\n";
    std::cout << "               fastfair levelhash cceh woart\n";
    std::cout << "2. ycsb workload type: a, b, c, e\n";
    std::cout << "3. key distribution: randint, string\n";
    std::cout << "4. access pattern: uniform, zipfian\n";
    std::cout << "5. number of threads (integer)\n";
    return 1;
  }

  printf("%s, workload%s, %s, %s, threads %s\n", argv[1], argv[2], argv[3], argv[4], argv[5]);

  int index_type;
  if (strcmp(argv[1], "art") == 0)
    index_type = TYPE_ART;

  else if (strcmp(argv[1], "btree") == 0)
    index_type = TYPE_BTREE;
  else if (strcmp(argv[1], "hot") == 0)
  {
#ifdef HOT
    index_type = TYPE_HOT;
#else
    return 1;
#endif
  }
  else if (strcmp(argv[1], "bwtree") == 0)
    index_type = TYPE_BWTREE;
  else if (strcmp(argv[1], "masstree") == 0)
    index_type = TYPE_MASSTREE;
  else if (strcmp(argv[1], "clht") == 0)
    index_type = TYPE_CLHT;
  else if (strcmp(argv[1], "fastfair") == 0)
    index_type = TYPE_FASTFAIR;
  else if (strcmp(argv[1], "levelhash") == 0)
    index_type = TYPE_LEVELHASH;
  else if (strcmp(argv[1], "cceh") == 0)
    index_type = TYPE_CCEH;
  else if (strcmp(argv[1], "woart") == 0)
    index_type = TYPE_WOART;
  else
  {
    fprintf(stderr, "Unknown index type: %s\n", argv[1]);
    exit(1);
  }

  int wl;
  if (strcmp(argv[2], "a") == 0)
  {
    wl = WORKLOAD_A;
  }
  else if (strcmp(argv[2], "b") == 0)
  {
    wl = WORKLOAD_B;
  }
  else if (strcmp(argv[2], "c") == 0)
  {
    wl = WORKLOAD_C;
  }
  else if (strcmp(argv[2], "d") == 0)
  {
    wl = WORKLOAD_D;
  }
  else if (strcmp(argv[2], "e") == 0)
  {
    wl = WORKLOAD_E;
  }
  else if (strcmp(argv[2], "x") == 0)
  {
    wl = WORKLOAD_X;
  }
  else if (strcmp(argv[2], "y") == 0)
  {
    wl = WORKLOAD_Y;
  }
  else
  {
    fprintf(stderr, "Unknown workload: %s\n", argv[2]);
    exit(1);
  }

  int kt;
  if (strcmp(argv[3], "randint") == 0)
  {
    kt = RANDINT_KEY;
  }
  else if (strcmp(argv[3], "string") == 0)
  {
    kt = STRING_KEY;
  }
  else
  {
    fprintf(stderr, "Unknown key type: %s\n", argv[3]);
    exit(1);
  }

  int ap;
  if (strcmp(argv[4], "uniform") == 0)
  {
    ap = UNIFORM;
  }
  else if (strcmp(argv[4], "zipfian") == 0)
  {
    ap = ZIPFIAN;
  }
  else
  {
    fprintf(stderr, "Unknown access pattern: %s\n", argv[4]);
    exit(1);
  }

  int num_thread = atoi(argv[5]);
  // tbb::task_scheduler_init init(num_thread);

  if (kt != STRING_KEY)
  {
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

    ycsb_load_run_randint(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges_end, ranges, ops);
  }

  return 0;

  // ./ycsb btree a randint uniform 4
}
