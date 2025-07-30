import java.util.concurrent.ConcurrentSkipListMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class YCSB_CSLM {
  private static volatile int nothing;
  // OPCODES
  // Limited scope currently
  public enum OpCodes {
    INSERT,
    UPDATE,
    READ,
    DELETE,
    SCAN
  }

  // constants, since java has no global variables
  public static int RUN_SIZE = 100000000;

  // Percentile calculation yoinked from stackoverflow
  public static long percentile(List<Long> latencies, double percentile) {
    int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
    return latencies.get(index-1);
  }

  // parallel for accepting a single function with a single integer parameter
  public static void parallelFor(int num_threads, int start, int end, IntConsumer f) {
    ExecutorService executor = Executors.newFixedThreadPool(num_threads);
    int per_thread = (end - start) / num_threads;

    for (int n = 0; n < num_threads; n++) {
      final int thread_start = start + per_thread * n;
      final int thread_end = (n == num_threads - 1) ? end : thread_start + per_thread;

      // starts a thread for a range
      executor.submit(() -> {
        for(int j = thread_start; j < thread_end; j++) {
          f.accept(j);
        }
      });
    }

    // stops new jobs
    executor.shutdown();
    // tries its best to make sure executor is terminated
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (Exception e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      System.out.println("Bad parallel stuff happened:");
      System.out.println(e.getMessage());
    }
  }


  public static void main(String[] args) {
    // arguments
    if (args.length != 4) {
      System.out.println("Error incorrect arguments");
      System.out.println("<load path> <index path> <threads> <output path>");
      System.exit(0);
    }

    System.out.println("Do NOT use tasks with range queries. There will probably be undefined behavior.");

    String load_file = args[0];
    String index_file = args[1];
    int threads = Integer.parseInt(args[2]);
    String output_path = args[3];

    // Lists to store data in memory after reading from fileio
    // Initial data, only insert operations
    ArrayList<Long> init_keys = new ArrayList<>();


    // index data
    ArrayList<OpCodes> ops = new ArrayList<>();
    ArrayList<Long> keys = new ArrayList<>();
    // ranges tbd if we want to do them, no ranges
    ArrayList<Long> range_lengths = new ArrayList<>();
    // Pre-calculated end keys for SCAN operations
    ArrayList<Long> end_keys = new ArrayList<>();



    System.out.println("Loading input arraylist");
    // load in initial keys
    // Error checking will probably NOT catch specific problems like if the input file is badly formatted
    try (Scanner scanner = new Scanner(new File(load_file))) {
      int counter = 0;
      while(scanner.hasNextLine() && counter < RUN_SIZE) {
        init_keys.add(Long.parseLong(scanner.nextLine().split("\\s+")[1]));
        counter += 1;
      }
    } catch (Exception e) {
      System.out.println("Error on reading load file");
      System.out.println(e.getMessage());
    }
    
    // Sort the initial keys to help with end key calculation for scans
    ArrayList<Long> load_keys = new ArrayList<>(init_keys);
    Collections.sort(init_keys);
    System.out.println("Sorted " + init_keys.size() + " initial keys");

    int scan_counter = 0;
    System.out.println("Loading index arraylist");
    // load in index data, don't use range query files
    // Error checking will probably NOT catch specific problems like if the input file is badly formatted
    try (Scanner scanner = new Scanner(new File(index_file))) {
      int counter = 0;

      while(scanner.hasNextLine() && counter < RUN_SIZE) {
        String[] operation = scanner.nextLine().split("\\s+");
        switch(operation[0]) {
          case "INSERT":
            ops.add(OpCodes.INSERT);
            keys.add(Long.parseLong(operation[1]));
            range_lengths.add(1L);
            break;
          case "UPDATE":
            ops.add(OpCodes.UPDATE);
            keys.add(Long.parseLong(operation[1]));
            range_lengths.add(1L);
            break;
          case "READ":
            ops.add(OpCodes.READ);
            keys.add(Long.parseLong(operation[1]));
            range_lengths.add(1L);
            break;
          case "SCAN":
            ops.add(OpCodes.SCAN);
            keys.add(Long.parseLong(operation[1]));
            range_lengths.add(Long.parseLong(operation[2]));
            scan_counter += 1;
            break;
          default:
            throw new RuntimeException("Unknown operation found: "+operation[0]);
        }
        counter += 1;
      }
    } catch (Exception e) {
      System.out.println("Error on reading index file");
      System.out.println(e.getMessage());
    }
    
    // Pre-calculate end keys for all SCAN operations
    System.out.println("Pre-calculating end keys for SCAN operations...");
    for (int i = 0; i < ops.size(); i++) {
        if (ops.get(i) == OpCodes.SCAN) {
            long startKey = keys.get(i);
            long scanLength = range_lengths.get(i);
            
            // Find position of start key in sorted list
            int startPos = Collections.binarySearch(init_keys, startKey);
            if (startPos < 0) {
                // Key not found, get insertion point
                startPos = -startPos - 1;
            }
            
            // Calculate end position with bounds check
            int endPos = (int)Math.min(startPos + scanLength, init_keys.size() - 1);
            
            // Get actual end key value
            long endKey = (endPos < init_keys.size()) ? init_keys.get(endPos) : Long.MAX_VALUE;
            end_keys.add(endKey);
        } else {
            // Add a placeholder for non-SCAN operations
            end_keys.add(0L);
        }
    }
    System.out.println("End key calculation complete. Found " + scan_counter + " scan operations.");

    // for (int ii = 0; ii < 20; ii ++) {
    //     System.out.println("Start key: " + keys.get(ii));
    //     System.out.println("End key: " + end_keys.get(ii));
    //     int startIndex = Collections.binarySearch(init_keys, keys.get(ii));
    //     int endIndex = Collections.binarySearch(init_keys, end_keys.get(ii));
    //     System.out.println("length: " + range_lengths.get(ii));
    //     System.out.println("diff: " + (endIndex - startIndex));
    // }


    // Timing data
    ArrayList<Double> load_tpts = new ArrayList<>();
    ArrayList<Double> index_tpts = new ArrayList<>();


    // Thread safe latency data
    List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    List<Long> loadLatencies = Collections.synchronizedList(new ArrayList<>());

    System.out.println("Processing operations");
    // do index ops
    // process operations 6 times
    for (int k = 0; k < 6; k++) {
      // Create the skiplist,
      ConcurrentSkipListMap<Long, Long> map = new ConcurrentSkipListMap<>();

      int batch_size = 10;
      // getting a constant k for thread safety reasons in Java
      final int final_k = k;

      // Loading
      System.out.println("Processing loading");
      Instant load_start = Instant.now();
      parallelFor(threads, 0, init_keys.size(), i -> {
        map.put(load_keys.get(i), load_keys.get(i));
      });
      Instant load_end = Instant.now();
      Duration load_duration = Duration.between(load_start, load_end);

      if (k != 0) {
        // probably don't actually need the second cast, but it's been ages since I took AP comp sci
        load_tpts.add(((double)RUN_SIZE)/((double)load_duration.toNanos()/1000.0));
      }

      // index run
      System.out.println("Processing index run");
      Instant run_start = Instant.now();

      // AtomicInteger s_counter = new AtomicInteger(0);

      parallelFor(threads, 0, ops.size(), i -> {
        switch(ops.get(i)) {
          case INSERT:
            map.put(keys.get(i), keys.get(i));
            break;
          case READ:
            var found = map.floorEntry(keys.get(i));
            break;
          case SCAN:
            // s_counter.incrementAndGet();
            
            // Use pre-calculated end key
            long startKey = keys.get(i);
            long scanLength = range_lengths.get(i);
            long endKey = end_keys.get(i);
            
            // Efficient subMap operation using pre-calculated keys
            var smap = map.subMap(startKey, true, endKey, true);
            
            // Process entries in the submap
            long sum = 0;
            for (var entry : smap.entrySet()) {
                sum += entry.getValue();
                // Stop if we've processed enough entries
                if (--scanLength <= 0) break;
            }

            if (i == 0) {
                System.out.println("Sample scan sum: " + sum);
            }
            
            // Prevent optimization from eliminating the calculation
            // if (sum < 0 && i % 1000000 == 0) {
            //     System.out.println("Sample scan sum: " + sum);
            // }
            
            break;
          default:
            System.out.println("Something broke while executing index operations");
            break;
        }
      });
      
      Instant run_end = Instant.now();
      Duration run_duration = Duration.between(run_start, run_end);

      // System.out.println("Invalid SCAN operations detected: " + s_counter.get());

      if (k != 0) {
        index_tpts.add(((double)RUN_SIZE)/((double)run_duration.toNanos()/1000.0));
      }
    }

    // for calculation
    Collections.sort(load_tpts);
    Collections.sort(index_tpts);

    System.out.println("Report:");
    System.out.println("--------");
    System.out.println("load: " + (load_tpts.get(2) + load_tpts.get(3)) / 2);
    System.out.println("run: " + (index_tpts.get(2) + index_tpts.get(3)) / 2);
    System.out.println("--------");

    System.out.println("Completed");
  }
}
