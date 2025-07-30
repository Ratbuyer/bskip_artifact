import os
import glob
import numpy as np

def read_latencies(folder, pattern):
    latencies = []
    for filepath in glob.glob(os.path.join(folder, pattern)):
        with open(filepath, "r") as file:
            content = file.read()
            latencies += [int(x) for x in content.strip().split()]
    return latencies

def compute_percentiles(data):
    sorted_data = np.sort(data)
    return {
        "count": len(data),
        "median": np.percentile(sorted_data, 50),
        "p90": np.percentile(sorted_data, 90),
        "p99": np.percentile(sorted_data, 99),
        "p999": np.percentile(sorted_data, 99.9)
    }

def print_stats(name, stats):
    print(f"\n{name} Latencies:")
    print(f"  Total samples: {stats['count']}")
    print(f"  Median:        {stats['median']}")
    print(f"  90th %ile:     {stats['p90']}")
    print(f"  99th %ile:     {stats['p99']}")
    print(f"  99.9th %ile:   {stats['p999']}")

if __name__ == "__main__":
    folder = "latencydata"
    
    latency_files = "latency[0-9]*.txt"
    latencyload_files = "latencyload[0-9]*.txt"

    latency_data = read_latencies(folder, latency_files)
    latencyload_data = read_latencies(folder, latencyload_files)

    if latency_data:
        stats_latency = compute_percentiles(latency_data)
        print_stats("Execution", stats_latency)
    else:
        print("No execution latency files found.")

    if latencyload_data:
        stats_latencyload = compute_percentiles(latencyload_data)
        print_stats("Load", stats_latencyload)
    else:
        print("No load latency files found.")
