import os
import glob


latencies = []

for file in os.listdir("lat2"):
    with open(os.path.join("lat2", file), 'r') as f:
        data = f.read()
        latencies.extend(data.split())

latencies = [int(item) for item in latencies]
latencies.sort()

percentiles = [50, 90, 99, 99.9, 99.99]
for p in percentiles:
    index = int((p/100.0) * float(len(latencies)-1))
    print(p, ":", latencies[index])
