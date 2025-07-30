import re

# Define a function to extract relevant metrics from a log file
def extract_metrics(file_path):
    # Patterns to match median throughputs and percentiles
    median_load_pattern = r"Median Load throughput: ([\d\.]+) ,ops/us"
    median_run_pattern = r"Median Run throughput: ([\d\.]+) ,ops/us"
    percentile_50_pattern = r"Percentile 50: (\d+)"
    percentile_90_pattern = r"Percentile 90: (\d+)"
    percentile_99_pattern = r"Percentile 99: (\d+)"
    percentile_999_pattern = r"Percentile 99\.9: (\d+)"
    percentile_9999_pattern = r"Percentile 99\.99: (\d+)"

    results = []

    # Read the log file
    with open(file_path, 'r') as file:
        content = file.read()

        # Find all matches for the patterns
        median_loads = re.findall(median_load_pattern, content)
        median_runs = re.findall(median_run_pattern, content)
        percentiles_50 = re.findall(percentile_50_pattern, content)
        percentiles_90 = re.findall(percentile_90_pattern, content)
        percentiles_99 = re.findall(percentile_99_pattern, content)
        percentiles_999 = re.findall(percentile_999_pattern, content)
        percentiles_9999 = re.findall(percentile_9999_pattern, content)
        
        for i in range(len(median_loads)):
        	if 1:
	            results.append((
	                median_loads[i] if i < len(median_loads) else "N/A",
	                median_runs[i] if i < len(median_runs) else "N/A",
	                # percentiles_50[i] if i < len(percentiles_50) else "N/A",
	                # percentiles_90[i] if i < len(percentiles_90) else "N/A",
	                # percentiles_99[i] if i < len(percentiles_99) else "N/A",
	                # percentiles_999[i] if i < len(percentiles_999) else "N/A",
	                # percentiles_9999[i] if i < len(percentiles_9999) else "N/A",
	            ))

        # Combine the extracted values into rows
        for i in range(len(median_loads) * 2):
        	if i % 2 == 0:
	            results.append((
	                # median_loads[i] if i < len(median_loads) else "N/A",
	                # median_runs[i] if i < len(median_runs) else "N/A",
	                percentiles_50[i] if i < len(percentiles_50) else "N/A",
	                percentiles_90[i] if i < len(percentiles_90) else "N/A",
	                percentiles_99[i] if i < len(percentiles_99) else "N/A",
	                percentiles_999[i] if i < len(percentiles_999) else "N/A",
	                percentiles_9999[i] if i < len(percentiles_9999) else "N/A",
	            ))
		
        for i in range(len(median_loads) * 2):
        	if i % 2 == 1:
	            results.append((
	                # median_loads[i] if i < len(median_loads) else "N/A",
	                # median_runs[i] if i < len(median_runs) else "N/A",
	                percentiles_50[i] if i < len(percentiles_50) else "N/A",
	                percentiles_90[i] if i < len(percentiles_90) else "N/A",
	                percentiles_99[i] if i < len(percentiles_99) else "N/A",
	                percentiles_999[i] if i < len(percentiles_999) else "N/A",
	                percentiles_9999[i] if i < len(percentiles_9999) else "N/A",
				))
				

    return results

# Specify the path to your log file
log_file_path = "results/tmp.txt"

# Call the function to extract metrics
metrics_list = extract_metrics(log_file_path)

# Print each set of extracted values in a single line
print("Median Load\tMedian Run\tPercentile 50\tPercentile 90\tPercentile 99\tPercentile 99.9\tPercentile 99.99")
for metrics in metrics_list:
    print("\t".join(metrics))
