import csv
import matplotlib.pyplot as plt

def plot_two_csvs_to_line(csv_file1, csv_file2, output_image=None, start_percentile=None, end_percentile=None):
    """
    Reads two CSV files and plots their contents as line plots on the same graph with the x-axis as percentiles (0-100).

    Parameters:
        csv_file1 (str): Path to the first input CSV file.
        csv_file2 (str): Path to the second input CSV file.
        output_image (str, optional): Path to save the output plot as an image. If None, the plot is displayed.
        start_percentile (float, optional): Starting percentile (0-100) of the range to plot.
        end_percentile (float, optional): Ending percentile (0-100) of the range to plot.
    """
    try:
        def read_csv_data(csv_file):
            """Helper function to read data from a CSV file."""
            values = []
            with open(csv_file, 'r') as file:
                reader = csv.reader(file)
                for row in reader:
                    if row:  # Skip empty rows
                        values.append(float(row[0]))
            return values

        # Read data from both CSV files
        y_values1 = read_csv_data(csv_file1)
        y_values2 = read_csv_data(csv_file2)

        print(f"Read {len(y_values1)} values from {csv_file1}"
              f" and {len(y_values2)} values from {csv_file2}")

        # Generate percentile x values (0-100)
        x_values1 = [(i / (len(y_values1) - 1)) * 100 for i in range(len(y_values1))]
        x_values2 = [(i / (len(y_values2) - 1)) * 100 for i in range(len(y_values2))]

        # Convert percentile range to indices
        if start_percentile is not None or end_percentile is not None:
            start_percentile = start_percentile if start_percentile is not None else 0
            end_percentile = end_percentile if end_percentile is not None else 100

            start_index1 = int((start_percentile / 100) * (len(y_values1) - 1))
            end_index1 = int((end_percentile / 100) * (len(y_values1) - 1)) + 1

            start_index2 = int((start_percentile / 100) * (len(y_values2) - 1))
            end_index2 = int((end_percentile / 100) * (len(y_values2) - 1)) + 1

            x_values1 = x_values1[start_index1:end_index1]
            y_values1 = y_values1[start_index1:end_index1]

            x_values2 = x_values2[start_index2:end_index2]
            y_values2 = y_values2[start_index2:end_index2]

        # Create the line plot
        plt.figure(figsize=(10, 6))
        plt.plot(x_values1, y_values1, marker='o', linestyle='-', label=csv_file1)
        plt.plot(x_values2, y_values2, marker='s', linestyle='--', label=csv_file2)
        plt.title("Line Plot of Two CSV Files")
        plt.xlabel("Percentile (0-100)")
        plt.ylabel("Value")
        plt.grid(True)
        plt.legend()
        plt.ylim(0, 100000)

        # Save or display the plot
        if output_image:
            plt.savefig(output_image)
            print(f"Plot saved as {output_image}")
        else:
            plt.show()

    except Exception as e:
        print(f"An error occurred: {e}")


lst = ["0", "01", "1", "5", "10", "25", "75", "100"]

# for string in lst:
#     plot_two_csvs_to_line(f"bskip/insert_{string}.csv", f"btree/insert_{string}.csv", f"combined_{string}.png",
#                           start_percentile=0, end_percentile=100)
#     plot_two_csvs_to_line(f"bskip/insert_{string}.csv", f"btree/insert_{string}.csv", f"zoomed_{string}.png",
#                           start_percentile=80, end_percentile=100)

plot_two_csvs_to_line(f"bskip/insert_100.csv", f"btree/insert_100.csv", f"plots/zoomed_100.png",
                        start_percentile=50, end_percentile=100)

# plot_two_csvs_to_line(f"bskip/insert_5.csv", f"btree/insert_5.csv", f"plots/zoomed_5.png",
#                         start_percentile=95, end_percentile=100)

# plot_two_csvs_to_line(f"bskip/insert_10.csv", f"btree/insert_10.csv", f"plots/zoomed_10.png",
#                         start_percentile=95, end_percentile=100)
