import os
import json
import argparse

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

from tol_colors import tol_cset

# (Absolute) path example '{REPO_DIR}/results/wf/synthetic_1s/k_mode/0_batch_su/100_keys/'
def parse_arguments():
    parser = argparse.ArgumentParser(description='Draw latency or throughput chart.')
    parser.add_argument('tests_path', type=str, help='Path to the tests folders')
    parser.add_argument('mode', type=str, choices=['wf', 'fl'], help='Benchmark: "windflow" or "flink"')
    parser.add_argument('chart_type', type=str, choices=['lt', 'th', 'batch', 'src', 'all'], help='Chart type: "lt" for latency, "th" for throughput, "src" for average performence per source, "all" for both, "batch" for average performence per batch')
    return parser.parse_args()

args = parse_arguments()

if(args.mode == 'wf'):
    cset = tol_cset('bright')
    main_color = cset.blue
    secondary_color = cset.cyan
    error_color = cset.red
elif(args.mode == 'fl'):
    cset = tol_cset('vibrant')
    main_color = cset.orange
    secondary_color = cset.teal
    error_color = cset.blue

#print(cset._fields)

def get_x_labels(folders):
    x_labels = sorted([par_degree.split('_')[2] for par_degree in folders], key=lambda x: int(x))
    return x_labels, range(1, len(x_labels)+1)

def draw_latency_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))], key=lambda x: int(x.split('_')[0]))
    x_labels, x = get_x_labels(folders)
    y = []
    y_line_points = []
    
    fig, ax = plt.subplots()

    for folder in folders:
        folder_path = os.path.join(tests_path, folder)
        throughput_file = os.path.join(folder_path, 'latency.json')

        with open(throughput_file, 'r') as file:
            data = json.load(file)

            #Dividing by 1000 to convert from us to ms
            lower_whiskers = np.mean([entry['0']/1000 for entry in data])
            _5th_percentile = np.mean([entry['5']/1000 for entry in data])
            lower_quartile = np.mean([entry['25']/1000 for entry in data])
            _50th_percentile = np.mean([entry['50']/1000 for entry in data])
            upper_quartile = np.mean([entry['75']/1000 for entry in data])
            _95th_percentile = np.mean([entry['95']/1000 for entry in data])
            upper_whiskers = np.mean([entry['100']/1000 for entry in data])

            means = np.mean([entry['mean']/1000 for entry in data])
            y_line_points.append(means)
            
            box = np.array([lower_whiskers, _5th_percentile, lower_quartile, _50th_percentile, upper_quartile, _95th_percentile, upper_whiskers])
            y.append(box)
    
    ax.boxplot(y, labels=x_labels, showfliers=True, showmeans=False, meanline=False,
                       patch_artist=True, boxprops=dict(facecolor=main_color),
                       medianprops=dict(color=secondary_color, linewidth=1),
                       meanprops=dict(color=error_color, linewidth=1.4, linestyle='-'))
    ax.plot(x, y_line_points, marker="o", markersize=5, color=error_color, ls='-', lw=1.4)

    _, max_y = ax.get_ylim()
    tick_interval = tick_interval = round(max_y / 15)

    ax.set_yticks(np.arange(0, max_y, tick_interval))
    ax.grid(True, axis="y", ls='--', lw=1, alpha=.5)
    ax.set_axisbelow(True)

    #plt.title('Latency chart')
    plt.xlabel('Parallelism')
    plt.ylabel('Latency (ms)')

    fig.set_dpi(100)
    fig.set_size_inches(18, 10, forward=True)
    fig.savefig(os.path.join(tests_path, 'latency.svg'))
    #fig.savefig(os.path.join(tests_path, 'latency.png'))

def draw_throughput_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))], key=lambda x: int(x.split('_')[0]))
    x_labels, x = get_x_labels(folders)
    y = [] 
    errors = []
    fig, ax = plt.subplots()

    for folder in folders:
        folder_path = os.path.join(tests_path, folder)
        throughput_file = os.path.join(folder_path, 'throughput.json')

        with open(throughput_file, 'r') as file:
            data = json.load(file)
            throughput = np.array(data)
            y.append(np.mean(throughput, axis=0))
            errors.append(np.std(throughput, axis=0))
    
    ax.bar(x = x, tick_label = x_labels, height = y, color = main_color, edgecolor = "black" , yerr=errors, ecolor = error_color, error_kw=dict(lw=2, capsize=10, capthick=1.5))
    ax.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    ax.set_axisbelow(True)

    #plt.title('Throughput Chart')
    plt.xlabel('Parallelism')
    plt.ylabel('Throughput (tuples/s)')

    fig.set_dpi(100)
    fig.set_size_inches(18, 10, forward=True)
    fig.savefig(os.path.join(tests_path, 'throughput.svg'))
    #fig.savefig(os.path.join(tests_path, 'throughput.png'))

def draw_avgmetrics_per_source(sources_path):
    # Get the list of source folders, sorted
    sources_folders = sorted([folder for folder in os.listdir(sources_path) if os.path.isdir(os.path.join(sources_path, folder))], key=lambda x: int(x.split('_')[1]))
    # Initialize the figure and axis for the plot
    fig, (lt, th) = plt.subplots(2, 1)
    # Define the colors for the lines
    rates = sorted([int(folder.split('_')[1]) for folder in sources_folders])
    colors = ['b', 'g', 'y', 'r']

    # Iterate over the source folders
    for i, source_folder in enumerate(sources_folders):
        # Initialize y list for the plots
        y_lt = []
        y_th = []
        # Get the list of parallelism folders, sorted
        parallelism_folders = sorted([folder for folder in os.listdir(os.path.join(sources_path, source_folder)) if os.path.isdir(os.path.join(sources_path, source_folder, folder))], key=lambda x: int(x.split('_')[0]))
        
        # Iterate over the parallelism folders
        for parallelism_folder in parallelism_folders:
            # Load the latency data from the file
            latency_file = os.path.join(sources_path, source_folder, parallelism_folder, 'latency.json')
            with open(latency_file, 'r') as file:
                data = json.load(file)
                mean_latency = np.mean([entry['mean']/1000 for entry in data])
            # Append the mean latency to the y values
            y_lt.append(mean_latency)

            # Load the throughput data from the file
            throughput_file = os.path.join(sources_path, source_folder, parallelism_folder, 'throughput.json')
            with open(throughput_file, 'r') as file:
                data = json.load(file)
                mean_throughput = np.mean(np.array(data), axis=0)

            # Append the mean throughput to the y values
            y_th.append(mean_throughput)

        # Plot the line for this source folder
        lt.plot(y_lt, color=colors[i % len(colors)], label="source = " + str(rates[i % len(rates)]), marker="x", markersize=9, ls='-', lw=2)
        th.plot(y_th, color=colors[i % len(colors)], label="source = " + str(rates[i % len(rates)]), marker="x", markersize=9, ls='-', lw=2)

    _, max_y = lt.get_ylim()
    tick_interval = round(max_y / 15)
    lt.set_yticks(np.arange(0, max_y, tick_interval))
    lt.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    lt.set_axisbelow(True)

    th.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    th.set_axisbelow(True)

    # Set the labels for the plot
    x_labels, _ = get_x_labels(parallelism_folders)
    lt.set_xticks(range(len(x_labels)), x_labels)
    th.set_xticks(range(len(x_labels)), x_labels)
    plt.xlabel('Parallelism')

    lt.set_ylabel('Average Latency (ms)',fontweight ='bold')
    th.set_ylabel('Average Throughput (tuples/s)',fontweight ='bold')

    # Add a legend
    #legend_labels = [f"Source {i+1}" for i in range(len(sources_folders))]
    lt.legend()

    fig.set_dpi(100)
    fig.set_size_inches(14, 10, forward=True)
    fig.savefig(os.path.join(sources_path, 'source.svg'), bbox_inches='tight', pad_inches=0.2)
    #fig.savefig(os.path.join(sources_path, 'source.png'))

    th.legend()
    th_extent = th.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(os.path.join(sources_path, 'source_throughput.svg'), bbox_inches=th_extent.expanded(1.1, 1.2))

    lt_extent = lt.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(os.path.join(sources_path, 'source_latency.svg'), bbox_inches=lt_extent.expanded(1.1, 1.2))

def draw_avgmetrics_per_batch(batch_path):
    # Get the list of batch folders, sorted
    batch_folders = sorted([folder for folder in os.listdir(batch_path) if os.path.isdir(os.path.join(batch_path, folder))], key=lambda x: int(x.split('_')[0]))
    # Initialize the figure and axis for the plot
    fig, (lt, th) = plt.subplots(2, 1)

    # Define the colors for the lines and batches sizes for the legend labels
    batches = sorted([int(folder.split('_')[0]) for folder in batch_folders])
    colors = ['b', 'g', 'r' ,'y' ]

    # Iterate over the batch folders
    for i, batch_folder in enumerate(batch_folders):
        # Initialize y list for the plots
        y_lt = []
        y_th = []

        # Get the list of parallelism folders, sorted
        main_path = os.path.join(batch_path, batch_folder, 'source_1')
        parallelism_folders = sorted([folder for folder in os.listdir(main_path) if os.path.isdir(os.path.join(main_path, folder))], key=lambda x: int(x.split('_')[0]))
        # Iterate over the parallelism folders
        for parallelism_folder in parallelism_folders:
            # Load the latency data from the file
            latency_file = os.path.join(main_path, parallelism_folder, 'latency.json')
            with open(latency_file, 'r') as file:
                data = json.load(file)
                mean_latency = np.mean([entry['mean']/1000 for entry in data])
            # Append the mean latency to the y values
            y_lt.append(mean_latency)

            # Load the throughput data from the file
            throughput_file = os.path.join(main_path, parallelism_folder, 'throughput.json')
            with open(throughput_file, 'r') as file:
                data = json.load(file)
                mean_throughput = np.mean(np.array(data), axis=0)

            # Append the mean throughput to the y values 
            y_th.append(mean_throughput)

        # Plot the lines per batch size per metric
        lt.plot(y_lt, color=colors[i % len(colors)], label="batch = " + str(batches[i % len(batches)]), marker="x", markersize=9, ls='-', lw=2)
        th.plot(y_th, color=colors[i % len(colors)], label="batch = " + str(batches[i % len(batches)]), marker="x", markersize=9, ls='-', lw=2)

    _, max_y = lt.get_ylim()
    tick_interval = round(max_y / 15)
    lt.set_yticks(np.arange(0, max_y, tick_interval))
    lt.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    lt.set_axisbelow(True)

    th.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    th.set_axisbelow(True)

    # Set the labels for the plot
    x_labels, _ = get_x_labels(parallelism_folders)
    lt.set_xticks(range(len(x_labels)), x_labels)
    th.set_xticks(range(len(x_labels)), x_labels)
    plt.xlabel('Parallelism')

    lt.set_ylabel('Average Latency (ms)',fontweight ='bold')
    th.set_ylabel('Average Throughput (tuples/s)',fontweight ='bold')
    lt.legend()

    fig.set_dpi(100)
    fig.set_size_inches(14, 10, forward=True)
    fig.savefig(os.path.join(batch_path, 'batch.svg'), bbox_inches='tight', pad_inches=0.2)
    #fig.savefig(os.path.join(sources_path, 'batch.png'))

    th.legend()
    th_extent = th.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(os.path.join(batch_path, 'batch_throughput.svg'), bbox_inches=th_extent.expanded(1.1, 1.2))

    lt_extent = lt.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(os.path.join(batch_path, 'batch_latency.svg'), bbox_inches=lt_extent.expanded(1.1, 1.2))

def draw_charts(args):
    switch = {
        'th': draw_throughput_chart,
        'lt': draw_latency_chart,
        'src': draw_avgmetrics_per_source,
        'batch': draw_avgmetrics_per_batch,
        'all': lambda tests_path: (draw_throughput_chart(tests_path), draw_latency_chart(tests_path))
    }
    switch[args.chart_type](args.tests_path)

draw_charts(args)
