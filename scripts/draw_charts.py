"""
Draws various charts based on the provided command line arguments.
Parameters:
- chart_type (str): The type of chart to draw. Valid options are 'lt' for latency, 'th' for throughput, 'all' for both, 'src' for average performance per source, 'batch' for average performance per batch, or 'comparison' for comparing different execution modes.
- res_dir (str): Path to the results directory where the images will be saved. Required if chart_type is 'comparison'.
- kp_dir (str): Path to the key parallelism mode directory. Required if chart_type is 'comparison'.
- dp_dir (str): Path to the data parallelism mode directory. Required if chart_type is 'comparison'.
- fl_dir (str): Path to the Flink tests directory. Required if chart_type is 'comparison'.
- img_name (str): Name of the image file to generate. Required if chart_type is 'comparison'.
- tests_path (str): Path to the tests folders. Required if chart_type is not 'comparison'.
"""

import os
import json
import argparse

import matplotlib.pyplot as plt
from cycler import cycler
import numpy as np

def parse_arguments():
    parser = argparse.ArgumentParser(description='Draw latency, throughput, per batch, per source and comparison charts.')
    parser.add_argument('chart_type', type=str, choices=['lt', 'th', 'all', 'batch', 'src','comparison'], help='Chart type: "lt" for latency, "th" for throughput, "all" for both, "src" for average performence per source, "batch" for average performence per batch or "comparison" between all 3 execution mode ( kp, dp and flink modes )')
    
    # Initially parse known arguments to determine the mode
    args, unknown = parser.parse_known_args()
    
    if args.chart_type == 'comparison':
        parser.add_argument('res_dir', type=str, help='Path to the results directory were will be saved the images')
        parser.add_argument('kp_dir', type=str, help='Path to the key parallelism wf mode directory')
        parser.add_argument('dp_dir', type=str, help='Path to the data parallelism wf mode directory')
        parser.add_argument('fl_dir', type=str, help='Path to the flink tests directory')
        parser.add_argument('img_name', type=str, help='Name of the image file to generate')
    else:
        parser.add_argument('tests_path', type=str, help='Path to the tests folders')
    
    # Re-parse all arguments including the newly added ones
    args = parser.parse_args()
    return args

args = parse_arguments()

textwidth = 6
aspect_ratio = 6/8
scale = 1.0
width = textwidth * scale
height = width * aspect_ratio

#print(plt.rcParams.keys())
#print(width, height)

# Global Configuration

FIG_SIZE = (6.4, 4)
FIG_DPI = 300
FORMAT = 'pdf'

plt.style.use("seaborn-v0_8-paper")

main_cycler = (
    cycler(color=plt.rcParams['axes.prop_cycle'].by_key()['color'])[:4] +
    cycler(ls=['-', '-.', '--', ':']) +
    cycler(marker=['X', 'd', 's', 'o'])
)

plt.rcParams.update({
                    # Adjust to your LaTex-Engine
                    'font.size': 14,
                    "font.family": "sans-serif",
                    #"text.usetex": True,
                    "axes.grid": True,
                    "axes.grid.axis": "y",
                    "axes.axisbelow": True,
                    "axes.prop_cycle": main_cycler,
                    "grid.linestyle": "--",
                    "grid.linewidth": 1,
                    "grid.alpha": 0.5,
                    "legend.fancybox": False,
                    "legend.edgecolor": "black",
                    "legend.loc": "best",
                    "legend.fontsize": 7,
                    'figure.figsize': FIG_SIZE,
                    'figure.dpi': FIG_DPI,
                    'savefig.dpi': FIG_DPI,
                    'savefig.format': FORMAT,
                    'savefig.bbox': 'tight',
                    'savefig.pad_inches': 0.1
                    })

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
        latency_file = os.path.join(folder_path, 'latency.json')

        with open(latency_file, 'r') as file:
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
                       patch_artist=True,
                       medianprops=dict(linewidth=0),
                       meanprops=dict(linewidth=1.5, linestyle='-'),
                       flierprops=dict(marker='d', markersize=5, markerfacecolor='lightblue'))

    for i, mean in enumerate(y_line_points):
        ax.hlines(mean, i + 0.76, i + 1.24, colors='orange', linestyles='--', linewidth=1)

    ax.plot(x, y_line_points, color="orange", marker="o", markersize=5, ls='-', lw=1.5)

    _, max_y = ax.get_ylim()
    tick_interval = tick_interval = round(max_y / 15)
    ax.set_yticks(np.arange(0, max_y, tick_interval))
    ax.set(xlabel='OIJ Parallelism', ylabel='Latency (ms)')

    fig.savefig(os.path.join(tests_path, 'latency'))

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
    
    ax.bar(x = x, tick_label = x_labels, height = y, yerr=errors, edgecolor="black", error_kw=dict(lw=1, capsize=5, capthick=0.8, ecolor="orange"))
    ax.set(xlabel='OIJ Parallelism', ylabel='Throughput (tuples/s)')

    fig.savefig(os.path.join(tests_path, 'throughput'))

def draw_avgmetrics(label_prefix, avg_path, source_dir=''):
    # Get the list of batch folders, sorted
    iter_folders = sorted([folder for folder in os.listdir(avg_path) if os.path.isdir(os.path.join(avg_path, folder))], key=lambda x: int(x.split('_')[0]))
    # Define the colors for the lines and batches sizes for the legend labels
    label_values = sorted([int(folder.split('_')[0]) for folder in iter_folders])

    # Initialize the figure and axis for the plot
    fig, (lt, th) = plt.subplots(2, 1)
    # Initialize signgle plots
    lt_fig, lt_ax = plt.subplots()
    th_fig, th_ax = plt.subplots()

    # Iterate over the batch/source folders
    for i, i_folder in enumerate(iter_folders):
        # Initialize y list for the plots
        y_lt = []
        y_th = []

        # Get the list of parallelism folders, sorted
        dir_path = os.path.join(avg_path, i_folder, source_dir)
        parallelism_folders = sorted([folder for folder in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, folder))], key=lambda x: int(x.split('_')[0]))
        
        # Iterate over the parallelism folders
        for parallelism_folder in parallelism_folders:
            # Load the latency data from the file
            latency_file = os.path.join(dir_path, parallelism_folder, 'latency.json')
            with open(latency_file, 'r') as file:
                data = json.load(file)
                mean_latency = np.mean([entry['mean']/1000 for entry in data])
            # Append the mean latency to the y values
            y_lt.append(mean_latency)

            # Load the throughput data from the file
            throughput_file = os.path.join(dir_path, parallelism_folder, 'throughput.json')
            with open(throughput_file, 'r') as file:
                data = json.load(file)
                mean_throughput = np.mean(np.array(data), axis=0)

            # Append the mean throughput to the y values 
            y_th.append(mean_throughput)

        # Plot the lines per batch size per metric
        lt.plot(y_lt, label=label_prefix + " = " + str(label_values[i % len(label_values)]), markersize=5, lw=1.5)
        lt_ax.plot(y_lt, label=label_prefix + " = " + str(label_values[i % len(label_values)]), markersize=5, lw=1.5)

        th.plot(y_th, label=label_prefix + " = " + str(label_values[i % len(label_values)]), markersize=5, lw=1.5)
        th_ax.plot(y_th, label=label_prefix + " = " + str(label_values[i % len(label_values)]), markersize=5, lw=1.5)

    x_labels, _ = get_x_labels(parallelism_folders)
    if label_prefix == 'source':
        img_name = 'avg_source'
    else:
        img_name = 'avg_batch_' + source_dir

    lt_ax.legend(loc='best', ncols=4)
    lt_ax.set(xlabel='OIJ Parallelism', ylabel='Average Latency (ms)')

    _, max_y = lt_ax.get_ylim()
    tick_interval = round(max_y / 15)
    lt_ax.set_yticks(np.arange(0, max_y, tick_interval))
    lt_ax.set_xticks(range(len(x_labels)), x_labels)
    lt_fig.tight_layout()
    lt_fig.savefig(os.path.join(avg_path, (img_name+'_latency.'+FORMAT)))

    th_ax.legend(loc='best', ncols=4)
    th_ax.set(xlabel='OIJ Parallelism', ylabel='Average Throughput (tuples/s)')
    th_ax.set_xticks(range(len(x_labels)), x_labels)
    th_fig.tight_layout()
    th_fig.savefig(os.path.join(avg_path, (img_name+'_throughput.'+FORMAT)))

    save_merged_avg_figures(avg_path, img_name, fig, th, lt, x_labels)

def save_merged_avg_figures(path, img_name, fig, th, lt, x_labels):
    _, max_y = lt.get_ylim()
    tick_interval = round(max_y / 15)
    lt.set_yticks(np.arange(0, max_y, tick_interval))

    # Set the labels for the plots
    lt.set_xticks(range(len(x_labels)), x_labels)
    th.set_xticks(range(len(x_labels)), x_labels)

    lt.set(xlabel='OIJ Parallelism', ylabel='Average Latency (ms)')
    th.set(xlabel='OIJ Parallelism', ylabel='Average Throughput (tuples/s)')

    lt.legend(loc='best', ncols=4)
    th.legend(loc='best', ncols=4)

    fig.tight_layout()
    fig.set_size_inches((9,7))
    fig.savefig(os.path.join(path, img_name))

def draw_avgmetrics_per_batch(tests_path):
    batch = ''
    for item in os.listdir(tests_path):
        if not os.path.isdir(os.path.join(tests_path, item)):
            continue
        batch = item
        break
    if batch == '':
        raise Exception('Invalid test path in draw_avgmetrics_per_batch function')
    source_folders = sorted([folder for folder in os.listdir(os.path.join(tests_path, batch)) if os.path.isdir(os.path.join(tests_path, batch, folder))], key=lambda x: int(x.split('_')[0]))
    for source_folder in source_folders:
        draw_avgmetrics('batch', tests_path, source_folder)

def draw_comparison_charts(res_dir, kp_dir, dp_dir, fl_dir, img_name):
    # Get the list of batch folders, sorted
    iter_folders = sorted([folder for folder in os.listdir(kp_dir) if os.path.isdir(os.path.join(kp_dir, folder))], key=lambda x: int(x.split('_')[0]))

    x_labels, _ = get_x_labels(iter_folders)
    x = np.arange(len(x_labels))
    variant = ['Flink', 'Key Parallelism', 'Data Parallelism']
    bar_means = {
        'Flink': [],
        'Key Parallelism': [],
        'Data Parallelism': []
    }
    width = 0.25
    multiplier = 0

    for i, i_folder in enumerate(iter_folders):
        for j, folder in enumerate([fl_dir, kp_dir, dp_dir]):
            dir_path = os.path.join(folder, i_folder)
            throughput_file = os.path.join(dir_path, 'throughput.json')
            with open(throughput_file, 'r') as file:
                data = json.load(file)
                mean_throughput = np.mean(np.array(data), axis=0)
                bar_means[variant[j]].append(mean_throughput)

    fig, ax = plt.subplots()
    for attribute, means in bar_means.items():
        offset = width * multiplier
        _ = ax.bar(x + offset, means, width, label=attribute)
        #ax.bar_label(rects, fmt = '%d', padding=3)
        multiplier += 1
    
    ax.legend(loc='best', ncols=3)
    ax.set(xlabel='OIJ Parallelism', ylabel='Average Throughput (tuples/s)')
    ax.set_xticks(x + width, x_labels)
    fig.tight_layout()
    fig.savefig(os.path.join(res_dir, (img_name+'_throughput.'+FORMAT)))

    # Create the latency chart
    fig, ax = plt.subplots()
    y_lt = []

    for j, folder in enumerate([fl_dir, kp_dir, dp_dir]):
        for i, i_folder in enumerate(iter_folders):
            dir_path = os.path.join(folder, i_folder)
            latency_file = os.path.join(dir_path, 'latency.json')
            with open(latency_file, 'r') as file:
                data = json.load(file)
                mean_latency = np.mean([entry['mean']/1000 for entry in data])
                y_lt.append(mean_latency)
        ax.plot(y_lt, label=variant[j], markersize=8, lw=1.8)
        y_lt = []
    
    ax.legend(loc='best', ncols=3)
    ax.set(xlabel='OIJ Parallelism', ylabel='Average Latency (ms)')

    _, max_y = ax.get_ylim()
    tick_interval = round(max_y / 15)
    ax.set_yticks(np.arange(0, max_y, tick_interval))
    ax.set_xticks(range(len(x_labels)), x_labels)
    fig.tight_layout()
    fig.savefig(os.path.join(res_dir, (img_name+'_latency.'+FORMAT)))
    return

def draw_chart(args):
    if args.chart_type == 'comparison':
        draw_comparison_charts(args.res_dir, args.kp_dir, args.dp_dir, args.fl_dir, args.img_name)
    else:
        switch = {
                    'th': draw_throughput_chart,
                    'lt': draw_latency_chart,
                    'all': lambda tests_path: (draw_throughput_chart(tests_path), draw_latency_chart(tests_path)),
                    'src': lambda tests_path: draw_avgmetrics('source', tests_path),  
                    'batch': lambda tests_path: draw_avgmetrics_per_batch(tests_path)
                }
        switch[args.chart_type](args.tests_path)

draw_chart(args)
