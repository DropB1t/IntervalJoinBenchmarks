from ast import parse
import os
import json
import argparse

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

from tol_colors import tol_cset

def parse_arguments():
    parser = argparse.ArgumentParser(description='Draw latency, throughput, per batch, per source and comparison charts.')
    parser.add_argument('mode', type=str, choices=['wf', 'fl', 'comparison'], help='Benchmark: "windflow" tests, "flink" tests or "comparison" between all 3 execution mode ( kp, dp and flink modes ).')
    
    # Initially parse known arguments to determine the mode
    args, unknown = parser.parse_known_args()
    
    if args.mode == 'comparison':
        parser.add_argument('res_dir', type=str, help='Path to the results directory were will be saved the images')
        parser.add_argument('kp_dir', type=str, help='Path to the key partitioning wf mode directory')
        parser.add_argument('dp_dir', type=str, help='Path to the data partitioning wf mode directory')
        parser.add_argument('fl_dir', type=str, help='Path to the flink tests directory')
        parser.add_argument('img_name', type=str, help='Name of the image file to generate')
    else:
        parser.add_argument('chart_type', type=str, choices=['lt', 'th', 'all', 'batch', 'src'], help='Chart type: "lt" for latency, "th" for throughput, "all" for both, "src" for average performence per source, "batch" for average performence per batch')
        parser.add_argument('tests_path', type=str, help='Path to the tests folders')
    
    # Re-parse all arguments including the newly added ones
    args = parser.parse_args()
    return args

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
    fig.set_size_inches(14, 10, forward=True)
    fig.savefig(os.path.join(tests_path, 'latency.svg'), bbox_inches='tight', pad_inches=0.2)

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
    fig.set_size_inches(14, 10, forward=True)
    fig.savefig(os.path.join(tests_path, 'throughput.svg'), bbox_inches='tight', pad_inches=0.2)

def draw_avgmetrics(label_prefix, avg_path, source_dir=''):
    # Get the list of batch folders, sorted
    iter_folders = sorted([folder for folder in os.listdir(avg_path) if os.path.isdir(os.path.join(avg_path, folder))], key=lambda x: int(x.split('_')[0]))
    # Define the colors for the lines and batches sizes for the legend labels
    label_values = sorted([int(folder.split('_')[0]) for folder in iter_folders])
    colors = ['b', 'g', 'r' ,'y' ]

    # Initialize the figure and axis for the plot
    fig, (lt, th) = plt.subplots(2, 1)
    # Iterate over the batch folders
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
        lt.plot(y_lt, color=colors[i % len(colors)], label=label_prefix + " = " + str(label_values[i % len(label_values)]), marker="x", markersize=9, ls='-', lw=2)
        th.plot(y_th, color=colors[i % len(colors)], label=label_prefix + " = " + str(label_values[i % len(label_values)]), marker="x", markersize=9, ls='-', lw=2)

    x_labels, _ = get_x_labels(parallelism_folders)
    save_avg_figures(avg_path, ('batch_'+source_dir ), fig, th, lt, x_labels)

def draw_comparison_charts(res_dir, kp_dir, dp_dir, fl_dir, img_name):
    # Get the list of batch folders, sorted
    iter_folders = sorted([folder for folder in os.listdir(kp_dir) if os.path.isdir(os.path.join(kp_dir, folder))], key=lambda x: int(x.split('_')[0]))

    x_labels, _ = get_x_labels(iter_folders)
    x = np.arange(len(x_labels))
    variant = ['Flink', 'Key Partitioning', 'Data Partitioning']
    bar_means = {
        'Flink': [],
        'Key Partitioning': [],
        'Data Partitioning': []
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
        rects = ax.bar(x + offset, means, width, label=attribute)
        #ax.bar_label(rects, fmt = '%d', padding=3)
        multiplier += 1
    
    ax.set_ylabel('Average Throughput (tuples/s)', fontweight ='bold')
    ax.set_xticks(x + width, x_labels)
    ax.legend(loc='upper left', ncols=3)
    ax.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    ax.set_axisbelow(True)

    fig.set_dpi(100)
    fig.set_size_inches(14, 10, forward=True)
    fig.savefig(os.path.join(res_dir, (img_name+'_throughput.svg')), bbox_inches='tight', pad_inches=0.2)

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
        ax.plot(y_lt, label=variant[j], marker="x", markersize=9, ls='-', lw=2)
        y_lt = []
    
    ax.set_ylabel('Average Latency (ms)', fontweight ='bold')
    ax.set_xticks(range(len(x_labels)), x_labels)
    ax.legend(loc='upper left', ncols=3)
    ax.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    ax.set_axisbelow(True)

    fig.set_dpi(100)
    fig.set_size_inches(14, 10, forward=True)
    fig.savefig(os.path.join(res_dir, (img_name+'_latency.svg')), bbox_inches='tight', pad_inches=0.2)
    return

def save_avg_figures(path, img_name, fig, th, lt, x_labels, dpi=100, size=(14, 10)):
    _, max_y = lt.get_ylim()
    tick_interval = round(max_y / 15)
    lt.set_yticks(np.arange(0, max_y, tick_interval))
    lt.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    lt.set_axisbelow(True)

    th.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    th.set_axisbelow(True)

    # Set the labels for the plot
    lt.set_xticks(range(len(x_labels)), x_labels)
    th.set_xticks(range(len(x_labels)), x_labels)
    plt.xlabel('Parallelism')

    lt.set_ylabel('Average Latency (ms)',fontweight ='bold')
    th.set_ylabel('Average Throughput (tuples/s)',fontweight ='bold')
    lt.legend()

    fig.set_dpi(dpi)
    fig.set_size_inches(size, forward=True)
    fig.savefig(os.path.join(path, (img_name+'.svg')), bbox_inches='tight', pad_inches=0.2)

    th.legend()
    th_extent = th.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(os.path.join(path, (img_name+'_throughput.svg')), bbox_inches=th_extent.expanded(1.1, 1.2))

    lt_extent = lt.get_window_extent().transformed(fig.dpi_scale_trans.inverted())
    fig.savefig(os.path.join(path, (img_name+'_latency.svg')), bbox_inches=lt_extent.expanded(1.1, 1.2))

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

def draw_charts(args):
    if args.mode == 'comparison':
        draw_comparison_charts(args.res_dir, args.kp_dir, args.dp_dir, args.fl_dir, args.img_name)
    else:
        switch = {
                    'th': draw_throughput_chart,
                    'lt': draw_latency_chart,
                    'all': lambda tests_path: (draw_throughput_chart(tests_path), draw_latency_chart(tests_path)),
                    'src': lambda tests_path: draw_avgmetrics('source', tests_path),  
                    'batch': lambda tests_path: [draw_avgmetrics('batch', tests_path, source_dir) for source_dir in ['1_source', '2_source', '3_source', '4_source']]
                }
        switch[args.chart_type](args.tests_path)

draw_charts(args)
