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
    parser.add_argument('chart_type', type=str, choices=['lt', 'th', 'all'], help='Type of chart: "latency", "throughput", or "all"')
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

x = [1 ,2, 3, 4, 5, 6, 7]
x_labels = ['1', '2', '4', '6', '8', '16', '32']  # Set the x-axis labels

def draw_latency_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))], key=lambda x: int(x.split('_')[1].split('_')[0]))
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
    
    boxes = ax.boxplot(y, labels=x_labels, showfliers=True, showmeans=False, meanline=False,
                       patch_artist=True, boxprops=dict(facecolor=main_color),
                       medianprops=dict(color=secondary_color, linewidth=1),
                       meanprops=dict(color=error_color, linewidth=1.4, linestyle='-'))
    
    #y_line_points = [item.get_ydata()[0] for item in boxes['means']]
    ax.plot(x, y_line_points, marker="o", markersize=5, color=error_color, ls='-', lw=1.4)

    #start, end = ax.get_ylim()
    locs, _ = plt.yticks()
    max_y = max(locs)
    tick_interval = max_y / 15
    #print(locs)

    ax.set_yticks(np.arange(0, max_y, tick_interval))
    ax.grid(True, axis="y", ls='--', lw=1, alpha=.5)

    ax.set_axisbelow(True)
    fig.set_size_inches(18.5, 10.5, forward=True)
    fig.set_dpi(100)

    #plt.title('Latency chart')
    plt.xlabel('Parallelism')
    plt.ylabel('Latency (ms)')
    #plt.show()

    fig.savefig(os.path.join(tests_path, 'latency.svg'), dpi=100)
    #fig.savefig(os.path.join(tests_path, 'latency.png'), dpi=100)

def draw_throughput_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))], key=lambda x: int(x.split('_')[1].split('_')[0]))

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

    fig.set_size_inches(18.5, 10.5, forward=True)
    fig.set_dpi(100)

    #plt.title('Throughput Chart')
    plt.xlabel('Parallelism')
    plt.ylabel('Throughput (tuples/s)')
    #plt.show()

    fig.savefig(os.path.join(tests_path, 'throughput.svg'), dpi=100)
    #fig.savefig(os.path.join(tests_path, 'throughput.png'), dpi=100)

def draw_charts(args):
    switch = {
        'th': draw_throughput_chart,
        'lt': draw_latency_chart,
        'all': lambda tests_path: (draw_throughput_chart(tests_path), draw_latency_chart(tests_path))
    }
    switch[args.chart_type](args.tests_path)

draw_charts(args)
