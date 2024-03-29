import os
import json
import argparse

import matplotlib.pyplot as plt
import numpy as np

from tol_colors import tol_cset

# (Absolute) path example '{REPO_DIR}/results/wf/synthetic_1s/k_mode/0_batch_su/100_keys/'
def parse_arguments():
    parser = argparse.ArgumentParser(description='Draw latency or throughput chart.')
    parser.add_argument('tests_path', type=str, help='Path to the tests folders')
    parser.add_argument('mode', type=str, choices=['wf', 'fl'], help='Benchmark: "windflow" or "flink"')
    parser.add_argument('chart_type', type=str, choices=['lt', 'th'], help='Type of chart: "latency" or "throughput"')
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

def draw_latency_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))])
    y = []
    y_line_points = []
    x = [1 ,2, 3, 4, 5, 6, 7, 8, 9, 10]
    x_labels = ['1', '2', '3', '4', '5', '6', '7', '8', '16', '32']  # Set the x-axis labels
    fig, ax = plt.subplots()

    for folder in folders:
        folder_path = os.path.join(tests_path, folder)
        throughput_file = os.path.join(folder_path, 'latency.json')

        with open(throughput_file, 'r') as file:
            data = json.load(file)

            #Dividing by 1000 to convert from us to ms
            lower_whiskers = sorted([entry['0']/1000 for entry in data])
            _5th_percentile = sorted([entry['5']/1000 for entry in data])
            lower_quartile = sorted([entry['25']/1000 for entry in data])
            _50th_percentile = sorted([entry['50']/1000 for entry in data])
            upper_quartile = sorted([entry['75']/1000 for entry in data])
            _95th_percentile = sorted([entry['95']/1000 for entry in data])
            upper_whiskers = sorted([entry['100']/1000 for entry in data])

            print(folder)
            print(upper_whiskers)
            
            #means = ([entry['mean'] for entry in data])
            #y_line_points.append(means)
            
            box = np.array([lower_whiskers, _5th_percentile, lower_quartile, _50th_percentile, upper_quartile, _95th_percentile, upper_whiskers])
            y.append(box.flatten())
    
    boxes = ax.boxplot(y, labels=x_labels, showfliers=False, showmeans=True, meanline=True,
                       patch_artist=True, boxprops=dict(facecolor=main_color),
                       medianprops=dict(color=secondary_color, linewidth=1),
                       meanprops=dict(color=error_color, linewidth=1.4, linestyle='-'))
    
    y_line_points = [item.get_ydata()[0] for item in boxes['means']]
    ax.plot(x, y_line_points, marker="o", markersize=4, color=error_color, ls='--', lw=1.4)

    ax.grid(True, axis="y", ls='--', lw=1, alpha=.8)
    ax.set_axisbelow(True)

    fig.set_size_inches(18.5, 10.5, forward=True)
    fig.set_dpi(100)

    plt.xlabel('Parallelism')
    plt.ylabel('Latency (ms)')
    #plt.title('Latency chart')
    #plt.show()

    fig.savefig(os.path.join(tests_path, 'latency.svg'), dpi=100)
    #fig.savefig(os.path.join(tests_path, 'latency.png'), dpi=100)

def draw_throughput_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))])

    y = [] 
    errors = []
    x = [1 ,2, 3, 4, 5, 6, 7, 8, 9, 10]
    x_labels = ['1', '2', '3', '4', '5', '6', '7', '8', '16', '32']  # Set the x-axis labels
    fig, ax = plt.subplots()

    for folder in folders:
        folder_path = os.path.join(tests_path, folder)
        throughput_file = os.path.join(folder_path, 'throughput.json')

        with open(throughput_file, 'r') as file:
            data = json.load(file)
            throughput = np.array(data)
            y.append(np.mean(throughput, axis=0))
            errors.append(np.std(throughput, axis=0))
    
    ax.bar(x = x, tick_label = x_labels, height = y, color = main_color, yerr=errors, ecolor = error_color, error_kw=dict(lw=2, capsize=10, capthick=1.5))
    ax.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    ax.set_axisbelow(True)
    #ax.set_yscale('log')

    fig.set_size_inches(18.5, 10.5, forward=True)
    fig.set_dpi(100)

    plt.xlabel('Parallelism')
    plt.ylabel('Throughput (tuples/s)')
    #plt.title('Throughput Chart')
    #plt.show()

    fig.savefig(os.path.join(tests_path, 'throughput.svg'), dpi=100)
    #fig.savefig(os.path.join(tests_path, 'throughput.png'), dpi=100)

if(args.chart_type == 'th'):
    draw_throughput_chart(args.tests_path)
elif(args.chart_type == 'lt'):
    draw_latency_chart(args.tests_path)
