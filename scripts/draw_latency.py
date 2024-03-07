import os
import json
import argparse

import matplotlib.pyplot as plt
import numpy as np

from tol_colors import tol_cset

cset = tol_cset('bright')

# (Absolute) path example '/home/dropbit/dev/thesis/IntervalJoinBenchmarks/results/wf/synthetic_1s/k_mode/0_batch_su/100_keys/'
def parse_arguments():
    parser = argparse.ArgumentParser(description='Draw throughput chart.')
    parser.add_argument('tests_path', type=str, help='Path to the tests folders')
    return parser.parse_args()

args = parse_arguments()

def draw_latency_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))])

    y = []
    y_line_points = []
    x_line_points = [1 ,2, 3, 4, 5, 6]
    x_labels = ['1', '2', '4', '8', '16', '32']  # Set the x-axis labels
    fig, ax = plt.subplots()

    for folder in folders:
        folder_path = os.path.join(tests_path, folder)
        throughput_file = os.path.join(folder_path, 'latency.json')

        with open(throughput_file, 'r') as file:
            data = json.load(file)

            lower_whiskers = np.mean([entry['0'] for entry in data])
            _5th_percentile = np.mean([entry['5'] for entry in data])
            lower_quartiles = np.mean([entry['25'] for entry in data])
            medians = np.mean([entry['50'] for entry in data])
            upper_quartiles = np.mean([entry['75'] for entry in data])
            _95th_percentile = np.mean([entry['95'] for entry in data])
            upper_whiskers = np.mean([entry['100'] for entry in data])
            
            #means = np.mean([entry['mean'] for entry in data])
            #y_line_points.append(means)

            box = np.array([lower_whiskers, _5th_percentile, lower_quartiles, medians, upper_quartiles, _95th_percentile, upper_whiskers])
            y.append(box)
    
    boxes = ax.boxplot(y, labels=x_labels, showfliers=False, showmeans=True, meanline=True,
                       patch_artist=True, boxprops=dict(facecolor=cset.blue),
                       medianprops=dict(color=cset.cyan, linewidth=1),
                       meanprops=dict(color=cset.red, linewidth=1.4, linestyle='-'))
    
    y_line_points = [item.get_ydata()[0] for item in boxes['means']]
    ax.plot(x_line_points, y_line_points, marker="o", markersize=4, color=cset.red, ls='--', lw=1.4)

    ax.grid(True, axis="y", ls='--', lw=1, alpha=.8)
    ax.set_axisbelow(True)

    fig.set_size_inches(18.5, 10.5, forward=True)
    fig.set_dpi(100)

    plt.xlabel('Parallelism')
    plt.ylabel('Latency (ms)')
    #plt.title('Latency chart')
    #plt.show()

    fig.savefig(os.path.join(tests_path, 'latency.svg'), dpi=100)
    fig.savefig(os.path.join(tests_path, 'latency.png'), dpi=100)

draw_latency_chart(args.tests_path)
