import os
import json
import argparse

import matplotlib.pyplot as plt
import numpy as np

from tol_colors import tol_cset

cset = tol_cset('bright')
#print(cset._fields)

# (Absolute) path example '/home/dropbit/dev/thesis/IntervalJoinBenchmarks/results/wf/synthetic_1s/k_mode/0_batch_su/100_keys/'
def parse_arguments():
    parser = argparse.ArgumentParser(description='Draw throughput chart.')
    parser.add_argument('tests_path', type=str, help='Path to the tests folders')
    return parser.parse_args()

args = parse_arguments()

def draw_throughput_chart(tests_path):
    folders = sorted([folder for folder in os.listdir(tests_path) if os.path.isdir(os.path.join(tests_path, folder))])

    y = [] 
    errors = []
    x = [1,2,3,4,5,6]
    x_labels = ['1', '2', '4', '8', '16', '32']  # Set the x-axis labels
    fig, ax = plt.subplots()

    for folder in folders:
        folder_path = os.path.join(tests_path, folder)
        throughput_file = os.path.join(folder_path, 'throughput.json')

        with open(throughput_file, 'r') as file:
            data = json.load(file)
            throughput = np.array(data)
            y.append(np.mean(throughput, axis=0))
            errors.append(np.std(throughput, axis=0))
    
    ax.bar(x = x, tick_label = x_labels, height = y, yerr=errors, capsize = 10, ecolor = cset.red, color = cset.blue)
    ax.grid(True, axis = "y", ls='--', lw=1, alpha=.8 )
    ax.set_axisbelow(True)
    #ax.set_yscale('log')

    fig.set_size_inches(18.5, 10.5, forward=True)
    fig.set_dpi(100)

    plt.xlabel('Parallelism')
    plt.ylabel('Throughput (tuples/s)')
    plt.title('Throughput Chart')
    #plt.show()

    fig.savefig(os.path.join(tests_path, 'throughput.png'), dpi=100)
    fig.savefig(os.path.join(tests_path, 'throughput.svg'), dpi=100)

draw_throughput_chart(args.tests_path)
