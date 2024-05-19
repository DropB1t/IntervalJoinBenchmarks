# Benchmarking Suite - Interval Join performance evaluation

This repository contains a benchmark suite for evaluation of Interval Join operation respectfully for [Flink](https://flink.apache.org/) and [Windflow](https://paragroup.github.io/WindFlow/) implementation.

## WindFlow Dependencies

In order to run the Flink implementation in this project, the following dependencies are needed:
* a C++ compiler with full support for **C++17** like **GCC** (GNU Compiler Collection) version >= 8 or **Clang 5** and later
* [WindFlow](https://github.com/ParaGroup/WindFlow/releases) library version >= 4.0.0 and all the required dependencies
* [FastFlow](https://github.com/fastflow/fastflow/releases) library version >= 3.0 and all the required dependencies
* [RapidJSON](https://rapidjson.org/) parser for C++. You can install the package on **Ubuntu** by running `sudo apt-get install -y rapidjson-dev` command.

## Flink Dependencies

Whole suite can be run on local machine, further information can be found in the [official documentation](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/dataset/local_execution/) and in internal [README](https://github.com/DropB1t/IntervalJoinBenchmarks/tree/main/flink) file.

In order to run the Flink implementation in this project, the following dependencies are needed:
* [Apache Flink](https://nightlies.apache.org/flink/flink-docs-release-1.16/) version >= 1.16.0
* [Java JDK](https://openjdk.java.net/install/) version >= 1.11
* [Maven](https://maven.apache.org/install.html) version >= 3.9.6

## Run Benchmark script
You can generate whole test cases simply by runnig the `run_benchmarks.sh` located in `/scripts` folder. The provided script cycles through all parameters at lines [`64:78`](https://github.com/DropB1t/IntervalJoinBenchmarks/blob/main/scripts/run_benchmarks.sh#L64-L78) and generates *throughput* and *latency* charts by running `draw_charts.py` python tool.

In order to install the `draw_charts.py` dependencies you need to have on your system [**pip**](https://github.com/pypa/pip): the package installer for Python.
After that simply run the following command inside the `/scripts` folder:

```
pip install -r requirements.txt
```

### Results folder structure example
```
.
└── results/
    └── {benchmark_suite}/
        └── {dataset_type}/
            └── [{partitiong_mode}]/
                └── [{synthetic_keys_number}]/
                    └── [{batching_size}]/
                        ├── {source_degree}/
                        │   ├── {1_test_1}/
                        │   │   ├── ...
                        │   │   ├── latency.svg
                        │   │   └── throughput.svg
                        │   ├── {2_test_2}
                        │   ├── {3_test_4}
                        │   ├── {4_test_6}
                        │   └── source.svg
                        └── [batch.svg]
```


## Synthetic Datesets
In order to generate the synthetic datasets you can compile and run the C++ tool located in `/gen_dataset` folder. Further instructions for using the tool can be found in the inner [README](https://github.com/DropB1t/IntervalJoinBenchmarks/tree/main/gen_dataset) file.

## Real World Datasets
In order to run a benchmark, of each implementaion, with real datasets you need to download the [datasets.tar.gz](https://www.dropbox.com/scl/fi/y4qkcvci7yqcypg41tu85/datasets.tar.gz?rlkey=6o2d4byhx95d860pojddka4iq&dl=0) archive and unzip it by calling `tar -zvxf datasets.tar.gz` command into root of this repository. Otherwise you can run the `./download_datasets.sh` script located in `/scripts` folder.

Credits for datasets go to [AllianceDB](https://github.com/intellistream/AllianceDB/blob/master/docs/README.md) project.

## Latency & Throughput Chart Example
![Latency Chart Example](./imgs/latency.svg)
![Throughput Chart Example](./imgs/throughput.svg)

