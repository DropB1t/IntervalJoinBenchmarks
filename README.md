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

## Synthetic Datesets
In order to generate the synthetic datasets you can compile and run the C++ tool located in `gen_dataset` folder. Further instructions for using the tool can be found in the inner [README](https://github.com/DropB1t/IntervalJoinBenchmarks/tree/main/gen_dataset) file.

## Real World Datasets
In order to run a benchmark, of each implementaion, with real datasets you need to download the [datasets.tar.gz](https://www.dropbox.com/scl/fi/y4qkcvci7yqcypg41tu85/datasets.tar.gz?rlkey=6o2d4byhx95d860pojddka4iq&dl=0) archive and unzip it by calling `tar -zvxf datasets.tar.gz` command into root of this repository. Otherwise you can run the `./download_datasets.sh` script located in `/scripts` folder.
