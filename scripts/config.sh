#!/usr/bin/env bash

# run_benchmarks configuration file
SAMPLING=250

parallelism=( 1 2 4 6 8 16 32 )
source_degrees=( 1 2 3 4 )
num_key=( 1000 10000 )
batch_size=( 0 16 32 )

lower_bounds=( -500 )
upper_bounds=( 500 )

exec_mode=( k d )
real_type=( rd sd )

synt_type=( su sz )
zipfian_skews=( 0.0 0.9 )
