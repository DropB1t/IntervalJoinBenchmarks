#!/usr/bin/env bash
# shellcheck disable=SC2034

# run_benchmarks configuration file
SAMPLING=250

parallelism=( 1 2 4 8 16 )
source_degrees=( 1 4 )
num_key=( 1 )
batch_size=( 32 )

lower_bounds=( -500 )
upper_bounds=( 500 )

exec_mode=( k d )
real_type=( rd sd )

synt_type=( su sz )
zipfian_skews=( 0.0 )
