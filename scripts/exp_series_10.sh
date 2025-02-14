#!/bin/bash
# Launcher of the tenth series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/series_10

# # Tests WindFlow
# echo "Running tests with WindFlow..."
# mkdir -p ${MYDIR}/results/series_10/windflow
# cd ${MYDIR}/comparison_wf
# for i in 5000; do
#     for k in 10 100 1000; do
#         for b in 0 2 4 6 8 16 32; do
#             for p in 1 2 4 8 12 16 24 32; do
#                 for t in 1 2 3 4 5; do
#                     echo "Test WindFlow with interval ${i}, keys ${k}, batch ${b}, parallelism ${p}, repetition # ${t}"
#                     echo "./bin/test_comparison -b ${b} -p 1,1,${p},${p} -k ${k} -w ${i} -c"
#                     ./bin/test_comparison -b ${b} -p 1,1,${p},${p} -k ${k} -w ${i} -c &> output-i${i}-k${k}_b${b}_p${p}_t${t}.log
#                     th=$(grep "throughput" output-i${i}-k${k}_b${b}_p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
#                     echo "Measured throughput: ${th}"
#                     # Save results
#                     mv output-i${i}-k${k}_b${b}_p${p}_t${t}.log ${MYDIR}/results/series_10/windflow/output-i${i}-k${k}_b${b}_p${p}_t${t}.log
#                     echo "$th" >> ${MYDIR}/results/series_10/windflow/bw-i${i}-k${k}_b${b}_p${p}.log
#                     mv all_latencies.csv ${MYDIR}/results/series_10/windflow/latencies-i${i}-k${k}_b${b}_p${p}_t${t}.csv
#                 done
#             done
#         done
#     done
# done

# cd ${MYDIR}/Scripts
# echo "...end"

# Tests Flink
echo "Running tests with Flink..."
mkdir -p ${MYDIR}/results/series_10/flink
cd ${MYDIR}/comparison_flink
for i in 5000; do
    for k in 100 1000; do
        for p in 1 2 4 8 12 16 24 32; do
            for t in 1 2 3 4 5; do
                echo "Test Flink with interval ${i}, keys ${k}, parallelism ${p}, repetition # ${t}"
                echo "java -jar target/Comparison_Flink-1.0.jar --parallelism 1,1,${p},${p} --keys ${k} --win ${i}"
                java -jar target/Comparison_Flink-1.0.jar --parallelism 1,1,${p},${p} --keys ${k} --win ${i} &> output-i${i}-k${k}_p${p}_t${t}.log
                th=$(grep "throughput" output-i${i}-k${k}_p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
                echo "Measured throughput: ${th}"
                # Save results
                mv output-i${i}-k${k}_p${p}_t${t}.log ${MYDIR}/results/series_10/flink/output-i${i}-k${k}_p${p}_t${t}.log
                echo "$th" >> ${MYDIR}/results/series_10/flink/bw-i${i}-k${k}_p${p}.log
                mv all_latencies.csv ${MYDIR}/results/series_10/flink/latencies-i${i}-k${k}_p${p}_t${t}.csv
            done
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# # Tests ScaleOIJ
# echo "Running tests with ScaleOIJ..."
# mkdir -p ${MYDIR}/results/series_10/scaleoij
# cd /home/mencagli/OpenMLDB
# for i in 5000; do
#     for k in 10 100 1000; do
#         for p in 1 2 4 8 12 16 24 32; do
#             for t in 1 2 3 4 5; do
#                 echo "Test ScaleOIJ with interval ${i}, keys ${k}, parallelism ${p}, repetition # ${t}"
#                 echo "./build/bin/interval_join -base_stream_count 1000000 -probe_stream_count 2000000 -join_strategy dynamic -joiner_thread ${p} -key_num ${k} -op_type count -window ${i}"
#                 ./build/bin/interval_join -base_stream_count 1000000 -probe_stream_count 2000000 -join_strategy dynamic -joiner_thread ${p} -key_num ${k} -op_type count -window ${i} &> output-i${i}-k${k}_p${p}_t${t}.log
#                 th=$(grep "throughput" output-i${i}-k${k}_p${p}_t${t}.log | cut -f2 -d " " | awk '{ sum += $1 } END { print int(sum) }')
#                 echo "Measured throughput: ${th}"
#                 # Save results
#                 mv output-i${i}-k${k}_p${p}_t${t}.log ${MYDIR}/results/series_10/scaleoij/output-i${i}-k${k}_p${p}_t${t}.log
#                 echo "$th" >> ${MYDIR}/results/series_10/scaleoij/bw-i${i}-k${k}_p${p}.log
#                 mv latency.csv ${MYDIR}/results/series_10/scaleoij/latencies-i${i}-k${k}_p${p}_t${t}.csv
#             done
#         done
#     done
# done

# cd ${MYDIR}/Scripts
# echo "...end"
