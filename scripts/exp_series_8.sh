#!/bin/bash
# Launcher of the eighth series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/series_8

# Tests KP
echo "Running tests with KP mode..."
mkdir -p ${MYDIR}/results/series_8/kp
cd ${MYDIR}
for i in 10000 30000; do
    for b in 0 2 4 8 16 32 64 128; do
        for t in 1 2 3 4 5; do
            echo "Test KP with interval ${i}, batch ${b}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch ${b} --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode k -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch ${b} --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode k -l -${i} -u ${i} &> output-i${i}-b${b}_t${t}.log
            th=$(grep "throughput" output-i${i}-b${b}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-b${b}_t${t}.log ${MYDIR}/results/series_8/kp/output-i${i}-b${b}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_8/kp/bw-i${i}_b${b}.log
            mv latency.json ${MYDIR}/results/series_8/kp/latency-i${i}_b${b}_t${t}.log
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests DP
echo "Running tests with DP mode..."
mkdir -p ${MYDIR}/results/series_8/dp
cd ${MYDIR}
for i in 10000 30000; do
    for b in 0 2 4 8 16 32 64 128; do
        for t in 1 2 3 4 5; do
            echo "Test DP with interval ${i}, batch ${b}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch ${b} --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode d -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch ${b} --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode d -l -${i} -u ${i} &> output-i${i}-b${b}_t${t}.log
            th=$(grep "throughput" output-i${i}-b${b}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-b${b}_t${t}.log ${MYDIR}/results/series_8/dp/output-i${i}-b${b}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_8/dp/bw-i${i}_b${b}.log
            mv latency.json ${MYDIR}/results/series_8/dp/latency-i${i}_b${b}_t${t}.log
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests HYBRID
echo "Running tests with HYBRID mode..."
mkdir -p ${MYDIR}/results/series_8/hp
cd ${MYDIR}
for i in 10000 30000; do
    for b in 0 2 4 8 16 32 64 128; do
        for t in 1 2 3 4 5; do
            echo "Test HYBRID with interval ${i}, batch ${b}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch ${b} --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode h -h 8 -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch ${b} --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode h -h 8 -l -${i} -u ${i} &> output-i${i}-b${b}_t${t}.log
            th=$(grep "throughput" output-i${i}-b${b}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-b${b}_t${t}.log ${MYDIR}/results/series_8/hp/output-i${i}-b${b}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_8/hp/bw-i${i}_b${b}.log
            mv latency.json ${MYDIR}/results/series_8/hp/latency-i${i}_b${b}_t${t}.log
        done
    done
done

cd ${MYDIR}/scripts
echo "...end"
