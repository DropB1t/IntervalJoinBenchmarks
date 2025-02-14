#!/bin/bash
# Launcher of the sixth series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/series_6

# Tests KP
echo "Running tests with KP mode..."
mkdir -p ${MYDIR}/results/series_6/kp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for s in 0.20 0.30 0.35 0.40 0.45 0.50; do
        for t in 1 2 3 4 5; do
            echo "Test KP with interval ${i}, skewness ${s}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode k -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode k -l -${i} -u ${i} &> output-i${i}-s${s}_t${t}.log
            th=$(grep "throughput" output-i${i}-s${s}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-s${s}_t${t}.log ${MYDIR}/results/series_6/kp/output-i${i}-s${s}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_6/kp/bw-i${i}_s${s}.log
            mv latency.json ${MYDIR}/results/series_6/kp/latency-i${i}_s${s}_t${t}.log
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests DP
echo "Running tests with DP mode..."
mkdir -p ${MYDIR}/results/series_6/dp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for s in 0.20 0.30 0.35 0.40 0.45 0.50; do
        for t in 1 2 3 4 5; do
            echo "Test DP with interval ${i}, skewness ${s}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode d -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode d -l -${i} -u ${i} &> output-i${i}-s${s}_t${t}.log
            th=$(grep "throughput" output-i${i}-s${s}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-s${s}_t${t}.log ${MYDIR}/results/series_6/dp/output-i${i}-s${s}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_6/dp/bw-i${i}_s${s}.log
            mv latency.json ${MYDIR}/results/series_6/dp/latency-i${i}_s${s}_t${t}.log
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests HYBRID
echo "Running tests with HYBRID mode..."
mkdir -p ${MYDIR}/results/series_6/hp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for s in 0.20 0.30 0.35 0.40 0.45 0.50; do
        for t in 1 2 3 4 5; do
            echo "Test HYBRID with interval ${i}, skewness ${s}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode h -h 8 -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode h -h 8 -l -${i} -u ${i} &> output-i${i}-s${s}_t${t}.log
            th=$(grep "throughput" output-i${i}-s${s}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-s${s}_t${t}.log ${MYDIR}/results/series_6/hp/output-i${i}-s${s}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_6/hp/bw-i${i}_s${s}.log
            mv latency.json ${MYDIR}/results/series_6/hp/latency-i${i}_s${s}_t${t}.log
        done
    done
done

cd ${MYDIR}/scripts
echo "...end"
