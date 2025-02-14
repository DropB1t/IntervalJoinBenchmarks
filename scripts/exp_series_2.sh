#!/bin/bash
# Launcher of the second series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/series_2

# Tests KP
echo "Running tests with KP mode..."
mkdir -p ${MYDIR}/results/series_2/kp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for k in 1 2 3 4 5 10 25 50 100 250 500 1000; do
        for t in 1 2 3 4 5; do
            echo "Test KP with interval ${i}, keys ${k}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k${k}_s0.35.txt --mode k -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k${k}_s0.35.txt --mode k -l -${i} -u ${i} &> output-i${i}-k${k}_t${t}.log
            th=$(grep "throughput" output-i${i}-k${k}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-k${k}_t${t}.log ${MYDIR}/results/series_2/kp/output-i${i}-k${k}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_2/kp/bw-i${i}_k${k}.log
            mv latency.json ${MYDIR}/results/series_2/kp/latency-i${i}_k${k}_t${t}.log
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests DP
echo "Running tests with DP mode..."
mkdir -p ${MYDIR}/results/series_2/dp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for k in 1 2 3 4 5 10 25 50 100 250 500 1000; do
        for t in 1 2 3 4 5; do
            echo "Test DP with interval ${i}, keys ${k}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k${k}_s0.35.txt --mode d -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k${k}_s0.35.txt --mode d -l -${i} -u ${i} &> output-i${i}-k${k}_t${t}.log
            th=$(grep "throughput" output-i${i}-k${k}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-k${k}_t${t}.log ${MYDIR}/results/series_2/dp/output-i${i}-k${k}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_2/dp/bw-i${i}_k${k}.log
            mv latency.json ${MYDIR}/results/series_2/dp/latency-i${i}_k${k}_t${t}.log
        done
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests HYBRID
echo "Running tests with HYBRID mode..."
mkdir -p ${MYDIR}/results/series_2/hp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for k in 1 2 3 4 5 10 25 50 100 250 500 1000; do
        for t in 1 2 3 4 5; do
            echo "Test HYBRID with interval ${i}, keys ${k}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k${k}_s0.35.txt --mode h -h 8 -e 1.2 -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,8,8 -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k${k}_s0.35.txt --mode h -h 8 -e 1.2 -l -${i} -u ${i} &> output-i${i}-k${k}_t${t}.log
            th=$(grep "throughput" output-i${i}-k${k}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-k${k}_t${t}.log ${MYDIR}/results/series_2/hp/output-i${i}-k${k}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_2/hp/bw-i${i}_k${k}.log
            mv latency.json ${MYDIR}/results/series_2/hp/latency-i${i}_k${k}_t${t}.log
        done
    done
done

cd ${MYDIR}/scripts
echo "...end"
