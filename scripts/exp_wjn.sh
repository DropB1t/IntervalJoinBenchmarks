#!/bin/bash
# Launcher of the first WJN series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/wjn

# Tests KP
echo "Running tests with KP mode..."
mkdir -p ${MYDIR}/results/wjn/kp
cd ${MYDIR}
for w in 1000 5000 10000 15000 20000 25000 30000; do
    for t in 1 2 3 4 5; do
        echo "Test KP with window ${w}, repetition # ${t}"
        echo "./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode k -w ${w} -s 1000 --chaining"
        ./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode k -w ${w} -s 1000 --chaining &> output-w${w}_t${t}.log
        th=$(grep "throughput" output-w${w}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
        echo "Measured throughput: ${th}"
        # Save results
        mv output-w${w}_t${t}.log ${MYDIR}/results/wjn/kp/output-w${w}_t${t}.log
        echo "$th" >> ${MYDIR}/results/wjn/kp/bw-w${w}.log
        mv metric_latency.json ${MYDIR}/results/wjn/kp/latency-w${i}_t${t}.log
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests DP
echo "Running tests with DP mode..."
mkdir -p ${MYDIR}/results/wjn/dp
cd ${MYDIR}
for w in 1000 5000 10000 15000 20000 25000 30000; do
    for t in 1 2 3 4 5; do
        echo "Test DP with window ${w}, repetition # ${t}"
        echo "./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode d -w ${w} -s 1000 --chaining"
        ./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode d -w ${w} -s 1000 --chaining &> output-w${w}_t${t}.log
        th=$(grep "throughput" output-w${w}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
        echo "Measured throughput: ${th}"
        # Save results
        mv output-w${w}_t${t}.log ${MYDIR}/results/wjn/dp/output-w${w}_t${t}.log
        echo "$th" >> ${MYDIR}/results/wjn/dp/bw-w${w}.log
        mv metric_latency.json ${MYDIR}/results/wjn/dp/latency-w${i}_t${t}.log
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests HYBRID
echo "Running tests with HP mode..."
mkdir -p ${MYDIR}/results/wjn/hp
cd ${MYDIR}
for w in 1000 5000 10000 15000 20000 25000 30000; do
    for t in 1 2 3 4 5; do
        echo "Test HP with window ${w}, repetition # ${t}"
        echo "./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode h -h 8 -w ${w} -s 1000 --chaining"
        ./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s0.35.txt --mode h -h 8 -w ${w} -s 1000 --chaining &> output-w${w}_t${t}.log
        th=$(grep "throughput" output-w${w}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
        echo "Measured throughput: ${th}"
        # Save results
        mv output-w${w}_t${t}.log ${MYDIR}/results/wjn/hp/output-w${w}_t${t}.log
        echo "$th" >> ${MYDIR}/results/wjn/hp/bw-w${w}.log
        mv metric_latency.json ${MYDIR}/results/wjn/hp/latency-w${i}_t${t}.log
    done
done

cd ${MYDIR}/Scripts
echo "...end"
