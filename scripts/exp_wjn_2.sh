#!/bin/bash
# Launcher of the second WJN series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/wjn2

# Tests KP
echo "Running tests with KP mode..."
mkdir -p ${MYDIR}/results/wjn2/kp
cd ${MYDIR}
for s in 0.10 0.20 0.30 0.40 0.50; do
    for t in 1 2 3 4 5; do
        echo "Test KP with skewneess ${s}, repetition # ${t}"
        echo "./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode k -w 20000 -s 1000 --chaining"
        ./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode k -w 20000 -s 1000 --chaining &> output-s${s}_t${t}.log
        th=$(grep "throughput" output-s${s}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
        echo "Measured throughput: ${th}"
        # Save results
        mv output-s${s}_t${t}.log ${MYDIR}/results/wjn2/kp/output-s${s}_t${t}.log
        echo "$th" >> ${MYDIR}/results/wjn2/kp/bw-s${s}.log
        mv metric_latency.json ${MYDIR}/results/wjn2/kp/latency-s${s}_t${t}.log
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests DP
echo "Running tests with DP mode..."
mkdir -p ${MYDIR}/results/wjn2/dp
cd ${MYDIR}
for s in 0.10 0.20 0.30 0.40 0.50; do
    for t in 1 2 3 4 5; do
        echo "Test DP with skewneess ${s}, repetition # ${t}"
        echo "./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode d -w 20000 -s 1000 --chaining"
        ./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode d -w 20000 -s 1000 --chaining &> output-s${s}_t${t}.log
        th=$(grep "throughput" output-s${s}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
        echo "Measured throughput: ${th}"
        # Save results
        mv output-s${s}_t${t}.log ${MYDIR}/results/wjn2/dp/output-s${s}_t${t}.log
        echo "$th" >> ${MYDIR}/results/wjn2/dp/bw-s${s}.log
        mv metric_latency.json ${MYDIR}/results/wjn2/dp/latency-s${s}_t${t}.log
    done
done

cd ${MYDIR}/Scripts
echo "...end"

# Tests HYBRID
echo "Running tests with HP mode..."
mkdir -p ${MYDIR}/results/wjn2/hp
cd ${MYDIR}
for s in 0.10 0.20 0.30 0.40 0.50; do
    for t in 1 2 3 4 5; do
        echo "Test HP with skewneess ${s}, repetition # ${t}"
        echo "./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode h -h 8 -w 20000 -s 1000 --chaining"
        ./windflow/bin/wjn --batch 32 --parallelism 1,1,8,8 --type ~/IntervalJoinBenchmarks/datasets/synt_ss/r_ss_k50_s${s}.txt --mode h -h 8 -w 20000 -s 1000 --chaining &> output-s${s}_t${t}.log
        th=$(grep "throughput" output-s${s}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
        echo "Measured throughput: ${th}"
        # Save results
        mv output-s${s}_t${t}.log ${MYDIR}/results/wjn2/hp/output-s${s}_t${t}.log
        echo "$th" >> ${MYDIR}/results/wjn2/hp/bw-s${s}.log
        mv metric_latency.json ${MYDIR}/results/wjn2/hp/latency-s${s}_t${t}.log
    done
done

cd ${MYDIR}/Scripts
echo "...end"
