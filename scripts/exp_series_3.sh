#!/bin/bash
# Launcher of the third series of experiments on Noam

# Working directory
MYDIR=${HOME}/IntervalJoinBenchmarks

echo "Creating the output directory for storing the results --> OK!"
mkdir -p ${MYDIR}/results/series_3

# Tests KP
echo "Running tests with KP mode..."
mkdir -p ${MYDIR}/results/series_3/kp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for p in 1 2 4 6 8 10 12 14; do
        for t in 1 2 3 4 5; do
            echo "Test KP with interval ${i}, parallelism ${p}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,${p},${p} -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_u/r_uniform_k50.txt --mode k -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,${p},${p} -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_u/r_uniform_k50.txt --mode k -l -${i} -u ${i} &> output-i${i}-p${p}_t${t}.log
            th=$(grep "throughput" output-i${i}-p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-p${p}_t${t}.log ${MYDIR}/results/series_3/kp/output-i${i}-p${p}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_3/kp/bw-i${i}_p${p}.log
            mv latency.json ${MYDIR}/results/series_3/kp/latency-i${i}_p${p}_t${t}.log
        done
    done
done
cd ${MYDIR}/Scripts
echo "...end"

# Tests DP
echo "Running tests with DP mode..."
mkdir -p ${MYDIR}/results/series_3/dp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for p in 1 2 4 6 8 10 12 14; do
        for t in 1 2 3 4 5; do
            echo "Test DP with interval ${i}, parallelism ${p}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,${p},${p} -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_u/r_uniform_k50.txt --mode d -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,${p},${p} -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_u/r_uniform_k50.txt --mode d -l -${i} -u ${i} &> output-i${i}-p${p}_t${t}.log
            th=$(grep "throughput" output-i${i}-p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-p${p}_t${t}.log ${MYDIR}/results/series_3/dp/output-i${i}-p${p}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_3/dp/bw-i${i}_p${p}.log
            mv latency.json ${MYDIR}/results/series_3/dp/latency-i${i}_p${p}_t${t}.log
        done
    done
done
cd ${MYDIR}/Scripts
echo "...end"

# Tests HYBRID
echo "Running tests with HYBRID mode..."
mkdir -p ${MYDIR}/results/series_3/hp
cd ${MYDIR}
for i in 5000 10000 20000 30000; do
    for p in 1 2 4 6 8 10 12 14; do
        for t in 1 2 3 4 5; do
            echo "Test HYBRID with interval ${i}, parallelism ${p}, repetition # ${t}"
            echo "./windflow/bin/ij2 --batch 32 --parallelism 1,1,${p},${p} -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_u/r_uniform_k50.txt --mode h -h ${p} -l -${i} -u ${i}"
            ./windflow/bin/ij2 --batch 32 --parallelism 1,1,${p},${p} -chaining --type ~/IntervalJoinBenchmarks/datasets/synt_u/r_uniform_k50.txt --mode h -h ${p} -l -${i} -u ${i} &> output-i${i}-p${p}_t${t}.log
            th=$(grep "throughput" output-i${i}-p${p}_t${t}.log | cut -f3 -d " " | awk '{ sum += $1 } END { print sum }')
            echo "Measured throughput: ${th}"
            # Save results
            mv output-i${i}-p${p}_t${t}.log ${MYDIR}/results/series_3/hp/output-i${i}-p${p}_t${t}.log
            echo "$th" >> ${MYDIR}/results/series_3/hp/bw-i${i}_p${p}.log
            mv latency.json ${MYDIR}/results/series_3/hp/latency-i${i}_p${p}_t${t}.log
        done
    done
done

cd ${MYDIR}/scripts
echo "...end"
