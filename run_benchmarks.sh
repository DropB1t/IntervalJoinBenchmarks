#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
if [[ "${TRACE-0}" == "1" ]]; then
    set -o xtrace
fi

helpFunction()
{
    echo ""
    echo 'Usage: run_benchmarks.sh -r <number of runs> -d <results_dir>'
    exit 1
}

if [[ "${1-}" =~ ^-*h(elp)?$ ]]; then
    helpFunction
fi

cd "$(dirname "$0")"
SCRIPT_DIR=$(pwd)
GEN_DIR="$SCRIPT_DIR/gen_dataset"
WF_BENCH_DIR="$SCRIPT_DIR/windflow_benchmark"
FL_BENCH_DIR="$SCRIPT_DIR/flink_benchmark"

num_runs=5
res_dir="$SCRIPT_DIR/results"

options='r:d:'
while getopts $options option
do
    case "$option" in
        r  ) num_runs="$OPTARG";;
        d  ) res_dir="$SCRIPT_DIR/$OPTARG" ;;
        \? ) echo "Unknown option: -$OPTARG" >&2; exit 1;;
        :  ) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
        *  ) echo "Unimplemented option: -$OPTARG" >&2; exit 1;;
    esac
done

# Print helpFunction in case parameters are empty
if [ -z "$res_dir" ] || [ -z "$num_runs" ]
then
   echo "Some or all of the parameters are empty";
   helpFunction
fi

#mkdir -p "$res_dir/wf"
#mkdir -p "$res_dir/fl"

if [ ! -d "$SCRIPT_DIR/datasets" ]; then
    wget -q -O "$SCRIPT_DIR/datasets.tar.gz" "https://www.dropbox.com/scl/fi/y4qkcvci7yqcypg41tu85/datasets.tar.gz?rlkey=6o2d4byhx95d860pojddka4iq&dl=0"
    tar -zvxf datasets.tar.gz
    rm datasets.tar.gz
fi

SAMPLING=100

parallelism=( 1 2 4 8 16 32 )
num_key=( 100 1000 10000 )
batch_size=( 0 16 32 )

lower_bounds=( -500 -2500 )
upper_bounds=( 500 2500 )

exec_mode=( k d )
real_type=( rd sd )

synt_type=( su sz )
zipfian_skews=( 0.0 0.6 0.9 )

main() {
    echo "${num_runs} runs will be executed for each benchmark."
    echo "Running benchmarks..."
    wf_run_synthetic_benchmarks
    wf_run_real_benchmarks
    fl_run_synthetic_benchmarks
    fl_run_real_benchmarks
    echo "Done"
}

wf_run_synthetic_benchmarks() {
    cd $WF_BENCH_DIR || exit
    for bound_idx in "${!lower_bounds[@]}"; do
        for mode in "${exec_mode[@]}"; do 
            for batch in "${batch_size[@]}"; do
                local su=1
                local sz_1=1
                local sz_2=1
                for skewness in "${zipfian_skews[@]}"; do
                    if [ "$skewness" == "0.0" ]; then
                        local type="${synt_type[0]}"
                    else
                        local type="${synt_type[1]}"
                    fi
                    for key in "${num_key[@]}"; do
                        gen_dataset "$key" "$type" "$skewness"
                        for p_deg in "${parallelism[@]}"; do
                            if [ "$type" == "su" ]; then
                                local test_dir="$res_dir/wf/synthetic/${mode}_mode/${batch}_batch_${type}/test_$((su++))/"
                            elif [ "$skewness" == "0.6" ]; then
                                local test_dir="$res_dir/wf/synthetic/${mode}_mode/${batch}_batch_${type}_${skewness}/test_$((sz_1++))/"
                            else
                                local test_dir="$res_dir/wf/synthetic/${mode}_mode/${batch}_batch_${type}_${skewness}/test_$((sz_2++))/"
                            fi
                            mkdir -p "$test_dir"
                            rm -f "$test_dir"/*
                            for run in $(seq 1 "$num_runs"); do
                                ./bin/ij --rate 0 --sampling "$SAMPLING" --batch "$batch" --parallelism 1,1,"$p_deg",1 --type "$type" -m "$mode" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                                sed -i '22,28d' "$test_dir/run_${run}.log"
                            done
                        done
                    done
                done
            done
        done
    done
    cd - || exit
}

wf_run_real_benchmarks() {
    cd $WF_BENCH_DIR || exit
    for bound_idx in "${!lower_bounds[@]}"; do
        for mode in "${exec_mode[@]}"; do 
            for batch in "${batch_size[@]}"; do
                local rd=1
                local sd=1
                for type in "${real_type[@]}"; do
                    for p_deg in "${parallelism[@]}"; do
                        if [ "$type" == "rd" ]; then
                            local test_dir="$res_dir/wf/real/${mode}_mode/${batch}_batch_${type}/test_$((rd++))/"
                        else
                            local test_dir="$res_dir/wf/real/${mode}_mode/${batch}_batch_${type}/test_$((sd++))/"
                        fi
                        mkdir -p "$test_dir"
                        rm -f "$test_dir"/*
                        for run in $(seq 1 "$num_runs"); do
                            ./bin/ij --rate 0 --sampling "$SAMPLING" --batch "$batch" --parallelism 1,1,"$p_deg",1 --type "$type" -m "$mode" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                            sed -i '22,28d' "$test_dir/run_${run}.log"
                        done
                    done
                done
            done
        done
    done
    cd - || exit
}

fl_run_synthetic_benchmarks() {
    cd $FL_BENCH_DIR || exit
    local su=1
    local sz_1=1
    local sz_2=1
    for bound_idx in "${!lower_bounds[@]}"; do
        for skewness in "${zipfian_skews[@]}"; do
                if [ "$skewness" == "0.0" ]; then
                    local type="${synt_type[0]}"
                else
                    local type="${synt_type[1]}"
                fi
            for key in "${num_key[@]}"; do
                gen_dataset "$key" "$type" "$skewness"
                for p_deg in "${parallelism[@]}"; do
                    if [ "$type" == "su" ]; then
                        local test_dir="$res_dir/fl/synthetic/${type}/test_$((su++))/"
                    elif [ "$skewness" == "0.6" ]; then
                        local test_dir="$res_dir/fl/synthetic/${type}_${skewness}/test_$((sz_1++))/"
                    else
                        local test_dir="$res_dir/fl/synthetic/${type}_${skewness}/test_$((sz_2++))/"
                    fi
                    mkdir -p "$test_dir"
                    rm -f "$test_dir"/*
                    for run in $(seq 1 "$num_runs"); do
                        java -Xmx5g -jar target/IntervalJoinBench-1.0.jar --rate 0 --sampling "$SAMPLING" --parallelism 1,1,"$p_deg",1 --type "$type" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                    done
                    cp -f latency.json "$test_dir"
                    rm -f latency.json
                    cp -f throughput.json "$test_dir"
                    rm -f throughput.json
                done
            done
        done
    done
    cd - || exit
}

fl_run_real_benchmarks() {
    cd $FL_BENCH_DIR || exit
    local rd=1
    local sd=1
    for bound_idx in "${!lower_bounds[@]}"; do
        for type in "${real_type[@]}"; do
            for p_deg in "${parallelism[@]}"; do
                if [ "$type" == "rd" ]; then
                    local test_dir="$res_dir/fl/real/${type}/test_$((rd++))/"
                else
                    local test_dir="$res_dir/fl/real/${type}/test_$((sd++))/"
                fi
                mkdir -p "$test_dir"
                rm -f "$test_dir"/*
                for run in $(seq 1 "$num_runs"); do
                    java -Xmx5g -jar target/IntervalJoinBench-1.0.jar --rate 0 --sampling "$SAMPLING" --parallelism 1,1,"$p_deg",1 --type "$type" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                done
                cp -f latency.json "$test_dir"
                rm -f latency.json
                cp -f throughput.json "$test_dir"
                rm -f throughput.json
            done
        done
    done
    cd - || exit
}

gen_dataset() {
    local key="$1"
    local type="$2"
    local zipfian_skewness="$3"
    cd $GEN_DIR || exit
    if [ "$type" == "su" ]; then
        ./bin/gen --num_key "$key" --type "$type" 
    else
        ./bin/gen --num_key "$key" --type "$type" --zipf "$zipfian_skewness"
    fi
    cd - || exit
}

main "$@"