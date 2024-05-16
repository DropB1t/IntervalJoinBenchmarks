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
cd ..
MAIN_DIR=$(pwd)
GEN_DIR="$MAIN_DIR/gen_dataset"
WF_BENCH_DIR="$MAIN_DIR/windflow"
FL_BENCH_DIR="$MAIN_DIR/flink"

compile=0
num_runs=3
res_dir="$MAIN_DIR/results"

options='r:d:c'
while getopts $options option
do
    case "$option" in
        r  ) num_runs="$OPTARG";;
        d  ) res_dir="$MAIN_DIR/$OPTARG" ;;
        c  ) compile=1 ;;
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

if [ ! -d "$MAIN_DIR/datasets" ]; then
    wget -q -O "$MAIN_DIR/datasets.tar.gz" "https://www.dropbox.com/scl/fi/y4qkcvci7yqcypg41tu85/datasets.tar.gz?rlkey=6o2d4byhx95d860pojddka4iq&dl=0"
    tar -zvxf datasets.tar.gz
    rm datasets.tar.gz
fi

if command -v pip &> /dev/null; then
    cd $SCRIPT_DIR || exit
    pip install -r requirements.txt 1> /dev/null
    cd - || exit
else
    echo "Error: pip command not found. Please make sure pip is installed."
    exit 1
fi

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

main() {
    echo "${num_runs} runs will be executed for each benchmark."
    if [ "$compile" -eq 1 ]; then
        echo "Compiling..."
        compile_all
    fi
    fl_run_synthetic_benchmarks
    fl_run_real_benchmarks
    wf_run_synthetic_benchmarks
    wf_run_real_benchmarks
    echo "Done!"
}

wf_run_synthetic_benchmarks() {
    cd $WF_BENCH_DIR || exit
    for bound_idx in "${!lower_bounds[@]}"; do
        #if [ "${upper_bounds[$bound_idx]}" == 500 ]; then
        #    local boundir="1s"
        #else
        #    local boundir="5s"
        #fi
        for skewness in "${zipfian_skews[@]}"; do
            if [ "$skewness" == "0.0" ]; then
                local type="${synt_type[0]}"
            else
                local type="${synt_type[1]}"
            fi
            for mode in "${exec_mode[@]}"; do 
                if [ "$mode" == "k" ]; then
                    local mp="kp"
                else
                    local mp="dp"
                fi
                for key in "${num_key[@]}"; do
                    if [ "$key" = 10000 ]; then
                        local keydir="10k_keys"
                    elif [ "$key" = 1000 ]; then
                        local keydir="1k_keys"
                    else
                        local keydir="${key}_keys"
                    fi
                    gen_dataset "$key" "$type" "$skewness"
                    for batch in "${batch_size[@]}"; do
                        for s_deg in "${source_degrees[@]}"; do
                            local i=1
                            for p_deg in "${parallelism[@]}"; do
                                if [ "$type" == "su" ]; then
                                    local test_dir="$res_dir/wf/${type}/${mp}/${keydir}/${batch}_batch/source_${s_deg}/$((i++))_test_${p_deg}"
                                else
                                    local test_dir="$res_dir/wf/${type}_${skewness}/${mp}/${keydir}/${batch}_batch/source_${s_deg}/$((i++))_test_${p_deg}"
                                fi
                                batch_path="${test_dir%/*_batch*}"
                                python3 $SCRIPT_DIR/draw_charts.py "$batch_path" wf batch
                                exit 0
                                mkdir -p "$test_dir"
                                rm -f "$test_dir"/*
                                for run in $(seq 1 "$num_runs"); do
                                    ./bin/ij --rate 0 --sampling "$SAMPLING" --batch "$batch" --parallelism "$s_deg","$s_deg","$p_deg",1 --type "$type" -m "$mode" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                                    sed -i '22,28d' "$test_dir/run_${run}.log"
                                done
                            done
                            chart_path="${test_dir%/*}"
                            python3 $SCRIPT_DIR/draw_charts.py "$chart_path" wf all
                        done
                        # Average latency per source degree and parallelism
                        src_path="${chart_path%/*}"
                        python3 $SCRIPT_DIR/draw_charts.py "$src_path" wf avg
                    done
                    batch_path="${test_dir%/*_batch*}"
                    python3 $SCRIPT_DIR/draw_charts.py "$batch_path" wf batch
                done
            done
        done
    done
    cd - || exit
}

wf_run_real_benchmarks() {
    cd $WF_BENCH_DIR || exit
    for bound_idx in "${!lower_bounds[@]}"; do
        for type in "${real_type[@]}"; do
            for mode in "${exec_mode[@]}"; do
                if [ "$mode" == "k" ]; then
                    local mp="kp"
                else
                    local mp="dp"
                fi
                for batch in "${batch_size[@]}"; do
                    for s_deg in "${source_degrees[@]}"; do
                        local i=1
                        for p_deg in "${parallelism[@]}"; do
                            local test_dir="$res_dir/wf/${type}/${mp}/${batch}_batch/source_${s_deg}/$((i++))_test_${p_deg}"
                            mkdir -p "$test_dir"
                            rm -f "$test_dir"/*
                            for run in $(seq 1 "$num_runs"); do
                                ./bin/ij --rate 0 --sampling "$SAMPLING" --batch "$batch" --parallelism "$s_deg","$s_deg","$p_deg",1 --type "$type" -m "$mode" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                                sed -i '22,28d' "$test_dir/run_${run}.log"
                            done
                        done
                        chart_path="${test_dir%/*}"
                        python3 $SCRIPT_DIR/draw_charts.py "$chart_path" wf all
                    done
                    # Average latency per source degree and parallelism
                    src_path="${chart_path%/*}"
                    python3 $SCRIPT_DIR/draw_charts.py "$src_path" wf avg
                done
                batch_path="${test_dir%/*_batch*}"
                python3 $SCRIPT_DIR/draw_charts.py "$batch_path" wf batch
            done
        done
    done
    cd - || exit
}

fl_run_synthetic_benchmarks() {
    cd $FL_BENCH_DIR || exit
    rm -f latency.json
    rm -f throughput.json
    for bound_idx in "${!lower_bounds[@]}"; do
        for skewness in "${zipfian_skews[@]}"; do
                if [ "$skewness" == "0.0" ]; then
                    local type="${synt_type[0]}"
                else
                    local type="${synt_type[1]}"
                fi
            for key in "${num_key[@]}"; do
                if [ "$key" = 10000 ]; then
                    local keydir="10k_keys"
                elif [ "$key" = 1000 ]; then
                    local keydir="1k_keys"
                else
                    local keydir="${key}_keys"
                fi
                gen_dataset "$key" "$type" "$skewness"
                for s_deg in "${source_degrees[@]}"; do
                    local i=1
                    for p_deg in "${parallelism[@]}"; do
                        if [ "$type" == "su" ]; then
                            local test_dir="$res_dir/fl/${type}/${keydir}/source_${s_deg}/$((i++))_test_${p_deg}"
                        else
                            local test_dir="$res_dir/fl/${type}_${skewness}/${keydir}/source_${s_deg}/$((i++))_test_${p_deg}"
                        fi
                        mkdir -p "$test_dir"
                        rm -f "$test_dir"/*
                        for run in $(seq 1 "$num_runs"); do
                            java -jar target/IntervalJoinBench-1.0.jar --rate 0 --sampling "$SAMPLING" --parallelism "$s_deg","$s_deg","$p_deg",1 --type "$type" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                        done
                        cp -f latency.json "$test_dir"
                        rm -f latency.json
                        cp -f throughput.json "$test_dir"
                        rm -f throughput.json
                    done
                    chart_path="${test_dir%/*}"
                    python3 $SCRIPT_DIR/draw_charts.py "$chart_path" fl all
                done
                # Average latency per source degree and parallelism
                src_path="${chart_path%/*}"
                echo "$src_path"
                python3 $SCRIPT_DIR/draw_charts.py "$src_path" wf avg
            done
        done
    done
    cd - || exit
}

fl_run_real_benchmarks() {
    cd $FL_BENCH_DIR || exit
    rm -f latency.json
    rm -f throughput.json
    for bound_idx in "${!lower_bounds[@]}"; do
        for type in "${real_type[@]}"; do
            for s_deg in "${source_degrees[@]}"; do
                local i=1
                for p_deg in "${parallelism[@]}"; do
                    local test_dir="$res_dir/fl/${type}/source_${s_deg}/$((i++))_test_${p_deg}"
                    mkdir -p "$test_dir"
                    rm -f "$test_dir"/*
                    for run in $(seq 1 "$num_runs"); do
                        java -jar target/IntervalJoinBench-1.0.jar --rate 0 --sampling "$SAMPLING" --parallelism "$s_deg","$s_deg","$p_deg",1 --type "$type" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                    done
                    cp -f latency.json "$test_dir"
                    rm -f latency.json
                    cp -f throughput.json "$test_dir"
                    rm -f throughput.json
                done
                chart_path="${test_dir%/*}"
                python3 $SCRIPT_DIR/draw_charts.py "$chart_path" fl all
            done
            # Average latency per source degree and parallelism
            src_path="${chart_path%/*}"
            echo "$src_path"
            python3 $SCRIPT_DIR/draw_charts.py "$src_path" wf avg
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

compile_all() {
    cd $FL_BENCH_DIR || exit
    mvn clean package
    cd - || exit
    cd $WF_BENCH_DIR || exit
    make
    cd - || exit
    cd $GEN_DIR || exit
    make
    cd - || exit
}

main "$@"