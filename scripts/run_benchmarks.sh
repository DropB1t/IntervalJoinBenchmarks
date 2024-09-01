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
        d  ) res_dir="$OPTARG" ;;
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

( "$SCRIPT_DIR/download_datasets.sh" )

if command -v pip &> /dev/null; then
    cd "$SCRIPT_DIR" || exit
    pip install -r requirements.txt 1> /dev/null
    cd - || exit
else
    echo "Error: pip command not found. Please make sure pip is installed."
    exit 1
fi

# shellcheck disable=SC1091
. "$SCRIPT_DIR"/config.sh

main() {
    echo "${num_runs} runs will be executed for each benchmark."
    if [ "$compile" -eq 1 ]; then
        echo "Compiling..."
        compile_all
    fi
    wf_run_synthetic_benchmarks
    wf_run_real_benchmarks
    fl_run_synthetic_benchmarks
    fl_run_real_benchmarks
    synt_mode_comparison_charts
    real_mode_comparison_charts
    echo "Done!"
}

# shellcheck disable=SC2154
wf_run_synthetic_benchmarks() {
    cd "$WF_BENCH_DIR" || exit
    for bound_idx in "${!lower_bounds[@]}"; do
        #if [ "${upper_bounds[$bound_idx]}" == 500 ]; then
        #    local boundir="1s"
        #else
        #    local boundir="5s"
        #fi
        for skewness in "${zipfian_skews[@]}"; do
            local type
            type=$(get_synthetic_type "$skewness")
            local typedir
            typedir=$(get_synthetic_typedir "$skewness")
            for mode in "${exec_mode[@]}"; do 
                if [ "$mode" == "k" ]; then
                    local mp="kp"
                else
                    local mp="dp"
                fi
                for key in "${num_key[@]}"; do
                    local keydir
                    keydir=$(get_keydir "$key")
                    gen_dataset "$key" "$type" "$skewness"
                    for batch in "${batch_size[@]}"; do
                        for s_deg in "${source_degrees[@]}"; do
                            local i=1
                            for p_deg in "${parallelism[@]}"; do
                                # Add ../wf/${boundir}/${typedir}/.. to the path if you want to discriminate between interval boundaries
                                local test_dir="$res_dir/wf/${typedir}/${mp}/${keydir}/${batch}_batch/${s_deg}_source/$((i++))_test_${p_deg}"
                                mkdir -p "$test_dir"
                                rm -f "$test_dir"/*
                                for run in $(seq 1 "$num_runs"); do
                                    ./bin/ij --rate 0 --sampling "$SAMPLING" --batch "$batch" --parallelism "$s_deg","$s_deg","$p_deg",1 --type "$type" -m "$mode" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                                    sed -i '22,28d' "$test_dir/run_${run}.log"
                                done
                            done
                            chart_path="${test_dir%/*}"
                            python3 "$SCRIPT_DIR"/draw_charts.py all "$chart_path"
                        done
                        # Average latency per source degree and parallelism
                        src_path="${chart_path%/*}"
                        python3 "$SCRIPT_DIR"/draw_charts.py src "$src_path"
                    done
                    batch_path="${test_dir%/*_batch*}"
                    python3 "$SCRIPT_DIR"/draw_charts.py batch "$batch_path"
                done
            done
        done
    done
    cd - || exit
}

# shellcheck disable=SC2154
wf_run_real_benchmarks() {
    cd "$WF_BENCH_DIR" || exit
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
                            local test_dir="$res_dir/wf/${type}/${mp}/${batch}_batch/${s_deg}_source/$((i++))_test_${p_deg}"
                            mkdir -p "$test_dir"
                            rm -f "$test_dir"/*
                            for run in $(seq 1 "$num_runs"); do
                                ./bin/ij --rate 0 --sampling "$SAMPLING" --batch "$batch" --parallelism "$s_deg","$s_deg","$p_deg",1 --type "$type" -m "$mode" -l "${lower_bounds[$bound_idx]}" -u "${upper_bounds[$bound_idx]}" --chaining -o "$test_dir" | tee "$test_dir/run_${run}.log"
                                sed -i '22,28d' "$test_dir/run_${run}.log"
                            done
                        done
                        chart_path="${test_dir%/*}"
                        python3 "$SCRIPT_DIR"/draw_charts.py all "$chart_path"
                    done
                    # Average latency per source degree and parallelism
                    src_path="${chart_path%/*}"
                    python3 "$SCRIPT_DIR"/draw_charts.py src "$src_path"
                done
                batch_path="${test_dir%/*_batch*}"
                python3 "$SCRIPT_DIR"/draw_charts.py batch "$batch_path"
            done
        done
    done
    cd - || exit
}

fl_run_synthetic_benchmarks() {
    cd "$FL_BENCH_DIR" || exit
    rm -f latency.json
    rm -f throughput.json
    for bound_idx in "${!lower_bounds[@]}"; do
        for skewness in "${zipfian_skews[@]}"; do
            local type
            type=$(get_synthetic_type "$skewness")
            local typedir
            typedir=$(get_synthetic_typedir "$skewness")
            for key in "${num_key[@]}"; do
                local keydir
                keydir=$(get_keydir "$key")
                gen_dataset "$key" "$type" "$skewness"
                for s_deg in "${source_degrees[@]}"; do
                    local i=1
                    for p_deg in "${parallelism[@]}"; do
                        local test_dir="$res_dir/fl/${typedir}/${keydir}/${s_deg}_source/$((i++))_test_${p_deg}"
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
                    python3 "$SCRIPT_DIR"/draw_charts.py all "$chart_path"
                done
                # Average latency per source degree and parallelism
                src_path="${chart_path%/*}"
                echo "$src_path"
                python3 "$SCRIPT_DIR"/draw_charts.py src "$src_path"
            done
        done
    done
    cd - || exit
}

fl_run_real_benchmarks() {
    cd "$FL_BENCH_DIR" || exit
    rm -f latency.json
    rm -f throughput.json
    for bound_idx in "${!lower_bounds[@]}"; do
        for type in "${real_type[@]}"; do
            for s_deg in "${source_degrees[@]}"; do
                local i=1
                for p_deg in "${parallelism[@]}"; do
                    local test_dir="$res_dir/fl/${type}/${s_deg}_source/$((i++))_test_${p_deg}"
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
                python3 "$SCRIPT_DIR"/draw_charts.py all "$chart_path"
            done
            # Average latency per source degree and parallelism
            src_path="${chart_path%/*}"
            echo "$src_path"
            python3 "$SCRIPT_DIR"/draw_charts.py src "$src_path"
        done
    done
    cd - || exit
}

synt_mode_comparison_charts() {
    for skewness in "${zipfian_skews[@]}"; do
        local typedir
        typedir=$(get_synthetic_typedir "$skewness")
        local save_dir="$res_dir/${typedir}_comparison"
        mkdir -p "$save_dir"
        #rm -f "$save_dir"/*
        for key in "${num_key[@]}"; do
            local keydir
            keydir=$(get_keydir "$key")
            for batch in "${batch_size[@]}"; do
                for s_deg in "${source_degrees[@]}"; do
                    local kp_dir="$res_dir/wf/${typedir}/kp/${keydir}/${batch}_batch/${s_deg}_source"
                    local dp_dir="$res_dir/wf/${typedir}/dp/${keydir}/${batch}_batch/${s_deg}_source"
                    local fl_dir="$res_dir/fl/${typedir}/${keydir}/${s_deg}_source"
                    img_name="${typedir}_${key}_keys_${s_deg}_srate_${batch}_wf_batch"
                    if [ -d "$kp_dir" ] && [ -d "$dp_dir" ] && [ -d "$fl_dir" ]; then
                        python3 "$SCRIPT_DIR"/draw_charts.py comparison "${save_dir}" "${kp_dir}" "${dp_dir}" "${fl_dir}" "${img_name}"
                    fi
                done
            done
        done
    done
}

real_mode_comparison_charts() {
    for type in "${real_type[@]}"; do
        local save_dir="$res_dir/${type}_comparison"
        mkdir -p "$save_dir"
        #rm -f "$save_dir"/*
        for batch in "${batch_size[@]}"; do
            for s_deg in "${source_degrees[@]}"; do
                local kp_dir="$res_dir/wf/${type}/kp/${batch}_batch/${s_deg}_source"
                local dp_dir="$res_dir/wf/${type}/dp/${batch}_batch/${s_deg}_source"
                local fl_dir="$res_dir/fl/${type}/${s_deg}_source"
                img_name="${type}_${s_deg}_srate_${batch}_wf_batch"
                if [ -d "$kp_dir" ] && [ -d "$dp_dir" ] && [ -d "$fl_dir" ]; then
                    python3 "$SCRIPT_DIR"/draw_charts.py comparison "${save_dir}" "${kp_dir}" "${dp_dir}" "${fl_dir}" "${img_name}"
                fi
            done
        done
    done
}

get_keydir() {
    if [ "$1" = 10000 ]; then
        echo "10k_keys"
    elif [ "$1" = 1000 ]; then
        echo "1k_keys"
    else
        echo "${1}_keys"
    fi
}

get_synthetic_typedir() {
    local skew="$1"
    if [ "$skew" == "0.0" ]; then
        echo "${synt_type[0]:?}"
    else
        echo "${synt_type[1]}_${skew}"
    fi
}

get_synthetic_type() {
    local skewness="$1"
    if [ "$skewness" == "0.0" ]; then
        echo "${synt_type[0]}"
    else
        echo "${synt_type[1]}"
    fi
}

gen_dataset() {
    local key="$1"
    local type="$2"
    local zipfian_skewness="$3"
    cd "$GEN_DIR" || exit
    if [ "$type" == "su" ]; then
        ./bin/gen --num_key "$key" --type "$type" 
    else
        ./bin/gen --num_key "$key" --type "$type" --zipf "$zipfian_skewness"
    fi
    cd - || exit
}

compile_all() {
    cd "$FL_BENCH_DIR" || exit
    mvn clean package
    cd - || exit
    cd "$WF_BENCH_DIR" || exit
    make
    cd - || exit
    cd "$GEN_DIR" || exit
    make
    cd - || exit
}

main "$@"