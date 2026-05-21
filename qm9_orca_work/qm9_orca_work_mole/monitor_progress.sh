#!/usr/bin/env bash

# This script monitors the ORCA output directory and calculates the processing speed.
# Usage: ./monitor_progress.sh

WORK_ROOT="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
OUTPUT_ROOT="/lustre1/g/chem_yangjun/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
METHOD="ccsdt"
BASIS="631gss"
OUT_DIR="${OUTPUT_ROOT}/orca_output/orca_out_${METHOD}_${BASIS}"
FAIL_DIR="${WORK_ROOT}/orca_output/failed_logs"

echo "================================================="
echo "   ORCA High-Throughput Job Monitor              "
echo "================================================="
echo "Monitoring Output Dir: $OUT_DIR"

# 获取基准数量
prev_count=$(find "$OUT_DIR" -name "*.out" 2>/dev/null | wc -l)
prev_fail=$(find "$FAIL_DIR" -name "*_failed.out" 2>/dev/null | wc -l)

echo "Initial SUCCESS count : $prev_count"
echo "Initial FAILED count  : $prev_fail"
echo "Estimating speed... Please wait 30 seconds."
echo "================================================="

# 每30秒刷新一次
sleep 30

while true; do
    curr_count=$(find "$OUT_DIR" -name "*.out" 2>/dev/null | wc -l)
    curr_fail=$(find "$FAIL_DIR" -name "*_failed.out" 2>/dev/null | wc -l)
    
    diff_success=$((curr_count - prev_count))
    diff_fail=$((curr_fail - prev_fail))
    
    # 30秒内产生的数量乘以2即为每分钟的速度
    speed_success=$((diff_success * 2))
    
    timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo "[$timestamp] SUCCESS: $curr_count | FAILED: $curr_fail | SPEED: ~$speed_success molecules/min"
    
    # 检查是否有正在运行的节点 (SLURM)
    running_nodes=$(squeue -u $USER -h -t R | wc -l)
    if [ "$running_nodes" -gt 0 ]; then
        avg_speed=$(( speed_success / running_nodes ))
        echo "   -> Active Nodes: $running_nodes | Avg Speed per Node: ~$avg_speed molecules/min"
    else
        echo "   -> No active jobs found."
    fi
    echo "-------------------------------------------------"

    prev_count=$curr_count
    prev_fail=$curr_fail
    sleep 30
done
