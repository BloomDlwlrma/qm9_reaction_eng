#!/bin/bash
#SBATCH --job-name=gen_inp
#SBATCH --partition=intel
#SBATCH --qos=normal
#SBATCH --time=1-00:00:00
#SBATCH --nodes=1                                 
#SBATCH --ntasks=32                               
#SBATCH --ntasks-per-node=32                      
#SBATCH --mem=128G
#SBATCH --output=./logs/gen_inp_%A_%a.out
#SBATCH --error=./logs/gen_inp_%A_%a.err
#SBATCH --array=0-8

set -euo pipefail
cd "$SLURM_SUBMIT_DIR"

# 确保 logs 目录存在
mkdir -p ./logs

START_MOL_BASE=1
END_MOL_BASE=133885
CHUNKSIZE=16000

ARRAY_ID="${SLURM_ARRAY_TASK_ID}"
START_MOL=$(( START_MOL_BASE + ARRAY_ID * CHUNKSIZE ))
END_MOL=$(( START_MOL + CHUNKSIZE - 1 ))

if [[ "$END_MOL" -gt "$END_MOL_BASE" ]]; then
    END_MOL="$END_MOL_BASE"
fi

if [[ "$START_MOL" -gt "$END_MOL_BASE" ]]; then
    echo "No molecules left for task ${ARRAY_ID}." 
    exit 0
fi

echo "=========================================================="
echo "Task Array ID    : ${ARRAY_ID}"
echo "Processing Range : ${START_MOL} to ${END_MOL}"
echo "=========================================================="

# 运行已有的 gen_orca_input.sh 并传入起始与结束 ID
bash ./gen_orca_input.sh "$START_MOL" "$END_MOL"

echo "=== Generation Done for Array ${ARRAY_ID} ==="
