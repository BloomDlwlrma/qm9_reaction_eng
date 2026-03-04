#!/bin/bash
#SBATCH --job-name=QM9_AMD_Even
#SBATCH --partition=amd
#SBATCH --qos=normal
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=480G
#SBATCH --output=./logs/amd_even_%a_%j.out
#SBATCH --error=./logs/amd_even_%a_%j.err
#SBATCH --array=0-13

# ==============================================================================
# Logic: AMD takes EVEN chunks (0, 2, 4...)
# Range 16000 - 128000
# ==============================================================================
BASE_OFFSET=16000
CHUNKSIZE=4000
CONCURRENCY=8  # 128 cores / 16 per task = 8 tasks

# Calculate Start/End based on Even steps
# Task 0 -> Chunk 0 (16k)
# Task 1 -> Chunk 2 (24k)
# Task 2 -> Chunk 4 (32k)
EFFECTIVE_CHUNK_ID=$(( SLURM_ARRAY_TASK_ID * 2 ))

START_MOL=$(( BASE_OFFSET + EFFECTIVE_CHUNK_ID * CHUNKSIZE ))
END_MOL=$(( START_MOL + CHUNKSIZE - 1 ))

# Persistent Directory Name
WORK_SUBDIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/run_chunk_${START_MOL}_${END_MOL}"
mkdir -p "$WORK_SUBDIR" "./logs"

# Environment
module purge
module load openmpi/gcc/4.1.6-gcc12.3
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"
export ORCA_SKIP_CPU_BIND=1 

echo "=== AMD (Even) Task ${SLURM_ARRAY_TASK_ID} ==="
echo "Chunk ID: ${EFFECTIVE_CHUNK_ID}"
echo "Range: ${START_MOL} to ${END_MOL}"

# Execute Python Manager: 631gss ccsdt
TIMEFORMAT="Simple chunk ${START_MOL}-${END_MOL} elapsed=%E user=%U sys=%S"
time python run_batch_srunmanager.py "${START_MOL}" "${END_MOL}" "${CONCURRENCY}" "631gss" "ccsdt" --work-subdir "${WORK_SUBDIR}"
COMPLETED_TASKS=$(grep -c "DONE" "${RUN_LOG}" 2>/dev/null || echo 0)
echo "=== Simple Job ${SLURM_JOB_ID} END (${COMPLETED_TASKS} tasks) ==="