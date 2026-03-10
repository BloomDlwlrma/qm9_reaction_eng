#!/bin/bash
#SBATCH --job-name=QM9_AMD_631gs_ccsdt
#SBATCH --partition=amd
#SBATCH --qos=normal
#SBATCH --time=7-00:00:00
#SBATCH --nodes=1                                 # Number of compute node
#SBATCH --ntasks=192                               # CPUs used for ORCA
#SBATCH --ntasks-per-node=192                      # CPUs used per node
#SBATCH --mem=576G
#SBATCH --output=./logs/amd_even_%a_%j.out
#SBATCH --error=./logs/amd_even_%a_%j.err
#SBATCH --array=0-7

# ==============================================================================
# Logic: AMD takes EVEN chunks (0, 2, 4...)
# Range 16000 - 128000
# ==============================================================================
BASE_OFFSET=1
CHUNKSIZE=2000
CONCURRENCY=12  # 64 cores / 16 per task = 8 tasks

# Calculate Start/End based on Even steps
# EFFECTIVE_CHUNK_ID=$(( SLURM_ARRAY_TASK_ID * 2 ))

START_MOL=$(( BASE_OFFSET + SLURM_ARRAY_TASK_ID * CHUNKSIZE )) # EFFECTIVE_CHUNK_ID
END_MOL=$(( START_MOL + CHUNKSIZE - 1 ))

# Persistent Directory Name
WORK_SUBDIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/run_chunk/run_chunk_${START_MOL}_${END_MOL}"
mkdir -p "$WORK_SUBDIR" "./logs"

# Environment
module purge
module load openmpi/gcc/4.1.6-gcc12.3
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"
export ORCA_SKIP_CPU_BIND=1 

echo "=== AMD (Even) Task ${SLURM_ARRAY_TASK_ID} ==="
echo "Chunk ID: ${SLURM_ARRAY_TASK_ID}" # SLURM_ARRAY_TASK_ID
echo "Range: ${START_MOL} to ${END_MOL}"

# Execute Python Manager: 631gss ccsdt
TIMEFORMAT="Simple chunk ${START_MOL}-${END_MOL} elapsed=%E user=%U sys=%S"
time python run_batch_srunmanager.py "${START_MOL}" "${END_MOL}" "${CONCURRENCY}" "631gss" "ccsdt" --work-subdir "${WORK_SUBDIR}"
COMPLETED_TASKS=$(grep -c "DONE" "${RUN_LOG}" 2>/dev/null || echo 0)
echo "=== Simple Job ${SLURM_JOB_ID} END (${COMPLETED_TASKS} tasks) ==="
