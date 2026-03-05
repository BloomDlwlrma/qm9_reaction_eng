#!/bin/bash
#SBATCH --job-name=QM9_Huge
#SBATCH --partition=hugemem
#SBATCH --qos=hugemem
#SBATCH --time=7-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=400G
#SBATCH --output=./logs/huge_%j.out
#SBATCH --error=./logs/huge_%j.err

# ============================================================================== 
# QM9 ORCA Batch (No explicit CPU binding)
# hugemem 128 2T 16; intel 32 192G 4 ; amd 64 256G 8/ 128 512g/ 192 768G 24;
# ============================================================================== 
START_MOL=131000
END_MOL=134000
MAX_MOL=133885
if [ $END_MOL -gt $MAX_MOL ]; then
    END_MOL=$MAX_MOL
fi
echo "Array task ${SLURM_ARRAY_TASK_ID}: molecules ${START_MOL} to ${END_MOL}"
CONCURRENCY=8 # Number of concurrent tasks to run (adjust based on workload and resources)

WORK_SUBDIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/run_chunk_HUGE_${START_MOL}_${END_MOL}"
mkdir -p "$WORK_SUBDIR" "./logs"

# ==============================================================================
# Automatically set CORES_PER_SOCKET based on partition
# ==============================================================================
module purge
module load openmpi/gcc/4.1.6-gcc12.3
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"

export ORCA_SKIP_CPU_BIND=1

# ==============================================================================
# Main slurm function: 631gss ccsdt
# ==============================================================================
TIMEFORMAT="Simple chunk ${START_MOL}-${END_MOL} elapsed=%E user=%U sys=%S"
time python run_batch_srunmanager.py "${START_MOL}" "${END_MOL}" "${CONCURRENCY}" "631gss" "ccsdt" --work-subdir "${WORK_SUBDIR}"
COMPLETED_TASKS=$(grep -c "DONE" "${RUN_LOG}" 2>/dev/null || echo 0)
echo "=== Simple Job ${SLURM_JOB_ID} END (${COMPLETED_TASKS} tasks) ==="