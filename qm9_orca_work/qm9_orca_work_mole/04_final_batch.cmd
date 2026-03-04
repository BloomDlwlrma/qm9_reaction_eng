#!/bin/bash
#SBATCH --job-name=66gsst # Job name with end_id
#SBATCH --partition=condo_amd # Partition name (adjust as needed)
#SBATCH --time=7-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=128
#SBATCH --qos=normal
#SBATCH --mem=480G  # Use all memory on node if exclusive, or set explicitly
#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/%j_batch_ccsdt_631gss.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/%j_batch_ccsdt_631gss.err
#SBATCH --array=0-7
# ============================================================================== 
# QM9 ORCA Batch (No explicit CPU binding)
# hugemem 128 2T 16; intel 32 192G 4 ; amd 64 256G 8/ 128 512g/ 192 768G 24;
# ============================================================================== 
START_ID=2001
CHUNKSIZE=4000
START_MOL=$(( START_ID + SLURM_ARRAY_TASK_ID * CHUNKSIZE ))
END_MOL=$(( START_MOL + CHUNKSIZE - 1 ))
MAX_MOL=128000
if [ $END_MOL -gt $MAX_MOL ]; then
    END_MOL=$MAX_MOL
fi
echo "Array task ${SLURM_ARRAY_TASK_ID}: molecules ${START_MOL} to ${END_MOL}"

# Task Configuration
export OMPI_MCA_rmaps_base_oversubscribe=true
BASIS="631gss"
METHODS="ccsdt"
CONCURRENCY=$((SLURM_CPUS_PER_TASK / 16)) # Number of concurrent tasks to run (adjust based on workload and resources)

WORK_DIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
LOG_DIR="${WORK_DIR}/orca_rundir_info"
mkdir -p "${LOG_DIR}"
cd "${WORK_DIR}"

JOB_WORK_SUBDIR="${WORK_DIR}/run_${SLURM_JOB_ID}"
mkdir -p "${JOB_WORK_SUBDIR}"
echo "Using unique working subdirectory: ${JOB_WORK_SUBDIR}"

# ==============================================================================
# Automatically set CORES_PER_SOCKET based on partition
# ==============================================================================
if [[ "$SLURM_JOB_PARTITION" == "intel" ]]; then
    export CORES_PER_SOCKET=16
elif [[ "$SLURM_JOB_PARTITION" == "amd" ]]; then
    export CORES_PER_SOCKET=64 #64 for 7742, 96 for 9654
elif [[ "$SLURM_JOB_PARTITION" == "condo_amd" ]]; then
    export CORES_PER_SOCKET=64 #64 for 7742, 96 for 9654
elif [[ "$SLURM_JOB_PARTITION" == "hugemem" ]]; then
    export CORES_PER_SOCKET=64
else
    # Default value (prevent errors for unknown partitions)
    export CORES_PER_SOCKET=16
    echo "Warning: Unknown partition $SLURM_JOB_PARTITION, using default CORES_PER_SOCKET=16"
fi

echo "Using CORES_PER_SOCKET = $CORES_PER_SOCKET (Partition: $SLURM_JOB_PARTITION)"

# Allow oversubscribe (safety)
export OMPI_MCA_rmaps_base_oversubscribe=true

module purge
module load openmpi/gcc/4.1.6-gcc12.3

export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"

#export ORCA_SKIP_CPU_BIND=0
export ORCA_SKIP_CPU_BIND=1

# ==============================================================================
# Main slurm function
# for debug, run run_batch_debugmanager.py (default: run_batch_srunmanager.py)
# ==============================================================================
echo "=== Simple Job ${SLURM_JOB_ID} ==="
echo "Node: $(hostname), Range: ${START_MOL}-${END_MOL}, CPUs: ${SLURM_CPUS_PER_TASK}"
echo "Config: Basis=${BASIS}, Methods=${METHODS}"

TIMEFORMAT="Simple chunk ${START_MOL}-${END_MOL} elapsed=%E user=%U sys=%S"
# time python run_batch_manager.py "${START_ID}" "${END_ID}" "${CONCURRENCY}" "${BASIS}" "${METHODS}"
time python run_batch_manager.py "${START_MOL}" "${END_MOL}" "${CONCURRENCY}" "${BASIS}" "${METHODS}" --work-subdir "${JOB_WORK_SUBDIR}"

COMPLETED_TASKS=$(grep -c "DONE" "${RUN_LOG}" 2>/dev/null || echo 0)
echo "=== Simple Job ${SLURM_JOB_ID} END (${COMPLETED_TASKS} tasks) ==="