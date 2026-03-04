#!/bin/bash
#SBATCH --job-name=srun_orca
#SBATCH --partition=amd
#SBATCH --time=7-00:00:00
#SBATCH --nodes=2
#SBATCH --ntasks=2
##SBATCH --cpus-per-task=192
##SBATCH -C CPU_MNF:AMD,CPU_SKU:9654
#SBATCH --cpus-per-task=128
#SBATCH -C CPU_MNF:AMD,CPU_SKU:7742
#SBATCH --qos=normal
##SBATCH --mem=720G  # Use all memory on node if exclusive, or set explicitly
#SBATCH --mem=480G  # Use all memory on node if exclusive, or set explicitly
##SBATCH --array=0-1
##SBATCH --array=0-8 # 26*5000 = 130000
#SBATCH --array=0-7
#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/simple_batch_%j.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/simple_batch_%j.err

# ============================================================================== 
# QM9 ORCA Batch (Multi-node with explicit CPU binding on high-core AMD nodes)
# ==============================================================================
export OMPI_MCA_rmaps_base_oversubscribe=true
#BASE_ID=1
BASE_ID=90001
CHUNK_SIZE=5000
OFFSET=$((SLURM_ARRAY_TASK_ID * CHUNK_SIZE))
START_ID=$((BASE_ID + OFFSET))
END_ID=$((START_ID + CHUNK_SIZE - 1))
#if [[ $END_ID -gt 133885 ]]; then END_ID=133885; fi
if [[ $END_ID -gt 130000 ]]; then END_ID=130000; fi
SUB_CHUNK_SIZE=$((CHUNK_SIZE / SLURM_NTASKS)) 
CONCURRENCY=$((SLURM_CPUS_PER_TASK / 8)) # Number of concurrent tasks to run 192/8


# Task Configuration
BASIS="631gs"
#METHODS="ccsdt"
METHODS="ccsd"

WORK_DIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
LOG_DIR="${WORK_DIR}/orca_rundir_info"

mkdir -p "${LOG_DIR}"
cd "${WORK_DIR}"

# ==============================================================================
# Set CORES_PER_SOCKET for EPYC9654 (96 cores per socket, dual-socket node)
# ==============================================================================
export CORES_PER_SOCKET=$(($SLURM_CPUS_PER_TASK / 2))

echo "Using CORES_PER_SOCKET = $CORES_PER_SOCKET (Partition: $SLURM_JOB_PARTITION, Constraint: $SLURM_JOB_CONSTRAINTS)"
# Allow oversubscribe (safety)
export OMPI_MCA_rmaps_base_oversubscribe=true

module purge
module load openmpi/gcc/4.1.6-gcc12.3
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"

# ==============================================================================
# Set ORCA_SKIP_CPU_BIND for core binding control
# ==============================================================================
export ORCA_SKIP_CPU_BIND=0
#export ORCA_SKIP_CPU_BIND=1

# ==============================================================================
# Main slurm function
# for debug, run run_batch_debugmanager.py (default: run_batch_srunmanager.py)
# ==============================================================================
echo "=== Job ${SLURM_JOB_ID} (Array Task ${SLURM_ARRAY_TASK_ID}) ==="
echo "Node List: $SLURM_NODELIST, Total Nodes: ${SLURM_NNODES}, Range: ${START_ID}-${END_ID}, CPUs per task: ${SLURM_CPUS_PER_TASK}"
echo "Config: Basis=${BASIS}, Methods=${METHODS}"

pids=()
for tid in $(seq 0 $((SLURM_NTASKS - 1))); do
    sub_start=$((START_ID + tid * SUB_CHUNK_SIZE))
    sub_end=$((sub_start + SUB_CHUNK_SIZE - 1))
    if [[ $tid -eq $((SLURM_NTASKS - 1)) ]]; then
        sub_end=$END_ID
    fi
    if [[ $sub_start -gt $END_ID ]]; then
        break
    fi
    (
        echo "[Subchunk $tid] Starting: $sub_start - $sub_end  at $(date '+%Y-%m-%d %H:%M:%S')"
        TIMEFORMAT="[Subchunk $tid] Elapsed: %E user=%U sys=%S"
        time srun --nodes=1 --ntasks=1 --cpus-per-task=${SLURM_CPUS_PER_TASK} --mem=${SLURM_MEM_PER_NODE} --exact python run_batch_srunmanager.py "${sub_start}" "${sub_end}" "${CONCURRENCY}" "${BASIS}" "${METHODS}"
        echo "[Subchunk $tid] Completed: $sub_start - $sub_end  at $(date '+%Y-%m-%d %H:%M:%S')"
    ) &
    pids+=($!)
done
# Wait for all sub-jobs to complete
wait "${pids[@]}"

echo "=== Job ${SLURM_JOB_ID} (Array Task ${SLURM_ARRAY_TASK_ID}) END at $(date)==="