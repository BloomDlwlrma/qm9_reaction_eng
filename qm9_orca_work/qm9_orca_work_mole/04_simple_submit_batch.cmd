#!/bin/bash
#SBATCH --job-name=qm9_orca_simple
#SBATCH --partition=amd
#SBATCH --time=15:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
##SBATCH --qos=hugemem
#SBATCH --mem=0  # Use all memory on node if exclusive, or set explicitly


#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/simple_batch_%j.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/simple_batch_%j.err
##SBATCH --mail-type=END,FAIL
##SBATCH --mail-user=1070461445@qq.com

# ============================================================================== 
# QM9 ORCA Batch (No explicit CPU binding)
# ============================================================================== 
START_ID=1
END_ID=999
CONCURRENCY=4

WORK_DIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
LOG_DIR="${WORK_DIR}/orca_rundir_info"
RUN_LOG="${LOG_DIR}/simple_batch_${SLURM_JOB_ID}.log"
TIME_LOG="${LOG_DIR}/simple_batch_${SLURM_JOB_ID}.time"

mkdir -p "${LOG_DIR}"
cd "${WORK_DIR}"

module purge
module load openmpi/gcc/4.1.6-gcc12.3

export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"
export ORCA_SKIP_CPU_BIND=1

echo "=== Simple Job ${SLURM_JOB_ID} ==="
echo "Node: $(hostname), Range: ${START_ID}-${END_ID}, CPUs: ${SLURM_CPUS_PER_TASK}"
#mail -s "QM9 ORCA Simple Job ${SLURM_JOB_ID} Started on $(hostname)" 1070461445@qq.com <<< "Processing ${START_ID}-${END_ID} without explicit CPU binding."

TIMEFORMAT="Simple chunk ${START_ID}-${END_ID} elapsed=%E user=%U sys=%S"
{ time python run_batch_manager.py "${START_ID}" "${END_ID}" "${CONCURRENCY}"; } \
	> >(tee "${RUN_LOG}") 2> >(tee -a "${RUN_LOG}" | tee -a "${TIME_LOG}" >&2)

COMPLETED_TASKS=$(grep -c "DONE" "${RUN_LOG}" 2>/dev/null || echo 0)
echo "=== Simple Job ${SLURM_JOB_ID} END (${COMPLETED_TASKS} tasks) ==="
#mail -s "QM9 ORCA Simple Job ${SLURM_JOB_ID} Completed on $(hostname)" 1070461445@qq.com <<< "Processing ${START_ID}-${END_ID}, ${COMPLETED_TASKS} tasks completed without binding."
