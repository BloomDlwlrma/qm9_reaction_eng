#!/bin/bash
#SBATCH --job-name=qm9_orca
#SBATCH --partition=amd # intel or amd
#SBATCH --time=1-00:00:00 # 3-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64 # AMD: 128, Intel: 32

#SBATCH --mem=80G     

#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.err
#SBATCH --array=0-15 # 0-259
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=1070461445@qq.com

# ==============================================================================
# QM9 ORCA Batch (Optimized for your cluster)
# 0-259 → 260 jobs * 500 = 130000 molecules (full QM9)
# Edit --partition(intel|amd), --cpus-per-task(intel: 32|amd: 128), --array=(0-259|0-99),
#      and concurrency(intel: 4|amd: 16), CHUNK_SIZE(intel: 500|amd: 2000) as needed.
# ##SBATCH --mem=10G # 180G
# ##SBATCH --mem-per-cpu=1200M
# ==============================================================================
BASE_ID=4161
CHUNK_SIZE=50 # 500
OFFSET=$((SLURM_ARRAY_TASK_ID * CHUNK_SIZE)) 
START_ID=$((BASE_ID + OFFSET))
END_ID=$((START_ID + CHUNK_SIZE - 1))

echo "=== Job ${SLURM_JOB_ID} Array ${SLURM_ARRAY_TASK_ID} ==="
echo "Processing memory #SBATCH --mem=96G"
echo "Node: $(hostname), Range: ${START_ID} ~ ${END_ID}, CPUs: ${SLURM_CPUS_PER_TASK}"
mail -s "QM9 ORCA Job ${SLURM_JOB_ID} Started on $(hostname)" 1070461445@qq.com <<< "Processing ${START_ID}-${END_ID}. Checkpoint enabled."

#################################################################################
# Setup Environment
#################################################################################
module purge
module load openmpi/gcc/4.1.6-gcc12.3

export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=${ORCA_HOME}/bin:${PATH}
export LD_LIBRARY_PATH=${ORCA_HOME}/lib:${LD_LIBRARY_PATH}

export WORK_DIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
cd ${WORK_DIR}

#################################################################################
# Main Loop
#################################################################################
python run_batch_manager.py ${START_ID} ${END_ID} 4

COMPLETED_TASKS=$(grep -c "DONE" batch_${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out 2>/dev/null || echo 0)
echo "=== Job ${SLURM_JOB_ID} Array ${SLURM_ARRAY_TASK_ID} END (${COMPLETED_TASKS} tasks) ==="
mail -s "QM9 ORCA Job ${SLURM_JOB_ID} Completed on $(hostname)" 1070461445@qq.com <<< "Processing ${START_ID}-${END_ID}, ${COMPLETED_TASKS} completed."
