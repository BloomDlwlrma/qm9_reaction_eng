#!/bin/bash
#SBATCH --job-name=qm9_orca
#SBATCH --partition=condo_amd # intel or amd, hugemem, l40s
#SBATCH --time=15:00:00 # Adjusted for 20 mols (5 waves * 3h)
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32 # Enough for 4 concurrent * 8 cores
#SBATCH --qos=long
#SBATCH --mem=0    # 32 cores * 4G = 128G + overhead

#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.err
#SBATCH --array=0-49 # 50 jobs * 20 = 1000 molecules
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
BASE_ID=10001
CHUNK_SIZE=20 # Process 20 molecules per job to reduce job count
OFFSET=$((SLURM_ARRAY_TASK_ID * CHUNK_SIZE)) 
START_ID=$((BASE_ID + OFFSET))
END_ID=$((START_ID + CHUNK_SIZE - 1))

echo "=== Job ${SLURM_JOB_ID} Array ${SLURM_ARRAY_TASK_ID} ==="
echo "Processing memory #SBATCH --mem=96G"
echo "Node: $(hostname), Range: ${START_ID} ~ ${END_ID}, CPUs: ${SLURM_CPUS_PER_TASK}"

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
