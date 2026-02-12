#!/bin/bash
#SBATCH --job-name=qm9_test_b0
#SBATCH --partition=amd           # amd has large memory, starts faster
#SBATCH --qos=debug               # High priority (50000), starts very fast
#SBATCH --time=00:30:00           # Test short time limit
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64        # amd nodes commonly have 64/128 cores
#SBATCH --mem=80G                 # amd abundant
#SBATCH --array=0-2
#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.err
##SBATCH --mail-type=END,FAIL
##SBATCH --mail-user=1070461445@qq.com

BASE_ID=1
CHUNK_SIZE=50
OFFSET=$((SLURM_ARRAY_TASK_ID * CHUNK_SIZE))
START_ID=$((BASE_ID + OFFSET))
END_ID=$((START_ID + CHUNK_SIZE - 1))

echo "Batch 0 Array $SLURM_ARRAY_TASK_ID: $START_ID ~ $END_ID on $(hostname)"

module purge
module load openmpi/gcc/4.1.6-gcc12.3

export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=${ORCA_HOME}/bin:$PATH
export LD_LIBRARY_PATH=${ORCA_HOME}/lib:$LD_LIBRARY_PATH

cd /scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole

python run_batch_manager.py ${START_ID} ${END_ID} 4

echo "Batch 0 Array $SLURM_ARRAY_TASK_ID completed."
