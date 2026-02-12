#!/bin/bash
#SBATCH --job-name=qm9_batch
#SBATCH --partition=intel
#SBATCH --time=2-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --output=batch_%A_%a.out
#SBATCH --error=batch_%A_%a.err
#SBATCH --array=0-0  # Modify this range to scale to multiple nodes!

# ==============================================================================
# QM9 High-Throughput Batch Submission Script
# ==============================================================================
# This script launches the Python manager to process a chunk of molecules.
# To run on multiple nodes, increase the --array range above.
# Example: --array=0-9 will launch 10 jobs (possibly on 10 nodes if available),
# processing 10 * CHUNK_SIZE molecules in total.
# ==============================================================================

# 1. Define Range
BASE_ID=38135           # Starting Molecule ID
CHUNK_SIZE=100          # How many molecules per node/job
OFFSET=$(( SLURM_ARRAY_TASK_ID * CHUNK_SIZE ))

START_ID=$(( BASE_ID + OFFSET ))
END_ID=$(( START_ID + CHUNK_SIZE - 1 ))

echo "Job ID: ${SLURM_JOB_ID} Array ID: ${SLURM_ARRAY_TASK_ID}"
echo "Node: $(hostname)"
echo "Processing QM9 Range: ${START_ID} to ${END_ID}"

# 2. Setup Environment
module load python/3.11.4 2>/dev/null || echo "Using system python"
# Select ORCA Version 
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=${ORCA_HOME}/bin:$PATH
export LD_LIBRARY_PATH=${ORCA_HOME}/lib:$LD_LIBRARY_PATH
echo ===========  Work Directory ${WORK_DIR} - JOB ${SLURM_JOBID} : ${NPROCS} CPUS  ==============

export WORK_DIR="/scr/u/u3651388/orcarun/qm9_orca_work_mole"
cd ${WORK_DIR}

# 3. Launch Manager
# Arguments: <START_ID> <END_ID> [CONCURRENCY]
# Concurrency 4 means 4 jobs running at once on the 32-core node (8 cores each).
# Adjust based on your basis set and method memory requirements.
python3 run_batch_manager.py ${START_ID} ${END_ID} 4

echo "Batch completed."
