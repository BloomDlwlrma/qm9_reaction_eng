#!/bin/bash
# Auto batch submission: use debug QOS + small batches to avoid QOS limits and increase priority

# Configuration (test 1-1000 molecules)
TOTAL_MOLECULES=1000
CHUNK_SIZE=50                 # Each SLURM job processes 50 molecules (~150 tasks, runs 20-60 min)
BATCH_SIZE=3                  # Submit 3 SLURM jobs per batch (occupies 3 slots, debug safe <6)
SLEEP_BASE=300                # Base interval 5 min
EXTRA_SLEEP_IF_PENDING_GT=3   # Extra sleep 10 min if pending >3

# Calculate total batches
TOTAL_BATCHES=$(( (TOTAL_MOLECULES + CHUNK_SIZE - 1) / (BATCH_SIZE * CHUNK_SIZE) ))
echo "Plan to submit $TOTAL_BATCHES batches, each with $BATCH_SIZE SLURM jobs, covering molecules 1-$TOTAL_MOLECULES"

for batch_id in $(seq 0 $((TOTAL_BATCHES - 1))); do
    # Calculate array range & molecule range for this batch
    array_start=$(( batch_id * BATCH_SIZE ))
    array_end=$(( array_start + BATCH_SIZE - 1 ))
    global_offset=$(( batch_id * BATCH_SIZE * CHUNK_SIZE ))
    batch_start_mol=$(( 1 + global_offset ))
    batch_end_mol=$(( batch_start_mol + BATCH_SIZE * CHUNK_SIZE - 1 ))
    if [ $batch_end_mol -gt $TOTAL_MOLECULES ]; then
        batch_end_mol=$TOTAL_MOLECULES
    fi

    echo "=== Batch $batch_id: array $array_start-$array_end (molecules $batch_start_mol-$batch_end_mol) ==="

    # Generate temporary SLURM script
    temp_cmd="temp_batch_${batch_id}.cmd"
    cat > "$temp_cmd" << EOF
#!/bin/bash
#SBATCH --job-name=qm9_test_b${batch_id}
#SBATCH --partition=amd           # amd has large memory, starts faster
#SBATCH --qos=debug               # High priority (50000), starts very fast
#SBATCH --time=00:30:00           # Test short time limit
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64        # amd nodes commonly have 64/128 cores
#SBATCH --mem=80G                 # amd abundant
#SBATCH --array=${array_start}-${array_end}
#SBATCH --output=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.out
#SBATCH --error=/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_rundir_info/batch_%A_%a.err
##SBATCH --mail-type=END,FAIL
##SBATCH --mail-user=1070461445@qq.com

BASE_ID=${batch_start_mol}
CHUNK_SIZE=${CHUNK_SIZE}
OFFSET=\$((SLURM_ARRAY_TASK_ID * CHUNK_SIZE))
START_ID=\$((BASE_ID + OFFSET))
END_ID=\$((START_ID + CHUNK_SIZE - 1))

echo "Batch ${batch_id} Array \$SLURM_ARRAY_TASK_ID: \$START_ID ~ \$END_ID on \$(hostname)"

module purge
module load openmpi/gcc/4.1.6-gcc12.3

export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=\${ORCA_HOME}/bin:\$PATH
export LD_LIBRARY_PATH=\${ORCA_HOME}/lib:\$LD_LIBRARY_PATH

cd /scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole

python run_batch_manager.py \${START_ID} \${END_ID} 4

echo "Batch ${batch_id} Array \$SLURM_ARRAY_TASK_ID completed."
EOF

    # Submit
    sbatch "$temp_cmd"
    echo "Submitted batch $batch_id"

    # Interval + monitor pending
    sleep $SLEEP_BASE
    pending=$(squeue -u $USER -t PD | wc -l)
    echo "Pending jobs: $pending"
    if [ $pending -gt $EXTRA_SLEEP_IF_PENDING_GT ]; then
        echo "Pending high, extra wait 10 min..."
        sleep 600
    fi

    rm "$temp_cmd"  # Clean up temporary
done

echo "All batches submitted! Monitor: watch -n 60 'squeue -u \$USER'"