#!/bin/bash
#SBATCH --job-name=AutoSubmitter
#SBATCH --partition=intel  
#SBATCH --qos=long              
#SBATCH --time=14-00:00:00      
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1       
#SBATCH --mem=1G                
#SBATCH --output=monitor_%j.log 

# ==============================================================================
# Automated submission monitoring script
# This script runs on a compute node to prevent login node freezing.
# ==============================================================================

# 1. Define log file
LOG_FILE="auto_submitter.log"

# 2. Define Base Directory (Ensure this path is correct!)
BASE_DIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"

# 3. Define tasks to monitor (Format: "JobName:ScriptPath")
# IMPORTANT: The JobName here MUST match the #SBATCH --job-name in your actual scripts.
TASKS=(
    "QM9_Intel:${BASE_DIR}/04_final_intelbatch.cmd"
    "QM9_AMD_Even:${BASE_DIR}/04_final_amdbatch.cmd"
    "QM9_Condo_Odd:${BASE_DIR}/04_final_condoamdbatch.cmd"
)

# Helper function: log messages
log_msg() {
    # Print to stdout (for .out file) AND append to log file
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Ensure log file exists
touch "$LOG_FILE"

log_msg "=== Monitor Daemon Started on Node $(hostname) (Job ID: $SLURM_JOB_ID) ==="
log_msg "Monitored tasks: ${#TASKS[@]} tasks"

# Verify scripts exist before starting loop
for task in "${TASKS[@]}"; do
    SCRIPT="${task##*:}"
    if [ ! -f "$SCRIPT" ]; then
        log_msg "CRITICAL ERROR: Script not found: $SCRIPT"
        # Optional: exit if scripts are missing, or just warn
    else
        log_msg "  - Found: ${task%%:*} -> $SCRIPT"
    fi
done
echo "---------------------------------------------------"

# Start Infinite Loop
while true; do
    log_msg "Checking queue status..."

    # Get all current user's jobs
    CURRENT_JOBS=$(squeue --me --format="%.200j" --noheader 2>/dev/null)

    # Check execution status
    if [ $? -ne 0 ]; then
        log_msg "Warning: squeue command failed, retrying in 0.5h..."
        sleep 1800
        continue
    fi

    # Iterate over tasks
    for entry in "${TASKS[@]}"; do
        JOB_NAME="${entry%%:*}"
        SCRIPT="${entry##*:}"

        # Double check script existence inside loop (in case you edit/move them)
        if [ ! -f "$SCRIPT" ]; then
            log_msg "Error: Script file $SCRIPT missing, skipping."
            continue
        fi

        # Check if job is in the queue
        if echo "$CURRENT_JOBS" | grep -F -q "$JOB_NAME"; then
            # Found in queue
            echo "[$(date '+%H:%M:%S')] [RUNNING] $JOB_NAME"
        else
            # Not found -> Resubmit
            log_msg "[STOPPED] Task '$JOB_NAME' is not in queue. Resubmitting..."
            
            # Run sbatch
            SUBMIT_OUT=$(sbatch "$SCRIPT" 2>&1)
            
            if [ $? -eq 0 ]; then
                # Extract Job ID
                NEW_JOB_ID=$(echo "$SUBMIT_OUT" | awk '{print $NF}')
                log_msg "[SUCCESS] Resubmitted $JOB_NAME (New Job ID: $NEW_JOB_ID)"
            else
                log_msg "[FAILED] Could not submit $JOB_NAME. Error: $SUBMIT_OUT"
            fi
        fi
    done

    log_msg "Check complete. Sleeping 2 hours (7200s)..."
    echo "---------------------------------------------------"
    
    # Sleep for 2 hours
    sleep 7200
done