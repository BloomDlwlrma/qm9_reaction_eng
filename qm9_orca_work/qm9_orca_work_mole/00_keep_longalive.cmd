#!/bin/bash
#SBATCH --job-name=16kAuto
#SBATCH --partition=intel
#SBATCH --qos=long
#SBATCH --time=14-00:00:00
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --output=./autolog/monitor_%j.log

# ==============================================================================
# Automated submission monitoring script
# Function: Check if specified jobs are in the queue; if not, resubmit them automatically
# nohup ./00_keep_longalive.cmd > keep_longalive.log 2>&1 &
# ==============================================================================

# Define log file
LOG_FILE="./autolog/auto_longsubmitter.log"

# Define tasks to monitor (format: "JobName:ScriptPath")
# Note: JobName must match the #SBATCH --job-name in your .slurm script
BASE_DIR="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
TASKS=(
    "QM9_Even:$BASE_DIR/04_final_condoevenbatch.cmd"
    "QM9_Odd:$BASE_DIR/04_final_condooddbatch.cmd"
    "QM9_Intel:$BASE_DIR/04_final_intelbatch.cmd"
    "QM9_AMD:$BASE_DIR/04_final_amdbatch.cmd"
    "QM9_Huge:$BASE_DIR/04_final_hugemembatch.cmd"
)

# Helper function: log messages
log_msg() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Ensure log file exists
touch "$LOG_FILE"

log_msg "=== Daemon started (PID: $$) ==="
log_msg "Monitored tasks: ${#TASKS[@]} tasks"
for task in "${TASKS[@]}"; do
    log_msg "  - ${task%%:*} -> ${task##*:}"
done
echo "---------------------------------------------------"

while true; do
    log_msg "Checking queue status..."

    # Get all current user's jobs (only JobName to reduce data)
    # Use squeue --me --format="%.200j" to list job names
    CURRENT_JOBS=$(squeue --me --format="%.200j" --noheader 2>/dev/null)

    # Check whether squeue executed successfully
    if [ $? -ne 0 ]; then
        log_msg "Warning: squeue command failed, skipping this check."
        sleep 60
        continue
    fi

    # Iterate over each task for checking
    for entry in "${TASKS[@]}"; do
        JOB_NAME="${entry%%:*}"
        SCRIPT="${entry##*:}"

        # Check if script file exists
        if [ ! -f "$SCRIPT" ]; then
            log_msg "Error: Script file $SCRIPT not found, skipping."
            continue
        fi

        # Check if job is in the queue (exact match)
        # grep -q: quiet mode; -F: fixed string; -x: whole line match (not used here because squeue output may contain spaces)
        if echo "$CURRENT_JOBS" | grep -F -q "$JOB_NAME"; then
            # Job exists, print a short status
            echo "[$(date '+%H:%M:%S')] [RUNNING] $JOB_NAME"
        else
            # Job not found; need to resubmit
            log_msg "[Stopped] Task '$JOB_NAME' is not in the queue. Preparing to resubmit..."
            
            # Submit job
            SUBMIT_OUT=$(sbatch "$SCRIPT" 2>&1)
            RET_CODE=$?

            if [ $RET_CODE -eq 0 ]; then
                # Extract Job ID (sbatch output usually contains "Submitted batch job 123456")
                JOB_ID=$(echo "$SUBMIT_OUT" | awk '{print $NF}')
                log_msg "[SUCCESS] Resubmitted $JOB_NAME (Job ID: $JOB_ID)"
            else
                log_msg "[FAILED] Submission of $JOB_NAME failed! Error: $SUBMIT_OUT"
            fi
        fi
    done

    log_msg "Check complete. Sleeping 201600 seconds (56 hours)..."
    echo "---------------------------------------------------"
    sleep 201600
done
