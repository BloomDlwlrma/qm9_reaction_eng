# Batch processing of qm9 reaction energies

## Troubleshooting Log for orca_elem
### 1. Slurm & Resource Managment
*   **Checking Partitions**: `sinfo`
*   **Monitoring Jobs**: `squeue -u $USER` or `watch -n 5 squeue -u $USER`
*   **Inspecting Nodes**: `scontrol show node <node>`
*   **Live Output**: `tail -f <jobname>.out`

### 2. Common Errors & Fixes

#### A. Permission Denied in `/var/spool/...`
*   **Error**: `mkdir: cannot create directory ... Permission denied`
*   **Cause**: Script tried to use `$(pwd)` inside a Slurm job before changing directory, or attempted to write to system directories.
*   **Fix**:
    *   Explicitly `cd ${SLURM_SUBMIT_DIR}` at start of script.
    *   Use absolute paths for scratch directories on `/scr` or `/lustre`.

#### B. ORCA_MDCI Parallelization Error
*   **Error**: `Error (ORCA_MDCI): Number of processes (32) in parallel calculation exceeds number of pairs (7)`
*   **Cause**: When calculating **Single Atoms** (e.g., Carbon) with Frozen Core, there are very few electron pairs (valence electrons). Requesting 32 cores is physically impossible to parallelize.
*   **Fix**: Force reduced core count for single atom calculations (e.g., `NPROCS=4` instead of 32/128), regardless of SLURM reservation.

### 3. Workflow Optimization (Batch Script)
*   **Strategy**: "Run in Scratch, Move Back"
*   **Implementation**:
    1.  Create temp `.inp` in fast `${SCRATCH}`.
    2.  Run ORCA in `${SCRATCH}` (generates large `.gbw` files there).
    3.  **Crucial**: Explicitly `mv ${SCRATCH}/${JOBNAME}* ${ATOM_DIR}/` after calculation.
    4.  Clean up temp files.