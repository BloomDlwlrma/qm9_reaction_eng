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

## Automated Batch Processing for QM9 Molecules
This section summarizes the automated batch processing solution for high-throughput QM9 molecule calculations, enabling efficient parallel processing across multiple nodes with CPU core binding.

### Core Scripts

1. **`run_batch_manager.py` (Python Manager Script)**
   - **File Management**: Automatically copies input files (MP2, CCSD, CCSD(T) with 631gs basis) from source directories (`/lustre1/.../source_files/`) to working directory based on molecule ID ranges.
   - **Dynamic Configuration**: Modifies `%pal nprocs` in input files to match allocated cores per task.
   - **CPU Core Binding**: Uses `taskset` for core binding (e.g., 4 concurrent jobs on a 32-core node, each bound to 8 cores: `0-7`, `8-15`, etc.), reducing context switching overhead as per ORCA optimization guidelines.
   - **Automated Workflow**: Runs ORCA → Integrated error checking (replaces `03_sum_errout.sh`) → Converts to MKL format (`orca2mkl.sh` logic).
   - **Result Archiving**: Aggregates `.out` and `.mkl` files to `orca_output` directories, and moves all generated files back to original source molecule folders.

2. **`04_submit_batch.cmd` (Slurm Submission Script)**
   - **Multi-Node Support**: Uses Slurm Job Arrays (`--array`) for scaling across nodes.
   - **Configuration**: Defines molecule ID ranges (e.g., `BASE_ID=38135`, `CHUNK_SIZE=100` for 100 molecules per node).
   - **Submission**: Launches parallel jobs; e.g., `--array=0-9` processes 1000 molecules across 10 nodes.

### Usage Instructions

1. **Adjust Ranges (Optional)**:
   - Edit `04_submit_batch.cmd` to set `BASE_ID` (starting molecule ID) and `CHUNK_SIZE` (molecules per node).

2. **Submit Jobs**:
   - For 100 molecules on 1 node: `sbatch --array=0-0 04_submit_batch.cmd`
   - For 500 molecules on 5 nodes: `sbatch --array=0-4 04_submit_batch.cmd`

### Key Features
- **Input Handling**: Automatically locates and copies `dsgdb9nsd_XXXXXX` directories with MP2/CCSD/CCSD(T) 631gs inputs.
- **Efficiency**: Default 4 concurrent tasks per node (8 cores each); adjustable for memory/basis requirements.
- **Data Safety**: Backs up `.out` and `.mkl` to repo, moves all files back to source directories post-computation.
- **Error Handling**: Integrated checks for common ORCA errors, generates `.err` files as needed.

Avoid using the old `_run_sig_mole_slurm.cmd`; use this new system for scalable QM9 processing.