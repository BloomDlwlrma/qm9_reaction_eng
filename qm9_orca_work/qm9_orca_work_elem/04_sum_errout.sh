#!/usr/bin/env bash
# =============================================================================
# Script: Clean up failed ORCA single-atom calculations
# Purpose: 
#   - Scan all *.out files in ${WORK_DIR}/${atom_lc} directories
#   - Detect common ORCA error patterns
#   - If error found: save error snippet to *.err, keep .inp and .out, delete others
#   - If no error: keep everything (do nothing)
# Author: Sherwin Zhang
echo "Last update: $(date "+%Y/%m/%d -- %H:%M:%S")"
# =============================================================================

# ==================== CONFIG ====================
# Uncomment and modify if you want to run for specific atoms only
# ATOMS=("c" "h" "o" "n" "f")
# For now, we scan all atom subdirectories under WORK_DIR

# Common ORCA error keywords (case-insensitive)
shopt -s extglob nullglob
ERROR_PATTERNS=(
    "aborting the run"
    "Error termination"
    "The MDCI module"
    "mdci_state.cpp"
    "orca_mdci_mpi"
    "not enough slots"
    "There are not enough slots available"
    "illegal state"
    "Segmentation fault"
    "Signal: Aborted"
    "*** Process.*received signal"
    "Primary job .* non-zero exit code"
)

# Base directory (should be the same as your submission script's WORK_DIR)
# If not set in environment, you can hardcode it here
WORK_DIR="${WORK_DIR:-$(pwd)}"   # fallback to current directory if not set

# ==================== MAIN ====================
echo "=== ORCA Failed Job Cleanup Script ==="
echo "Base directory: ${WORK_DIR}"
echo "Scanning subdirectories for atom folders..."
echo ""

# Find all atom directories (assuming lowercase like c, h, o...)
find "${WORK_DIR}" -mindepth 1 -maxdepth 1 -type d | while read -r atom_dir; do
    atom_lc=$(basename "${atom_dir}")
    
    # Skip if not looks like atom folder (optional safety)
    if [[ ! "${atom_lc}" =~ ^[a-z]$ ]]; then
        continue
    fi
    
    echo "Processing atom: ${atom_lc} (${atom_dir})"
    
    # Find all .out files in this atom directory
    find "${atom_dir}" -type f -name "*.out" | while read -r outfile; do
        jobname="${outfile%.out}"
        inpfile="${jobname}.inp"
        errfile="${jobname}.err"
        
        # Skip if no corresponding .inp (unusual case)
        if [[ ! -f "${inpfile}" ]]; then
            echo "  Warning: No .inp found for ${outfile}, skipping cleanup"
            continue
        fi
        
        # Check if already has .err → probably already processed
        if [[ -f "${errfile}" ]]; then
            echo "  Skipped (already has .err): ${jobname}"
            continue
        fi
        
        # Default: assume success
        has_error=false
        
        # Search for any error pattern
        for pattern in "${ERROR_PATTERNS[@]}"; do
            if grep -qiE "${pattern}" "${outfile}"; then
                has_error=true
                echo "  ERROR DETECTED: ${jobname}  (matched: ${pattern})"
                break
            fi
        done
        
        if ${has_error}; then
            echo "=== ORCA Calculation Error Report ===" > "${errfile}"
            echo "Job: ${jobname}" >> "${errfile}"
            echo "Date: $(date)" >> "${errfile}"
            echo "Detected error keywords:" >> "${errfile}"
            for pattern in "${ERROR_PATTERNS[@]}"; do
                if grep -qiE "${pattern}" "${outfile}"; then
                    echo "  - ${pattern}" >> "${errfile}"
                fi
            done
            echo "Last 50 lines of output:" >> "${errfile}"
            tail -n 50 "${outfile}" >> "${errfile}"
            
            echo "  Cleaning up failed job files..."
            shopt -s extglob nullglob
            rm -vf "${jobname}".!(inp|out|err) 2>/dev/null
            # Also remove possible .err if we want to overwrite, but we already created new one
            
            echo "  → Cleanup done for ${jobname}"
        else
            echo "  OK (no error detected): ${jobname}"
        fi
        
        echo ""
    done
done

echo "=== Cleanup finished ==="
echo "  - Failed jobs have .err files and most auxiliary files removed"
echo "  - Successful jobs are untouched"
echo ""