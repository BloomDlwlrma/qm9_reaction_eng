#!/bin/bash
#SBATCH --job-name=QM9_Debug
#SBATCH --partition=amd
#SBATCH --qos=debug
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=32G
#SBATCH --output=./logs/debug_%a_%j.out
#SBATCH --error=./logs/debug_%a_%j.err
# =============================================================================
# Adjusted for QM9: Scan molecule dirs in SOURCE_ROOT for .out errors
# =============================================================================

shopt -s extglob nullglob
ERROR_PATTERNS=(
    "aborting the run"
    "abnormal termination"
    "aborted"
    "killed"
    "segfault"
    "segmentation fault"
    "out of memory"
    "execution failed"
    "error termination"
    "not enough slots"
    "illegal state"
    "\*\*\* Process.*received signal"
    "non-zero exit code"
    "End of error message"
    "Invalid argument"
)

# Use SOURCE_ROOT from env or default
# SOURCE_ROOT="${SOURCE_ROOT:-/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources}"
SOURCE_ROOT="${SOURCE_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/orca_out_ccsdt_631gss}"
MKL_ROOT="${MKL_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/orca_mkl_ccsdt_631gss}"
ORCA_ERRS_DIR="${WORK_DIR:-$(pwd)}/orca_errs"  # Aggregate errs here

mkdir -p "${ORCA_ERRS_DIR}"

find "${SOURCE_ROOT}" -type f -name "*.out" | while read -r out_file; do
    job_base=$(basename "${out_file}" .out)
    mol_id=$(echo "${job_base}" | cut -d'_' -f1-2)
    err_file="${ORCA_ERRS_DIR}/${mol_id}_${job_base}.err"
    mkl_file="${MKL_ROOT}/${job_base}.mkl"
    # echo "Processing job: ${job_base}"

    if [[ -f "${err_file}" ]]; then
        # echo "  Skipped (already processed): ${job_base}"
        continue
    fi

    has_error=false
    matched_patterns=()
    tail_out=$(tail -n 25 "${out_file}")

    for pattern in "${ERROR_PATTERNS[@]}"; do
        if echo "${tail_out}" | grep -qiE "${pattern}"; then # Case-insensitive search：if match found, mark as error and record pattern
            has_error=true
            matched_patterns+=("${pattern}")
            # echo "  ERROR: ${job_base} (matched: ${pattern})"
        fi
    done

    if ${has_error}; then
        {
            for p in "${matched_patterns[@]}"; do
                echo "  - ${p}"
            done
            echo "Last 25 lines of output:"
            tail -n 25 "${out_file}"
        } > "${err_file}"

        if [[ -f "${mkl_file}" ]]; then
            rm -vf "${mkl_file}"
        else
            echo "  relative err mkl file is not reserve: ${job_base}.mkl"
        fi

        # rm -vf "${ORCA_FILES_DIR}/${job_base}".!(inp) 2>/dev/null
        rm -vf "${out_file}"
        # echo "  → Cleanup done for ${job_base}"
    fi
done

echo "Cleanup finished. Failed jobs moved to: ${ORCA_ERRS_DIR}."