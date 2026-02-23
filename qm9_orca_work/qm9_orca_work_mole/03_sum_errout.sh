#!/usr/bin/env bash
# =============================================================================
# Adjusted for QM9: Scan molecule dirs in SOURCE_ROOT for .out errors
# =============================================================================

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

# Use SOURCE_ROOT from env or default
# SOURCE_ROOT="${SOURCE_ROOT:-/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources}"
SOURCE_ROOT="${SOURCE_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/orca_out}"
MKL_ROOT="${MKL_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/orca_mkl}"
ORCA_ERRS_DIR="${WORK_DIR:-$(pwd)}/orca_errs"  # Aggregate errs here

mkdir -p "${ORCA_ERRS_DIR}"

echo "=== ORCA QM9 Error Cleanup ==="
echo "Scanning molecule dirs in: ${SOURCE_ROOT}"
echo ""

find "${SOURCE_ROOT}" -type f -name "*.out" | while read -r out_file; do
    job_base=$(basename "${out_file}" .out)
    mol_id=$(echo "${job_base}" | cut -d'_' -f1-2)
    err_file="${ORCA_ERRS_DIR}/${mol_id}_${job_base}.err"
    mkl_file="${MKL_ROOT}/${job_base}.mkl"
    # echo "Processing job: ${job_base}"

    if [[ -f "${err_file}" ]]; then
        echo "  Skipped (already processed): ${job_base}"
        continue
    fi

    has_error=false
    matched_patterns=()
    for pattern in "${ERROR_PATTERNS[@]}"; do
        if grep -qiE "${pattern}" "${out_file}"; then
            has_error=true
            matched_patterns+=("${pattern}")
            echo "  ERROR: ${job_base} (matched: ${pattern})"
        fi
    done

    if ${has_error}; then
        {
            echo "Job: ${job_base}"
            echo "Date: $(date)"
            echo "Detected errors:"
            for p in "${matched_patterns[@]}"; do
                echo "  - ${p}"
            done
            echo "Last 25 lines of output:"
            tail -n 25 "${out_file}"
        } > "${err_file}"

        if [[ -f "${mkl_file}" ]]; then
            echo "  remove err task mkl: ${mkl_file}"
            rm -vf "${mkl_file}"
        else
            echo "  relative err mkl file is not reserve: ${job_base}.mkl"
        fi

        echo "  Cleaning temp files for ${job_base}..."
        rm -vf "${ORCA_FILES_DIR}/${job_base}".!(inp) 2>/dev/null

        mv -v "${out_file}" "${ORCA_ERRS_DIR}/${mol_id}_${job_base}.out"

        echo "  → Cleanup done for ${job_base}"
    fi
done

echo "Cleanup finished. Failed jobs moved to: ${ORCA_ERRS_DIR}."