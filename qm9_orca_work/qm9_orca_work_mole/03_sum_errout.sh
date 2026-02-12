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
SOURCE_ROOT="${SOURCE_ROOT:-/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/source_files}"
ORCA_ERRS_DIR="${WORK_DIR:-$(pwd)}/orca_errs"  # Aggregate errs here

mkdir -p "${ORCA_ERRS_DIR}"

echo "=== ORCA QM9 Error Cleanup ==="
echo "Scanning molecule dirs in: ${SOURCE_ROOT}"
echo ""

# Find all molecule dirs (pattern dsgdb9nsd_*)
find "${SOURCE_ROOT}" -mindepth 1 -maxdepth 1 -type d -name "dsgdb9nsd_*" | while read -r mol_dir; do
    mol_name=$(basename "${mol_dir}")
    echo "Processing molecule: ${mol_name} (${mol_dir})"
    
    # Find all .out in this mol dir
    find "${mol_dir}" -type f -name "*.out" | while read -r outfile; do
        jobname="${outfile%.out}"
        inpfile="${jobname}.inp"
        out_file="${outfile}"
        errfile="${jobname}.err"
        
        if [[ ! -f "${inpfile}" ]]; then
            # echo "  Warning: No .inp for ${outfile}, skipping"
            continue
        fi
        
        if [[ -f "${errfile}" ]]; then
            # echo "  Skipped (has .err): ${jobname}"
            continue
        fi
        
        has_error=false
        for pattern in "${ERROR_PATTERNS[@]}"; do
            if grep -qiE "${pattern}" "${out_file}"; then
                has_error=true
                echo "  ERROR: ${jobname} (matched: ${pattern})"
                break
            fi
        done
        
        if ${has_error}; then
            # echo "=== Error Report ===" > "${errfile}"
            echo "Job: ${jobname}" >> "${errfile}"
            echo "Date: $(date)" >> "${errfile}"
            echo "Detected errors:" >> "${errfile}"
            for pattern in "${ERROR_PATTERNS[@]}"; do
                if grep -qiE "${pattern}" "${out_file}"; then
                    echo "  - ${pattern}" >> "${errfile}"
                fi
            done
            echo "Last 25 lines:" >> "${errfile}"
            tail -n 25 "${out_file}" >> "${errfile}"

            rm -vf "${jobname}".!(inp|out|err) 2>/dev/null
            mv "${out_file}" "${ORCA_ERRS_DIR}/${mol_name}_${jobname##*/}.out"
            mv "${errfile}" "${ORCA_ERRS_DIR}/${mol_name}_${jobname##*/}.err"
            echo "  → Cleanup done"
        fi
    done
done

echo "Cleanup finished. Failed jobs in ${ORCA_ERRS_DIR}."