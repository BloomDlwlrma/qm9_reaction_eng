#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

START_MOL="${START_MOL:-1000}"
END_MOL="${END_MOL:-2000}"
INPUT_ROOT="${INPUT_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/inp_files}"
XYZ_ROOT="${XYZ_ROOT:-/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files}"
LOG_ROOT="${LOG_ROOT:-${SCRIPT_DIR}/logs/orca_inp}"

BASIS_KEYS=(
    "631g**"
    "631+g*"
    "augccpvtz"
)

for basis_key in "${BASIS_KEYS[@]}"; do
    basis_safe="$basis_key"
    basis_safe="${basis_safe//+/p}"
    basis_safe="${basis_safe//\*/s}"
    basis_safe="$(echo "$basis_safe" | tr -cd 'A-Za-z0-9_')"
    basis_log_dir="${LOG_ROOT}/${basis_safe}"
    mkdir -p "$basis_log_dir"

    sbatch \
        --job-name="orca-inp-${basis_safe}" \
        --output="${basis_log_dir}/%j.out" \
        --error="${basis_log_dir}/%j.err" \
        --export=ALL,INPUT_ROOT="${INPUT_ROOT}",XYZ_ROOT="${XYZ_ROOT}",SINGLE_BASIS_KEY="${basis_key}" \
        --wrap="bash '${SCRIPT_DIR}/gen_orca_input.sh' '${START_MOL}' '${END_MOL}'"
done