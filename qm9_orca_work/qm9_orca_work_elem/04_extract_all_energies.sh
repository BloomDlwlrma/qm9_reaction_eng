#!/bin/bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${script_dir}/orca_outputs"
SUMMARY_FILE="${script_dir}/QM9_single_atom_energies_summary.csv"

ATOMS=("C" "H" "O" "N" "F")
METHODS=("MP2" "CCSD" "CCSD(T)")

# Method and Basis sets mapping: key -> ORCA format
declare -A METHOD_MAP=(
    ["MP2"]="RI-MP2"
    ["CCSD"]="DLPNO-CCSD"
    ["CCSD(T)"]="DLPNO-CCSD(T)"
)
declare -A BASIS_MAP=(
    ["631g"]="6-31G"
    ["631gs"]="6-31G*"
    ["631gss"]="6-31G**"
    ["631+gss"]="6-31+G**"
    ["def2svp"]="def2-SVP"
    ["def2tzvp"]="def2-TZVP"
    ["ccpvdz"]="cc-pVDZ"
    ["ccpvtz"]="cc-pVTZ"
    ["321g"]="3-21G"
)
BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "321g")

echo "ATOM,METHOD,BASIS,ENERGY(Hartree)" > "${SUMMARY_FILE}"

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}"
    for basis_key in "${BASIS_SETS[@]}"; do
        basis_name="${BASIS_MAP[$basis_key]}"
        for method in "${METHODS[@]}"; do
            # Create filename-safe method name
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"
            
            outfile="${out_dir}/${atom_lc}_${method_lc}_${basis_key}.out"

            if [[ ! -f "${outfile}" ]]; then
                echo "ERROR: missing ${atom_lc} ${method_lc} ${basis_key} output file: ${outfile}">> "${SUMMARY_FILE}"
                continue
            fi

            # Preferred for correlated methods (MP2/CCSD) and most SP jobs
            energy_line=$(grep -m 1 "FINAL SINGLE POINT ENERGY" "${outfile}" || true)

            # Fallback: SCF-only jobs may only have the TOTAL SCF ENERGY block
            if [[ -z "${energy_line}" ]]; then
                energy_line=$(grep -m 1 "^Total Energy[[:space:]]\+:" "${outfile}" || true)
            fi

            if [[ -z "${energy_line}" ]]; then
                echo "ERROR: could not find energy in file: ${outfile}" >&2
                continue
            fi

            # Parse energy (Hartree) depending on which line matched
            if [[ "${energy_line}" == *"FINAL SINGLE POINT ENERGY"* ]]; then
                energy_value=$(echo "${energy_line}" | awk '{print $5}')
            else
                energy_value=$(echo "${energy_line}" | awk '{print $4}')
            fi
            echo "${atom},${method},${basis_name},${energy_value}" >> "${SUMMARY_FILE}"
            echo "Extracted energy for ${atom} with ${method}/${basis_name}: ${energy_value} Hartree"
        done
    done
done

echo "Wrote CSV summary to: ${SUMMARY_FILE}"