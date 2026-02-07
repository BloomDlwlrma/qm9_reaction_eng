#!/bin/bash
set -euo pipefail # exit on error, undefined var, or failed pipe

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

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${script_dir}/orca_outputs"
mkdir -p "${out_dir}"

echo -e "Utilizing methods: ${METHODS[*]}, basis sets: ${BASIS_SETS[*]} to analyze single element atoms\n"

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}"
    
    for basis_key in "${BASIS_SETS[@]}"; do
        for method in "${METHODS[@]}"; do
            # Create filename-safe method name
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"

            infile="${script_dir}/${atom_lc}/${atom_lc}_${method_lc}_${basis_key}.inp"
            outfile="${out_dir}/${atom_lc}_${method_lc}_${basis_key}.out"

            if [[ ! -f "${infile}" ]]; then
                echo "ERROR: missing input file: ${infile}" >&2
                continue
            fi

            echo "Running ORCA: ${infile} -> ${outfile}"
            orca "${infile}" > "${outfile}"
        done
    done
done

echo -e "\nAll ORCA calculations completed. Outputs located in: ${out_dir}"
  
  
