#!/bin/bash
set -euo pipefail # exit on error, undefined var, or failed pipe

ATOMS=("C" "H" "O" "N" "F")
METHODS=("MP2" "CCSD")

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${script_dir}/orca_outputs"
mkdir -p "${out_dir}"

echo -e "utilizing methods: ${METHODS[*]} to analyze single element atoms for qm9 database\n"

for atom in "${ATOMS[@]}"; do
    for method in "${METHODS[@]}"; do
        atom_lc="${atom,,}"
        method_lc="${method,,}"

        infile="${script_dir}/${atom_lc}/${atom_lc}_${method_lc}_631g.inp"
        outfile="${out_dir}/${atom_lc}_${method_lc}_631g.out"

        if [[ ! -f "${infile}" ]]; then
            echo "ERROR: missing input file: ${infile}" >&2
            continue
        fi

        echo "Running ORCA: ${infile} -> ${outfile}"
        orca "${infile}" > "${outfile}"
    done
done

echo -e "\nAll ORCA calculations completed. Outputs located in: ${out_dir}"
  
  
