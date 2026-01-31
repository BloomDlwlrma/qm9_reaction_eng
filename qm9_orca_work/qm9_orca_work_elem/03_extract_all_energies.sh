#!/bin/bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${script_dir}/orca_outputs"
SUMMARY_FILE="${script_dir}/QM9_single_atom_energies_summary.csv"

ATOMS=("C" "H" "O" "N" "F")
METHODS=("MP2" "CCSD")
echo "ATOM,METHOD,ENERGY(Hartree)" > "${SUMMARY_FILE}"

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}"
    for method in "${METHODS[@]}"; do
        method_lc="${method,,}"
        outfile="${out_dir}/${atom_lc}_${method_lc}_631g.out"

        if [[ ! -f "${outfile}" ]]; then
            echo "ERROR: missing ${atom_lc} ${method_lc} output file: ${outfile}">> "${SUMMARY_FILE}"
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
        echo "${atom},${method},${energy_value}" >> "${SUMMARY_FILE}"
        echo "Extracted energy for ${atom} with ${method}: ${energy_value} Hartree"
    done
done

echo "Wrote CSV summary to: ${SUMMARY_FILE}"