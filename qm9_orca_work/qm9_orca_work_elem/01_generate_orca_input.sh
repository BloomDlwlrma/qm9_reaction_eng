#!/bin/bash
set -euo pipefail
declare -A ATOM_MULTIPLICITY=( ["H"]=2  ["C"]=3 ["N"]=4 ["O"]=3 ["F"]=2 )
ATOMS=("C" "H" "O" "N" "F")
METHODS=("MP2" "CCSD")
# BASIS_SET="631g"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)" # script located in same dir as run_all_elementatom_calculations.sh

# ORCA_PAL="%pal nprocs 16 end"
ORCA_MEM="%maxcore 5000"
ORCA_KEYWORDS="TightSCF SCFConvForced"

echo -e "Generating ORCA input files for single element atoms for qm9 database using methods: ${METHODS[*]}"

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}" # C -> c
    multiplicity="${ATOM_MULTIPLICITY["$atom"]}"
    out_dir="${script_dir}/${atom_lc}"
    mkdir -p "${out_dir}"

    for method in "${METHODS[@]}"; do
        method_lc="${method,,}" # MP2 -> mp2
        infile="${out_dir}/${atom_lc}_${method_lc}_631g.inp"

        cat > "${infile}" << EOF # EOF means "End Of File"
! ${method} 6-31G ${ORCA_KEYWORDS}
${ORCA_MEM}
*xyz 0 ${multiplicity}
${atom} 0.0 0.0 0.0
*
EOF
        echo "Successfully generated input file: ${infile}"
    done
done

echo "All ORCA input files generated. Overall output directory: ${script_dir}"