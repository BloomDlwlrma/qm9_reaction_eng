#!/bin/bash
set -euo pipefail

# Initial set
# declare -A ATOM_MULTIPLICITY=( ["H"]=2  ["C"]=3 ["N"]=4 ["O"]=3 ["F"]=2 )
# ATOMS=("H" "C" "O" "N" "F")
declare -A ATOM_MULTIPLICITY=( ["H"]=2 )
ATOMS=("H")
METHODS=("MP2" "CCSD" "CCSD(T)")

# Method and Basis sets mapping: key -> ORCA format
declare -A METHOD_MAP=(
    ["MP2"]="RI-MP2"
    ["CCSD"]="DLPNO-CCSD"
    ["CCSD(T)"]="DLPNO-CCSD(T)"
)
declare -A METHOD_MAP_H=(
    ["MP2"]="MP2"
    ["CCSD"]="HF"
    ["CCSD(T)"]="HF"
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
    ["aug-ccpvtz"]="aug-cc-pVTZ"
    ["321g"]="3-21G"
)
BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "aug-ccpvtz" "321g")
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# ORCA_PAL="%pal nprocs 16 end"
ORCA_MEM="%maxcore 4000"
echo -e "Generating ORCA input files for single element atoms: basis: ${BASIS_SETS[*]}, methods: ${METHODS[*]}"

get_basis_config() {
    local basis_key=$1
    local basis_name="${BASIS_MAP[$basis_key]}"

    if [[ "$basis_key" =~ ^def2 ]]; then
        # def2
        echo "def2/JK RIJK ${basis_name}/C"
    elif [[ "$basis_key" =~ ^(ccpvtz|aug-ccpvtz) ]]; then
        # cc
        echo "${basis_name}/JK RIJK ${basis_name}/C"
    else
        # 3-21G, 6-31G, ccpvdz: use def2 auxiliary basis
        echo "def2/JK RIJK def2-SVP/C"
    fi
}

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}" # C -> c
    multiplicity="${ATOM_MULTIPLICITY["$atom"]}"
    out_dir="${script_dir}/${atom_lc}"
    mkdir -p "${out_dir}"

    for basis_key in "${BASIS_SETS[@]}"; do
        basis_name="${BASIS_MAP[$basis_key]}"

        for method in "${METHODS[@]}"; do
            # Determine method string and aux_basis based on atom type
            if [[ $atom_lc == "h" ]]; then
                orca_method="${METHOD_MAP_H[$method]}"
                aux_basis="" # For H, use standard basis without RIJK
            else
                orca_method="${METHOD_MAP[$method]}"
                aux_basis=$(get_basis_config "$basis_key")
            fi
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"

            infile="${out_dir}/${atom_lc}_${method_lc}_${basis_key}.inp"

            # For MP2, don't include %mdci block (not needed for single atoms)
            if [[ "$method" == "MP2" && "$atom_lc" != "h" ]]; then
                cat > "${infile}" << EOF
! ${orca_method} ${basis_name} ${aux_basis}
${ORCA_MEM}

*xyz 0 ${multiplicity}
${atom} 0.0 0.0 0.0
*
EOF
            elif [[ ( "$method" == "CCSD" || "$method" == "CCSD(T)" ) && "$atom_lc" != "h" ]]; then
                # For CCSD methods, include %mdci and %loc blocks
                cat > "${infile}" << EOF
! ${orca_method} ${basis_name} ${aux_basis}
${ORCA_MEM}

%mdci
   TCutPairs 1e-6
   printlevel 4
end

%loc
LocMet AHFB
OCC true
end

*xyz 0 ${multiplicity}
${atom} 0.0 0.0 0.0
*
EOF
            else
                # For H with CCSD/CCSD(T), use standard method without RIJK
                cat > "${infile}" << EOF
! ${orca_method} ${basis_name}
${ORCA_MEM} 

*xyz 0 ${multiplicity}
${atom} 0.0 0.0 0.0
*
EOF
            fi
            echo "Successfully generated input file: ${infile}"
        done
    done
done

echo "All ORCA input files generated. Overall output directory: ${script_dir}"