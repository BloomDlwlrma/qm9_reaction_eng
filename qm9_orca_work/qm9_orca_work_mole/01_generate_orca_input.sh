#!/bin/bash
set -euo pipefail

METHODS=("MP2" "CCSD" "CCSD(T)")

# Method and Basis sets mapping: key -> ORCA format
declare -A METHOD_MAP=(
    ["MP2"]="RI-MP2"
    ["CCSD"]="DLPNO-CCSD"
    ["CCSD(T)"]="DLPNO-CCSD(T)"
)
declare -A BASIS_MAP=(
    # ["631g"]="6-31G"
    ["631gs"]="6-31G*"
    # ["631gss"]="6-31G**"
    # ["631+gss"]="6-31+G**"
    # ["def2svp"]="def2-SVP"
    # ["def2tzvp"]="def2-TZVP"
    # ["ccpvdz"]="cc-pVDZ"
    # ["ccpvtz"]="cc-pVTZ"
    # ["aug-ccpvtz"]="aug-cc-pVTZ"
    # ["321g"]="3-21G"
)
# BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "aug-ccpvtz" "321g")
BASIS_SETS=("631gs")

xyz_dir=$(cat ${WORK}/osv_mp2_ml_gen/orca2pyscf/xyz_files/dsgdb9nsd/dsgdb9nsd.txt)
#xyz_dir=/scr/u/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files/mywater
script_dir=$(cat ${WORK}/osv_mp2_ml_gen/orca2pyscf/xyz_files/dsgdb9nsd/dsgdb9nsd_script.txt)
#script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

#ORCA_PAL="%pal nprocs 32 end"
ORCA_MEM="%maxcore 4000"
echo -e "Generating ORCA input files for single element atoms: basis: ${BASIS_SETS[*]}, methods: ${METHODS[*]}"

get_basis_config() {
    local basis_key=$1
    local basis_name="${BASIS_MAP[$basis_key]}"

    if [[ "$basis_key" =~ ^def2 ]]; then
        # def2
        echo "def2/JK RIJK ${basis_name}/C"
    elif [[ "$basis_key" =~ ^(ccpvtz|aug-ccpvtz) ]]; then
        # cc-pvtz, aug-cc-pvtz
        echo "${basis_name}/JK RIJK ${basis_name}/C"
    elif [[ "$basis_key" == "ccpvdz" ]]; then
        echo "cc-pVTZ/JK RIJK ${basis_name}/C"
    else
        # 3-21G, 6-31G, ccpvdz: use def2 auxiliary basis
        echo "def2/JK RIJK def2-SVP/C"
    fi
}

for xyz_file_path in "${xyz_dir}"/*.xyz; do
    mole_filename=$(basename "${xyz_file_path}")
    mole_lc="${mole_filename%.xyz}"
    out_dir="${script_dir}/${mole_lc}"
    mkdir -p "${out_dir}"

    for basis_key in "${BASIS_SETS[@]}"; do
        basis_name="${BASIS_MAP[$basis_key]}"

        for method in "${METHODS[@]}"; do
            orca_method="${METHOD_MAP[$method]}"
            aux_basis=$(get_basis_config "$basis_key")
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"

            infile="${out_dir}/${mole_lc}_${method_lc}_${basis_key}.inp"
            if [ ! -f "${infile}" ]; then
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

*xyzfile 0 1 ${xyz_dir}/${mole_lc}.xyz
EOF
            fi
        # echo "Successfully generated input file: ${infile}"
        done
    done
    # echo "Successfully generated input file: $xyz_file_path"
done

echo "All ORCA input files generated. Overall output directory: ${script_dir}"