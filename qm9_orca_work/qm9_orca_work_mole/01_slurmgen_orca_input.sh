#!/bin/bash
#####################################################################################
###                                                                                 #
### slurm-orca-gen.cmd :                                                            #
### Generate ORCA input files in parallel on HPC2021                                #
###                                                                                 #
#####################################################################################

#SBATCH --job-name=orca-gen
#SBATCH --mail-type=END,FAIL
##SBATCH --mail-user=your@email
##SBATCH --partition=intel
#SBATCH --time=02:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=16G
#SBATCH --output=%x.out.%j
#SBATCH --error=%x.err.%j

set -uo pipefail

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
echo -e "Generating ORCA input files for single element atoms: basis: ${BASIS_SETS[*]}, methods: ${METHODS[*]}"

xyz_dir="/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files"
script_dir="/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources"

ORCA_MEM="%maxcore 4000"
file_count=0

get_basis_config() {
    local basis_key=$1
    local basis_name="${BASIS_MAP[$basis_key]}"

    if [[ -z "${basis_name}" ]]; then
        echo "def2/JK RIJK def2-SVP/C"
        return
    fi

    if [[ "$basis_key" =~ ^def2 ]]; then
        echo "def2/JK RIJK ${basis_name}/C"
    elif [[ "$basis_key" == "ccpvdz" ]]; then
        echo "cc-pVTZ/JK RIJK ${basis_name}/C"
    elif [[ "$basis_key" =~ ^(ccpvtz|aug-ccpvtz) ]]; then
        echo "${basis_name}/JK RIJK ${basis_name}/C"
    else
        echo "def2/JK RIJK def2-SVP/C"
    fi
}

process_xyz_file() {
    local xyz_file_path=$1
    if [[ ! -f "${xyz_file_path}" ]]; then
        echo "Warning: File ${xyz_file_path} does not exist, skip"
        return
    fi

    mole_filename=$(basename "${xyz_file_path}")
    mole_lc="${mole_filename%.xyz}"

    for method in "${METHODS[@]}"; do
        orca_method="${METHOD_MAP[$method]}"
        method_file="${method//(/}"
        method_file="${method_file//)/}"
        method_lc="${method_file,,}"

        method_parent_dir="${script_dir}/${method_lc}"
        mkdir -p "${method_parent_dir}" || { echo "Warning: Failed to create ${method_parent_dir}"; continue; }

        for basis_key in "${BASIS_SETS[@]}"; do
            basis_name="${BASIS_MAP[$basis_key]}"
            aux_basis=$(get_basis_config "$basis_key")

            basis_method_dir="${method_parent_dir}/${basis_key}_${method_lc}"
            mkdir -p "${basis_method_dir}" || { echo "Warning: Failed to create ${basis_method_dir}"; continue; }

            infile="${basis_method_dir}/${mole_lc}_${method_lc}_${basis_key}.inp"

            if [ ! -f "${infile}" ]; then
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
*xyzfile 0 1 ${xyz_file_path}
EOF
                if [[ -f "${infile}" ]]; then
                    ((file_count++))
                    if (( file_count % 5000 == 0 )); then
                        echo "[$(date)] Generated ${file_count} input files"
                    fi
                else
                    echo "Error: Failed to generate ${infile}"
                fi
            fi
        done
    done
}

echo "======== Start processing XYZ files ========"
echo "XYZ directory: ${xyz_dir}"
xyz_files=($(find "${xyz_dir}" -maxdepth 1 -type f -name "dsgdb9nsd_*.xyz" | sort))
total_xyz=${#xyz_files[@]}
echo "Total XYZ files found: ${total_xyz}"
echo "============================================"

if [[ ${total_xyz} -eq 0 ]]; then
    echo "Error: No XYZ files found in ${xyz_dir}"
    exit 1
fi

START_IDX=${1:-}
END_IDX=${2:-}

if [[ -n "$START_IDX" && -n "$END_IDX" ]]; then
    echo "Processing files from index $START_IDX to $END_IDX"
    for ((i=10#$START_IDX; i<=10#$END_IDX; i++)); do
        printf -v filename "dsgdb9nsd_%06d.xyz" "$i"
        full_path="${xyz_dir}/${filename}"
        if [[ -f "${full_path}" ]]; then
            echo "Processing ${full_path}"
            process_xyz_file "$full_path"
        else
            echo "Warning: ${full_path} not found, skip"
        fi
    done
else
    echo "Processing all ${total_xyz} XYZ files"
    for xyz_file_path in "${xyz_files[@]}"; do
        echo "Processing ${xyz_file_path}"
        process_xyz_file "$xyz_file_path"
    done
fi

echo "=== Final Summary ==="
echo "Total XYZ files: ${total_xyz}"
echo "Total input files generated: ${file_count}"
echo "Output directory: ${script_dir}"