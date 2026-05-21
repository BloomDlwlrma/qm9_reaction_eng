#!/bin/bash
set -euo pipefail
# ==========================================
# 路径与配置
# ==========================================
xyz_dir="${XYZ_ROOT:-/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files}"
script_dir="${INPUT_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/inp_files}"

# 分块大小设定
CHUNK_SIZE=16000

# 计算方法
METHODS=("CCSD(T)")

declare -A METHOD_MAP=(
    ["CCSD(T)"]="DLPNO-CCSD(T)"
)

# AO basis 列表和默认的 JK-fit / MP2-fit 映射
BASIS_SETS=(
    # "ccpvdz"
    # "ccpvtz"
    # "augccpvtz"
    # "def2svp"
    # "def2tzvp"
    # "321g"
    "631g"
    # "631g*"
    # "631g**"
    # "631+g*"
)

if [[ -n "${SINGLE_BASIS_KEY:-}" ]]; then
    BASIS_SETS=("${SINGLE_BASIS_KEY}")
fi

declare -A BASIS_LABEL_MAP=(
    # ["ccpvdz"]="cc-pVDZ"
    # ["ccpvtz"]="cc-pVTZ"
    # ["augccpvtz"]="aug-cc-pVTZ"
    # ["def2svp"]="def2-SVP"
    # ["def2tzvp"]="def2-TZVP"
    # ["321g"]="3-21G"
    ["631g"]="6-31G"
    # ["631g*"]="6-31G*"
    # ["631g**"]="6-31G**"
    # ["631+g*"]="6-31+G*"
)

declare -A DEFAULT_AUXBASIS=(
    # ["ccpvdz"]="cc-pvdz-jkfit|cc-pvdz-ri"
    # ["ccpvtz"]="cc-pvtz-jkfit|cc-pvtz-ri"
    # ["augccpvtz"]="aug-cc-pvtz-jkfit|aug-cc-pvtz-ri"
    # ["def2svp"]="def2-svp-jkfit|def2-svp-ri"
    # ["def2tzvp"]="def2-tzvp-jkfit|def2-tzvp-ri"
    # ["321g"]="def2-svp-jkfit|def2-svp-ri"
    ["631g"]="def2-svp-jkfit|def2-svp-ri"
    # ["631g*"]="cc-pvdz-jkfit|cc-pvdz-ri"
    # ["631g**"]="cc-pvdz-jkfit|cc-pvdz-ri"
    # ["631+g*"]="heavy-aug-cc-pvdz-jkfit|heavy-aug-cc-pvdz-ri"
)

ORCA_MEM="%maxcore 4000"

file_count=0
skip_count=0

# ==========================================
# 核心新增：修复 XYZ 坐标中无法识别的科学计数法
# ==========================================
fix_xyz_file() {
    local file=$1
    if grep -q -E "\*\^|[eE][+-]" "$file"; then
        awk '
        NR<=2 { print $0; next }
        NF>=4 {
            gsub(/\*\^/, "e", $2)
            gsub(/\*\^/, "e", $3)
            gsub(/\*\^/, "e", $4)
            
            printf "%-4s %18.10f %18.10f %18.10f", $1, $2, $3, $4
            
            for(i=5; i<=NF; i++) printf " %s", $i
            printf "\n"
            next
        }
        { print $0 }
        ' "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"
        
        echo "    [Fixed] Corrected scientific notation in: $(basename "$file")"
    fi
}

get_basis_config() {
    local basis_key=$1
    local aux_pair="${DEFAULT_AUXBASIS[$basis_key]:-}"
    [[ -n "${aux_pair}" ]] || return 1

    local jk_fit_pyscf="${aux_pair%%|*}"
    local mp2_fit_pyscf="${aux_pair#*|}"
    local jk_fit_orca
    local mp2_fit_orca

    to_orca_aux_label() {
        local pyscf_label="$1"
        case "$pyscf_label" in
            cc-pvdz-jkfit) echo "cc-pVDZ/JK" ;;
            cc-pvdz-ri) echo "cc-pVDZ/C" ;;
            cc-pvtz-jkfit) echo "cc-pVTZ/JK" ;;
            cc-pvtz-ri) echo "cc-pVTZ/C" ;;
            aug-cc-pvtz-jkfit) echo "aug-cc-pVTZ/JK" ;;
            aug-cc-pvtz-ri) echo "aug-cc-pVTZ/C" ;;
            def2-svp-jkfit) echo "def2/JK" ;;
            def2-svp-ri) echo "def2-SVP/C" ;;
            def2-tzvp-jkfit) echo "def2/JK" ;;
            def2-tzvp-ri) echo "def2-TZVP/C" ;;
            heavy-aug-cc-pvdz-jkfit) echo "aug-cc-pVDZ/JK" ;;
            heavy-aug-cc-pvdz-ri) echo "aug-cc-pVDZ/C" ;;
            *) return 1 ;;
        esac
    }

    jk_fit_orca="$(to_orca_aux_label "$jk_fit_pyscf")" || return 1
    mp2_fit_orca="$(to_orca_aux_label "$mp2_fit_pyscf")" || return 1

    echo "${jk_fit_orca} RIJK ${mp2_fit_orca}"
}

process_xyz_file() {
    local xyz_file_path=$1
    if [[ ! -f "${xyz_file_path}" ]]; then
        echo "Warning: File ${xyz_file_path} does not exist, skip"
        return
    fi

    # 修复 xyz 文件
    fix_xyz_file "${xyz_file_path}"

    mole_filename=$(basename "${xyz_file_path}")
    mole_lc="${mole_filename%.xyz}" # dsgdb9nsd_000001
    
    # 【修复重点】：只截取下划线后面的部分，然后用 10# 强制按十进制解析，去除前导零
    mol_id_str="${mole_lc##*_}"      # 提取出 000001
    mol_id=$(( 10#$mol_id_str ))     # 转化为整数 1，避免 Bash 把它当成八进制报错

    chunk_idx=$(( (mol_id - 1) / CHUNK_SIZE ))
    chunk_start=$(( chunk_idx * CHUNK_SIZE + 1 ))
    chunk_end=$(( chunk_start + CHUNK_SIZE - 1 ))
    chunk_dir="${chunk_start}_${chunk_end}"

    for method in "${METHODS[@]}"; do
        orca_method="${METHOD_MAP[$method]}"
        method_file="${method//(/}"
        method_file="${method_file//)/}"
        method_lc="${method_file,,}"

        for basis_key in "${BASIS_SETS[@]}"; do
            basis_name="${BASIS_LABEL_MAP[$basis_key]}"
            aux_basis="$(get_basis_config "$basis_key")"

            if [[ -z "${basis_name}" ]]; then
                echo "Warning: unknown basis key ${basis_key}, skip"
                continue
            fi
            if [[ -z "${aux_basis}" ]]; then
                echo "Warning: no ORCA aux mapping for basis key ${basis_key}, skip"
                continue
            fi

            # 自动生成如 inp_files/ccsdt/631gss_ccsdt/1_16000 的目录
            target_dir="${script_dir}/${method_lc}/${basis_key}_${method_lc}/${chunk_dir}"
            mkdir -p "${target_dir}"

            infile="${target_dir}/${mole_lc}_${method_lc}_${basis_key}.inp"

            if [[ -f "${infile}" ]]; then
                ((++skip_count))
                continue
            fi
            
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
            ((++file_count))
        done
    done
}

echo "======== Start processing XYZ files ========"
mapfile -t xyz_files < <(find "${xyz_dir}" -maxdepth 1 -type f -name "dsgdb9nsd_*.xyz" | sort)
total_xyz=${#xyz_files[@]}

if [[ ${total_xyz} -eq 0 ]]; then
    echo "Error: No XYZ files found in ${xyz_dir}"
    exit 1
fi

START_IDX=${1:-1}
END_IDX=${2:-$total_xyz}

if [[ -n "${SLURM_ARRAY_TASK_ID:-}" && $# -eq 0 ]]; then
    START_IDX="${SLURM_ARRAY_TASK_ID}"
    END_IDX="${SLURM_ARRAY_TASK_ID}"
fi

echo "Processing molecules from ${START_IDX} to ${END_IDX} (Chunk size: ${CHUNK_SIZE})"

for ((i=10#$START_IDX; i<=10#$END_IDX; i++)); do
    printf -v filename "dsgdb9nsd_%06d.xyz" "$i"
    full_path="${xyz_dir}/${filename}"
    if [[ -f "${full_path}" ]]; then
        process_xyz_file "$full_path"
    fi
    if (( i % 1000 == 0 )); then
        echo "Processed up to ID: $i"
    fi
done

echo "=== Final Summary ==="
echo "Total input files generated: ${file_count}"
echo "Total existing files skipped: ${skip_count}"