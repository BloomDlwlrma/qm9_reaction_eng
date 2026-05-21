#!/bin/bash
#SBATCH --job-name=rerun-failed
#SBATCH --partition=condo_amd
#SBATCH --qos=normal
#SBATCH --time=1-00:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=192
#SBATCH --ntasks-per-node=192
#SBATCH --mem=720G
#SBATCH --output=./logs/rerun_%j.out
#SBATCH --error=./logs/rerun_%j.err

set -euo pipefail
ulimit -l unlimited
cd "$SLURM_SUBMIT_DIR"

export WORK_ROOT="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
export OUTPUT_ROOT="/lustre1/g/chem_yangjun/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
export ORCA_SCR="/tmp/$USER/orca_jobs_${SLURM_JOB_ID}"
export METHOD="${METHOD:-ccsdt}"
export BASIS="${BASIS:-631g**}"
export SOURCE_ROOT="${SOURCE_ROOT:-/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/orca_output/inp_files}"
export NEW_XYZ_ROOT="/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files"
export FINAL_OUT_BASE="${OUTPUT_ROOT}/orca_output/orca_out_${METHOD}_${BASIS}"
export FINAL_MKL_BASE="${OUTPUT_ROOT}/orca_output/orca_mkl_${METHOD}_${BASIS}"
export FINAL_LOCMKL_BASE="${OUTPUT_ROOT}/orca_output/orca_locmkl_${METHOD}_${BASIS}"
export FAILED_LOG_DIR="${WORK_ROOT}/orca_output/failed_logs/${METHOD}_${BASIS}"

mkdir -p "$ORCA_SCR" "./logs"

# 提取所有失败的分子 ID 并修复 XYZ 文件
failed_mols=()
echo "Fixing xyz files..."
for log in "${FAILED_LOG_DIR}"/*_failed.out; do
    if [ ! -f "$log" ]; then continue; fi
    filename=$(basename "$log")
    mol_id=$(echo "$filename" | grep -oE 'dsgdb9nsd_[0-9]+' | cut -d_ -f2 | sed 's/^0*//')
    
    if [ -n "$mol_id" ]; then
        xyz_file="${NEW_XYZ_ROOT}/dsgdb9nsd_$(printf "%06d" $mol_id).xyz"
        if [ -f "$xyz_file" ]; then
            # 替换 *^- 为 e- 并且 *^+ 为 e+ 以及 *^ 为 e
            sed -i 's/\*\^-/e-/g; s/\*\^+/e+/g; s/\*\^/e/g' "$xyz_file"
            failed_mols+=("$mol_id")
        fi
    fi
done

echo "Found ${#failed_mols[@]} failed jobs."

if [ ${#failed_mols[@]} -eq 0 ]; then
    echo "No failed jobs to retry."
    exit 0
fi

export NPROCS=8
export CONCURRENCY=24

WORKER_SCRIPT="${ORCA_SCR}/worker.sh"
cat << 'IN_BASH' > "$WORKER_SCRIPT"
#!/bin/bash

mol_id=$1
mol_id_padded=$(printf "%06d" $mol_id)
job_base="dsgdb9nsd_${mol_id_padded}_${METHOD}_${BASIS}"
inp_file="${job_base}.inp"

chunk_idx=$(( (mol_id - 1) / 16000 ))
sub_start=$(( chunk_idx * 16000 + 1 ))
sub_end=$(( sub_start + 16000 - 1 ))
mol_sub="${sub_start}_${sub_end}"

# 清理可能存在的错误的 out 文件
#rm -f "${FINAL_OUT_BASE}/${mol_sub}/${job_base}.out"
rm -f "${FAILED_LOG_DIR}/${job_base}_failed.out"

src_path="${SOURCE_ROOT}/${METHOD}/${BASIS}_${METHOD}/${mol_sub}/${inp_file}"
if [ ! -f "$src_path" ]; then
    echo "Source missing: $src_path"
    exit 0
fi

slot_dir="${ORCA_SCR}/slot_${mol_id}"
mkdir -p "$slot_dir"
cd "$slot_dir"

awk -v np="${NPROCS}" -v xyz="${NEW_XYZ_ROOT}" '
BEGIN { print "%maxcore 3900\n%pal nprocs " np " end" }
tolower($0) ~ /%pal/ { next }
tolower($0) ~ /%maxcore/ { next }
tolower($0) ~ /^\* *xyzfile/ {
    n = split($0, a, " ")
    fname = a[n]
    sub(".*/", "", fname)
    chg = (n >= 4) ? a[n-2] : "0"
    mult = (n >= 4) ? a[n-1] : "1"
    print "* xyzfile", chg, mult, xyz "/" fname
    next
}
{ print }
' "${src_path}" > "${inp_file}"

${ORCA_HOME}/bin/orca "$inp_file" > "${job_base}.out"

if [ -f "${job_base}.gbw" ]; then
    ${ORCA_HOME}/bin/orca_2mkl "${job_base}" -mkl > /dev/null 2>&1
fi

if [ -f "${job_base}.loc" ]; then
    cp "${job_base}.loc" "${job_base}_loc.gbw"
    ${ORCA_HOME}/bin/orca_2mkl "${job_base}_loc" -mkl > /dev/null 2>&1
fi

if tail -n 20 "${job_base}.out" | grep -q "ORCA TERMINATED NORMALLY"; then
    if [ -f "${job_base}.mkl" ] && [ -f "${job_base}_loc.mkl" ]; then
        mkdir -p "${FINAL_OUT_BASE}/${mol_sub}"
        mkdir -p "${FINAL_MKL_BASE}/${mol_sub}"
        mkdir -p "${FINAL_LOCMKL_BASE}/${mol_sub}"
        
        cp "${job_base}.out" "${FINAL_OUT_BASE}/${mol_sub}/"
        cp "${job_base}.mkl" "${FINAL_MKL_BASE}/${mol_sub}/"
        cp "${job_base}_loc.mkl" "${FINAL_LOCMKL_BASE}/${mol_sub}/"
        echo "[$job_base] SUCCESS"
    else
        echo "[$job_base] FAILED (Missing MKL)"
        cp "${job_base}.out" "${FAILED_LOG_DIR}/${job_base}_failed.out"
    fi
else
    echo "[$job_base] FAILED (Not normally terminated)"
    cp "${job_base}.out" "${FAILED_LOG_DIR}/${job_base}_failed.out"
fi

cd "$ORCA_SCR"
rm -rf "$slot_dir"
IN_BASH
chmod +x "$WORKER_SCRIPT"

module purge
module load openmpi/gcc/4.1.6-gcc12.3

for var in $(env | awk -F= '{print $1}' | grep -E '^(PMI|SLURM)'); do unset $var; done

export ORCA_HOME="/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
export PATH="${ORCA_HOME}/bin:${PATH}"
export LD_LIBRARY_PATH="${ORCA_HOME}/lib:${LD_LIBRARY_PATH}"
export OMPI_MCA_btl="vader,self"
export OMPI_MCA_orte_precondition_transfers="1"
export OMPI_MCA_rmaps_base_oversubscribe="1"
export ORCA_SKIP_CPU_BIND="1"

echo "=== Processing failed chunks ==="
printf "%s\n" "${failed_mols[@]}" | xargs -n 1 -P $CONCURRENCY "$WORKER_SCRIPT"

rm -rf "/tmp/$USER/orca_jobs_${SLURM_JOB_ID}"
rm -f "$WORKER_SCRIPT"
echo "=== CHUNK DONE ==="
