#!/bin/bash
set -euo pipefail

ATOMS=("C" "H" "O" "N" "F")
METHODS=("MP2" "CCSD" "CCSD(T)")
BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "321g")

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
slurm_scripts_dir="${script_dir}/slurm_scripts"
mkdir -p "${slurm_scripts_dir}"

echo "Generating SLURM job scripts for all atom calculations..."

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}"
    
    for basis_key in "${BASIS_SETS[@]}"; do
        for method in "${METHODS[@]}"; do
            # Create filename-safe method name
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"
            
            jobname="${atom_lc}_${method_lc}_${basis_key}"
            infile="${script_dir}/${atom_lc}/${jobname}.inp"
            slurm_script="${slurm_scripts_dir}/${jobname}.sh"
            
            if [[ ! -f "${infile}" ]]; then
                echo "WARNING: Input file not found, skipping: ${infile}"
                continue
            fi
            
            # Generate SLURM script for this job
            cat > "${slurm_script}" << 'SLURM_EOF'
#!/bin/bash
# parallel job
#SBATCH -J orca
#SBATCH --job-name=JOBNAME_PLACEHOLDER
#SBATCH --error=slurm%J.%x.stderr
#SBATCH --output=slurm%J.%x.stdout
#SBATCH -N 1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH -t unlimited
#SBATCH --mail-type=begin
#SBATCH --mail-type=end

# setup temporary directory (pick a writable location)
choose_tmp_base() {
    for d in "${SLURM_TMPDIR:-}" "/tmp" "$HOME/scratch"; do
        [[ -n "$d" ]] || continue
        mkdir -p "$d" 2>/dev/null || continue
        [[ -w "$d" ]] || continue
        echo "$d"
        return 0
    done
    echo "$PWD"
}

scratch_root="$(choose_tmp_base)"
export TMPDIR="$scratch_root/orcajob_${SLURM_JOB_ID}"
mkdir -p "$TMPDIR" || { echo "ERROR: cannot create TMPDIR=$TMPDIR" >&2; exit 2; }

# OpenMPI sometimes needs a writable session dir
mkdir -p "$TMPDIR/ompi" 2>/dev/null || true
export OMPI_MCA_orte_tmpdir_base="$TMPDIR/ompi"

# Load OpenMPI
export LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH
export PATH="/share/openmpi_4.1.6/bin:$PATH"
export LD_LIBRARY_PATH="/share/openmpi_4.1.6/lib:$LD_LIBRARY_PATH"

# Load ORCA
export PATH=/share/bin/orca_6_0_0:$PATH
export LD_LIBRARY_PATH=/share/bin/orca_6_0_0:$LD_LIBRARY_PATH
export orcadir=/share/bin/orca_6_0_0

export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

# Testing location
echo "SLURM_JOB_ID=$SLURM_JOB_ID"
echo "SLURM_NODELIST=$SLURM_NODELIST"
echo "TMPDIR=$TMPDIR"
echo "PATH=$PATH"
echo "LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
echo "OMP_NUM_THREADS=$OMP_NUM_THREADS"

# Job-specific settings
jobname="JOBNAME_PLACEHOLDER"
input_dir="INPUTDIR_PLACEHOLDER"
output_dir="OUTPUTDIR_PLACEHOLDER"

# Debug: Check if input file exists
echo "Looking for input file: ${input_dir}/${jobname}.inp"
if [[ ! -f "${input_dir}/${jobname}.inp" ]]; then
    echo "ERROR: Input file not found: ${input_dir}/${jobname}.inp" >&2
    ls -la "${input_dir}/" >&2 || echo "Cannot list directory: ${input_dir}" >&2
    exit 1
fi

# Copy necessary input files
cp "${input_dir}/${jobname}.inp" "$TMPDIR/" || { echo "ERROR: cannot copy input file" >&2; exit 2; }
for f in "${input_dir}"/*.gbw; do [[ -e "$f" ]] || continue; cp "$f" "$TMPDIR"; done
for f in "${input_dir}"/*.xyz; do [[ -e "$f" ]] || continue; cp "$f" "$TMPDIR"; done
for f in "${input_dir}"/*.hess; do [[ -e "$f" ]] || continue; cp "$f" "$TMPDIR"; done
for f in "${input_dir}"/*.pc; do [[ -e "$f" ]] || continue; cp "$f" "$TMPDIR"; done

cd "$TMPDIR" || { echo "ERROR: cannot cd to TMPDIR=$TMPDIR" >&2; exit 2; }

# ORCA invoked here
orca_rc=0
echo "SLURM_CPUS_PER_TASK=$SLURM_CPUS_PER_TASK"
echo "Running: $orcadir/orca ${jobname}.inp"
"$orcadir/orca" "$jobname.inp" > "$jobname.out" || orca_rc=$?

# ORCA finished here
# Create output directory if needed
mkdir -p "$output_dir" || true

# Copy results back
cp "$TMPDIR/$jobname.out" "$output_dir/" 2>/dev/null || true
for f in "$TMPDIR"/*.gbw; do [[ -e "$f" ]] || continue; cp "$f" "$output_dir/"; done
for f in "$TMPDIR"/*.engrad; do [[ -e "$f" ]] || continue; cp "$f" "$output_dir/"; done
for f in "$TMPDIR"/*.xyz; do [[ -e "$f" ]] || continue; cp "$f" "$output_dir/"; done
for f in "$TMPDIR"/*.hess; do [[ -e "$f" ]] || continue; cp "$f" "$output_dir/"; done
for f in "$TMPDIR"/*.loc; do [[ -e "$f" ]] || continue; cp "$f" "$output_dir/"; done
for f in "$TMPDIR"/*.cis; do [[ -e "$f" ]] || continue; cp "$f" "$output_dir/"; done

echo "Job completed with exit code: $orca_rc"
exit $orca_rc
SLURM_EOF

            # Replace placeholders
            sed -i "s|JOBNAME_PLACEHOLDER|${jobname}|g" "${slurm_script}"
            sed -i "s|INPUTDIR_PLACEHOLDER|${script_dir}/${atom_lc}|g" "${slurm_script}"
            sed -i "s|OUTPUTDIR_PLACEHOLDER|${script_dir}/orca_outputs|g" "${slurm_script}"
            
            echo "Generated: ${slurm_script}"
        done
    done
done

echo -e "\nAll SLURM scripts generated in: ${slurm_scripts_dir}"
echo -e "\nTo submit all jobs, run:"
echo "  for script in ${slurm_scripts_dir}/*.sh; do sbatch \"\$script\"; done"
echo -e "\nOr submit specific jobs individually:"
echo "  sbatch ${slurm_scripts_dir}/c_mp2_631g.sh"
