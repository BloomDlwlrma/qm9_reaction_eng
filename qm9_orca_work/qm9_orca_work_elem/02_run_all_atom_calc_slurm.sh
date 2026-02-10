#!/bin/bash
#################################################################################
## SLURM Submission Script for All Atom Calculations                            #
#################################################################################
#SBATCH --job-name=orca-atoms                     # Job name
#SBATCH --partition=intel                         # Partition
#SBATCH --nodes=1                                 # Number of nodes
#SBATCH --ntasks=32                               # Total number of cores
#SBATCH --time=30:00:00                           # Walltime (30 hours)
#SBATCH --output=%x.out.%j                        # Standard output
#SBATCH --error=%x.err.%j                         # Standard error

#################################################################################
# Config & Arrays
#################################################################################
#ATOMS=("C" "H" "O" "N" "F")
ATOMS=("C")
METHODS=("MP2" "CCSD" "CCSD(T)")
BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "321g")

# Define Scratch Root (from your original script)
SCRATCH_ROOT="/lustre1/g/chem_yangjun/u3651388/qm9_elem_data/tmp"
SCRIPT_NAME="all_atom_calc"

#################################################################################
# Setup Environment
#################################################################################
# Load ORCA variables
# Using paths from your original script
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=${ORCA_HOME}/bin:$PATH
export LD_LIBRARY_PATH=${ORCA_HOME}/lib:$LD_LIBRARY_PATH

# OpenMPI Configuration (Crucial for performance and stability)
export OMP_NUM_THREADS=1                    # Ensure 1 MPI process per core, avoid thread oversubscription
mkdir -p "${SCRATCH_ROOT}/ompi_${SLURM_JOB_ID}"
export OMPI_MCA_orte_tmpdir_base="${SCRATCH_ROOT}/ompi_${SLURM_JOB_ID}" # Avoid filling /tmp

NPROCS=${SLURM_NTASKS}
WORK_DIR=$(pwd)

# Define clean_up function for trap
function clean_up() {
  if [ "${FINISHED}" != "1" ]; then
    echo "Job terminated early. Cleaning up..."
    # Warning: In a loop script, cleaning up might mean deleting the currently running scratch folder
    if [ -n "${SCRATCH_DIR}" ] && [ -d "${SCRATCH_DIR}" ]; then
        echo "Removing scratch dir: ${SCRATCH_DIR}"
        rm -rf "${SCRATCH_DIR}"
    fi
    exit 255
  fi
}

trap clean_up SIGHUP SIGINT SIGTERM SIGABRT

#################################################################################
# Main Loop
#################################################################################
echo "Job Start Time: $(date)"
echo "Running on node: $(hostname)"
echo "Scratch Directory Root: ${SCRATCH_ROOT}"

FINISHED="0"

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}" # Convert to lowercase
    
    for basis_key in "${BASIS_SETS[@]}"; do
        for method in "${METHODS[@]}"; do
            
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"
            
            JOBNAME="${atom_lc}_${method_lc}_${basis_key}"
            INFILE="${WORK_DIR}/${atom_lc}/${JOBNAME}.inp"
            OUTFILE="${WORK_DIR}/${atom_lc}/${JOBNAME}.out"
            
            if [[ ! -f "${INFILE}" ]]; then
                # echo "WARNING: Input file missing: ${INFILE}. Skipping." 
                # (Commented out to reduce log noise if many file combination don't exist)
                continue
            fi
            
            # # Check if already done (RunDir style skip)
            # if [[ -e "${OUTFILE}" ]]; then
            #     echo "SKIPPING: ${JOBNAME} (Output exists)"
            #     continue
            # fi

            echo "----------------------------------------------------------------"
            echo "Processing: ${JOBNAME}"
            
            SCRATCH_DIR="${SCRATCH_ROOT}/${JOBNAME}_${SLURM_JOBID}"
            mkdir -p "${SCRATCH_DIR}"
            
            # Construct Input in Scratch (Add %pal header)
            echo "%pal nprocs ${NPROCS} end" > "${SCRATCH_DIR}/${JOBNAME}.inp"
            cat "${INFILE}" >> "${SCRATCH_DIR}/${JOBNAME}.inp"
            
            # Copy auxiliary files if they exist (gbw, xyz, etc)
            cp "${WORK_DIR}/${atom_lc}/"*.gbw "${SCRATCH_DIR}/" 2>/dev/null || true
            cp "${WORK_DIR}/${atom_lc}/"*.xyz "${SCRATCH_DIR}/" 2>/dev/null || true
            
            # Run ORCA
            (
                cd "${SCRATCH_DIR}"
                echo "Display first 10 lines of input file..."
                head -10 "${JOBNAME}.inp"
                
                echo "Running ORCA..."
                # Use time to measure duration
                time ${ORCA_HOME}/bin/orca "${JOBNAME}.inp" > "${JOBNAME}.out"
            )
            
            mv "${SCRATCH_DIR}/${JOBNAME}.out" "${WORK_DIR}/${atom_lc}/"
            if [ -f "${SCRATCH_DIR}/${JOBNAME}.gbw" ]; then
                mv "${SCRATCH_DIR}/${JOBNAME}.gbw" "${WORK_DIR}/${atom_lc}/"
            fi
            
            rm -rf "${SCRATCH_DIR}"
            
            echo "Finished: ${JOBNAME} at $(date)"
            echo "----------------------------------------------------------------"

        done
    done
done

FINISHED="1"
echo "All Done. Job Finish Time: $(date)"
exit 0
