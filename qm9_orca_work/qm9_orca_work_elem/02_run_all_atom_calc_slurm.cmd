#!/bin/bash
#################################################################################
## SLURM Submission Script for All Atom Calculations                            #
#################################################################################
#SBATCH --job-name=orca-rundir                    # Job name
#SBATCH --mail-type=END,FAIL                      # Mail events
#SBATCH --mail-user=u3651388@domain.com         # Update your email address
#SBATCH --time=24:00:00                           # Wall time limit (days-hrs:min:sec)
#SBATCH --partition=intel                         # Specifiy Partition (intel/amd)
#SBATCH --nodes=1                                 # Number of compute node
#SBATCH --ntasks=32                              # CPUs used for ORCA
#SBATCH --ntasks-per-node=32                      # CPUs used per node
#SBATCH --output=%x.out.%j                        # Standard output file
#SBATCH --error=%x.err.%j                         # Standard error file

#################################################################################
# Config & Arrays
#################################################################################

#ATOMS=("C" "H" "O" "N" "F")
#METHODS=("MP2" "CCSD" "CCSD(T)")
#BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "321g")

# Testing to debugging
ATOMS=("H")
METHODS=("CCSD" "CCSD(T)")
BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "321g")

#################################################################################
# Setup Environment
#################################################################################
SCRIPT_NAME="all_atom_calc"
function clean_up()
{
  # Clean up only if processing is not finished
  if [ ${FINISHED} -eq 0 ]; then
    echo "$SCRIPT_NAME terminated when processing $INFILE"
    rm -vf ${INFILE_PREFIX}.[^i][^n][^p]
    exit 255
  else
    exit 0
  fi 
}

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
cd ${SLURM_SUBMIT_DIR}
#NPROCS=${SLURM_NTASKS}
NPROCS=8
#SCRATCH=${WORK}/ORCA_SCR
WORK_DIR=$(pwd)
SCRATCH=$WORK_DIR/ORCA_SCR
mkdir -p ${SCRATCH}
echo "Job Start  Time is `date "+%Y/%m/%d -- %H:%M:%S"`"
#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# Select ORCA Version 
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=${ORCA_HOME}/bin:$PATH
export LD_LIBRARY_PATH=${ORCA_HOME}/lib:$LD_LIBRARY_PATH
echo ===========  Work Directory ${WORK_DIR} - JOB ${SLURM_JOBID} : ${NPROCS} CPUS  ==============

#################################################################################
# Main Loop
#################################################################################
echo "Work Directory: ${WORK_DIR}"

# Not finished (yet)
FINISHED="0"
# Shell signal handling
trap clean_up SIGHUP SIGINT SIGTERM SIGABRT

for atom in "${ATOMS[@]}"; do
    atom_lc="${atom,,}" # Convert to lowercase
    
    for basis_key in "${BASIS_SETS[@]}"; do
        for method in "${METHODS[@]}"; do
            
            method_file="${method//(/}"
            method_file="${method_file//)/}"
            method_lc="${method_file,,}"
            
            JOBNAME="${atom_lc}_${method_lc}_${basis_key}"
            ATOM_DIR="${WORK_DIR}/${atom_lc}"
            INFILE="${ATOM_DIR}/${JOBNAME}.inp"
            OUTFILE="${ATOM_DIR}/${JOBNAME}.out"
            
            if [[ ! -f "${INFILE}" ]]; then
                echo "WARNING: Input file missing: ${INFILE} Skipping." 
                continue
            fi
            
            # Check if already done
            if [[ -e "${OUTFILE}" ]]; then
                echo "SKIPPING: ${JOBNAME} (Output exists)"
                continue
            fi

            echo ""
            echo "----------------------------------------------------------------"
            echo "Processing (In-Place): ${JOBNAME}, time: $(date)"
            
            # Run In-Place (Simplified)
            # Use a subshell to change dir safely
            (
                SCRATCH_INP="${SCRATCH}/${JOBNAME}.inp"
                
                #echo "%pal nprocs ${NPROCS} end" >  "${SCRATCH_INP}"
                cat  "${INFILE}"  >> "${SCRATCH_INP}"
                
                head -1  "${SCRATCH_INP}"

                time ${ORCA_HOME}/bin/orca "${SCRATCH_INP}" > "${SCRATCH}/${JOBNAME}.out"
                
                echo "Moving results from ${SCRATCH} to ${ATOM_DIR} ..."
                mv "${SCRATCH}/${JOBNAME}.out" "${ATOM_DIR}/"
                
                # Move all other generated files (gbw, hess, xyz, etc.)
                mv "${SCRATCH}/${JOBNAME}"* "${ATOM_DIR}/" 2>/dev/null || true
                rm -f "${SCRATCH_INP}"
            )
            
            echo "Finished: ${JOBNAME} at $(date)"
            echo "----------------------------------------------------------------"

        done
    done

done

FINISHED="1"
echo "All Done. Job Finish Time: $(date)"
exit 0
