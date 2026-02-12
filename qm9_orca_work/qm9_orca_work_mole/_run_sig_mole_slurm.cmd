#!/bin/bash
#####################################################################################
###                                                                                 #
### slurm-orca.cmd :                                                                #
### A SLURM submission script for parallel ORCA program in HPC2021 system           #
###                                                                                 #
### Usage:                                                                          #
###    cd to directory containing ORCA input file(e.g. hf-water.inp), then:         #
###    sbatch -J hf-water <location of this script>/slurm-orca.cmd                  #
###                                                                                 #
### - Written by Lilian Chan, HKU ITS (2021-7-2)                                    #
###                                                                                 #
#####################################################################################

#SBATCH --job-name=orca-mpi                       # Job name
##SBATCH --mail-type=END,FAIL                      # Mail events
##SBATCH --mail-user=abc@email                     # Update your email address
#SBATCH --time=30:00                              # Wall time limit (days-hrs:min:sec)
#SBATCH --partition=intel                         # Specify partition (intel/amd)
#SBATCH --nodes=1                                 # Number of Compute node(s)
#SBATCH --ntasks=32                               # Number of CPU cores for ORCA
#SBATCH --ntasks-per-node=32                      # Number of Cores to be used per node
#SBATCH --output=%x.out.%j                        # Standard output file
#SBATCH --error=%x.err.%j                         # Standard error file

#######################################################################################
##  Content between 'X' lines will be executed in the first allocated node.           #
##  Please don't modify it                                                            #
#######################################################################################

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
if [ ! -z "$SLURM_SUBMIT_DIR" ]; then
    cd ${SLURM_SUBMIT_DIR}
fi
NPROCS=${SLURM_NTASKS}
export SCRATCH=${WORK}/orca_scr
mkdir -p ${SCRATCH}
WORK_DIR="/scr/u/u3651388/orcarun/qm9_orca_work_mole/orca_files"
JOB_NAME="mywater"
#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

# Select ORCA Version 
export ORCA_HOME=/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64
export PATH=${ORCA_HOME}/bin:$PATH
export LD_LIBRARY_PATH=${ORCA_HOME}/lib:$LD_LIBRARY_PATH
echo ===========  Work Directory ${WORK_DIR} - JOB ${SLURM_JOBID} : ${NPROCS} CPUS  ==============

#################################################################################
# Main Function
#################################################################################
INFILE="${WORK_DIR}/${JOB_NAME}.inp"
OUTFILE="${WORK_DIR}/${JOB_NAME}.${SLURM_JOBID}.out"
echo "%pal nprocs ${NPROCS} end" >  ${SCRATCH}/${SLURM_JOBID}.inp
cat  ${INFILE}  >> ${SCRATCH}/${SLURM_JOBID}.inp
echo " Display first 10 line of input file..."
head -10  ${SCRATCH}/${SLURM_JOBID}.inp

# Run ORCA job 
time ${ORCA_HOME}/bin/orca ${SCRATCH}/${SLURM_JOBID}.inp > ${OUTFILE}
mv ${SCRATCH}/${SLURM_JOBID}.gbw ${WORK_DIR}/${JOB_NAME}.${SLURM_JOBID}.gbw
${ORCA_HOME}/bin/orca_2mkl  ${WORK_DIR}/${JOB_NAME}.${SLURM_JOBID} -molden

echo "Job Finish Time is `date "+%Y/%m/%d -- %H:%M:%S"`"

## Uncomment below line to delete all scratch files generated
mv "${SCRATCH}/${SLURM_JOBID}"* "${WORK_DIR}/" 2>/dev/null
rm -f ${SCRATCH}/${SLURM_JOBID}*

exit 0

