import os
import sys
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

# Configuration
WORK_DIR = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/"
#SRC_DIR = "/home/ubuntu/Shiwei/qm9_reaction_eng/qm9_xyz_files"
# Read SRC_DIR from txt file
txt_file = "/scr/u/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files/dsgdb9nsd/dsgdb9nsd.txt"
with open(txt_file, 'r') as f:
    SRC_DIR = f.read().strip()
BASIS_SETS=("631g" "631gs" "631gss" "631+gss" "def2svp" "def2tzvp" "ccpvdz" "ccpvtz" "aug-ccpvtz" "321g")

def main():
    # Paths
    xyz_dest_dir = os.path.join(WORK_DIR, "xyz_files")
    source_files_dir = os.path.join(WORK_DIR, "source_files")
    
    os.makedirs(xyz_dest_dir, exist_ok=True)
    os.makedirs(source_files_dir, exist_ok=True)
    
    logging.info(f"Target basis sets: {BASIS_SETS}")

    if not os.path.exists(SRC_DIR):
        logging.error(f"Source directory not found: {SRC_DIR}")
        sys.exit(1)

    src_files = [f for f in os.listdir(SRC_DIR) if f.endswith('.xyz')]
    logging.info(f"Found {len(src_files)} XYZ files to process.")
    
    for f in src_files:
        src_f = os.path.join(SRC_DIR, f)
        dest_f = os.path.join(xyz_dest_dir, f)
        mol_name = os.path.splitext(f)[0]
            
        # 2. Create Directory Structure in source_files
        # Create separate folders for each basis set: mol_name/mol_name_basis
        mol_dir = os.path.join(source_files_dir, mol_name)
        
        for basis in BASIS_SETS:
            folder_name = f"{mol_name}_{basis}"
            basis_dir = os.path.join(mol_dir, folder_name)
            os.makedirs(basis_dir, exist_ok=True)
                
    logging.info("Preprocessing complete.")

if __name__ == "__main__":
    main()
