#!/usr/bin/env python3
"""
Verify that the number of occupied orbitals in QM9 GDB-format .xyz files matches the corresponding HDF5 features.
"""
import os
import h5py
import numpy as np
import re
from ase import Atoms

def parse_gdb_xyz(gdb_file):
    with open(gdb_file, 'r') as f:
        lines = f.readlines()
    # First line: number of atoms
    try:
        num_atoms = int(lines[0].strip())
    except:
        num_atoms = int(re.split(r'\s+', lines[0].strip())[0])
    
    atom_data_start = 2
    atoms = []
    positions = []
    charges = []
    
    for i in range(atom_data_start, atom_data_start + num_atoms):
        line = lines[i].strip()
        parts = re.split(r'\s+', line)
        
        if len(parts) < 4:
            continue
        
        symbol = parts[0]
        # Replace *^ with e for scientific notation
        x = float(parts[1].replace('*^', 'e'))
        y = float(parts[2].replace('*^', 'e'))
        z = float(parts[3].replace('*^', 'e'))
        
        atoms.append(symbol)
        positions.append([x, y, z])
        
        if len(parts) >= 5:
            try:
                charge = float(parts[4].replace('*^', 'e'))
                charges.append(charge)
            except:
                charges.append(0.0)
    
    return atoms, np.array(positions), charges

def calculate_nocc_from_gdb(xyz_file):
    atoms, positions, _ = parse_gdb_xyz(xyz_file)
    
    valence_dict = {
        'H': 1, 
        'C': 4,  
        'N': 5,  
        'O': 6, 
        'F': 7   
    }
    
    total_valence = 0
    for atom in atoms:
        symbol = atom
        if symbol in valence_dict:
            total_valence += valence_dict[symbol]
        else:
            atomic_numbers = {'H': 1, 'C': 6, 'N': 7, 'O': 8, 'F': 9}
            if symbol in atomic_numbers:
                total_valence += atomic_numbers[symbol]
            else:
                raise ValueError(f"unknow_element: {symbol}，please update valence_dict accordingly.")
    
    # For closed-shell molecules
    expected_nocc = total_valence // 2
    return expected_nocc, atoms, positions

def verify_gdb_with_hdf5(hdf5_file, xyz_file, mol_name=None):
    if mol_name is None:
        mol_name = os.path.basename(xyz_file).replace('.xyz', '')
    
    print(f"Verifying molecule: {mol_name}")
    print("=" * 60)
    
    # 1. Calculate information from .xyz file
    if not os.path.exists(xyz_file):
        print(f"Error: File does not exist: {xyz_file}")
        return False
    
    try:
        expected_nocc, atoms, positions = calculate_nocc_from_gdb(xyz_file)
        num_atoms = len(atoms)
        
        # 计算化学式
        from collections import Counter
        counts = Counter(atoms)
        formula = ""
        for elem in ['C', 'H', 'N', 'O', 'F']:
            if elem in counts:
                count = counts[elem]
                formula += f"{elem}" if count == 1 else f"{elem}{count}"
        
        print(f".xyz file information:")
        print(f"  Formula: {formula}")
        print(f"  Number of atoms: {num_atoms}")
        print(f"  Atom composition: {dict(counts)}")
        print(f"  Expected number of occupied orbitals (nocc): {expected_nocc}")
        
    except Exception as e:
        print(f"Error parsing .xyz file: {e}")
        return False
    
    # 2. Check HDF5 file
    if not os.path.exists(hdf5_file):
        print(f"Error: HDF5 file does not exist: {hdf5_file}")
        return False
    
    with h5py.File(hdf5_file, 'r') as f:
        # Check if molecule exists
        in_diag = f"diag/{mol_name}" in f
        in_offdiag_close = f"offdiag_close/{mol_name}" in f
        in_offdiag_remote = f"offdiag_remote/{mol_name}" in f
        
        print(f"\nHDF5 file check:")
        print(f"  In diag group: {'Yes' if in_diag else 'No'}")
        print(f"  In offdiag_close group: {'Yes' if in_offdiag_close else 'No'}")
        print(f"  In offdiag_remote group: {'Yes' if in_offdiag_remote else 'No'}")
        
        if not in_diag:
            print("Error: Molecule not found in diag group")
            return False
        
        # Get actual feature dimensions
        diag_shape = f[f"diag/{mol_name}"].shape
        actual_nocc = diag_shape[0]
        
        print(f"\nActual feature dimensions:")
        print(f"  diag: {diag_shape} (nocc={actual_nocc})")
        
        if in_offdiag_close:
            offdiag_close_shape = f[f"offdiag_close/{mol_name}"].shape
            print(f"  offdiag_close: {offdiag_close_shape}")
        
        if in_offdiag_remote:
            offdiag_remote_shape = f[f"offdiag_remote/{mol_name}"].shape
            print(f"  offdiag_remote: {offdiag_remote_shape}")
        
        # 3. Verify nocc match
        if actual_nocc == expected_nocc:
            print(f"\n✅ Verification passed: nocc match (expected {expected_nocc} = actual {actual_nocc})")
            
            # Additional check: feature preview
            print(f"\nFeature preview (first 5 values of the first 3 diagonal features):")
            diag_data = f[f"diag/{mol_name}"][:]
            for i in range(min(3, diag_shape[0])):
                print(f"  Orbital {i}: {diag_data[i, :5].round(6)}...")
            
            # Calculate feature statistics
            mean_val = np.mean(diag_data)
            std_val = np.std(diag_data)
            print(f"\nDiagonal feature statistics:")
            print(f"  Mean: {mean_val:.6f}")
            print(f"  Standard deviation: {std_val:.6f}")
            
            return True
        else:
            print(f"\n❌ Verification failed: nocc mismatch (expected {expected_nocc} ≠ actual {actual_nocc})")
            
            # Possible explanations
            print(f"\nPossible reasons:")
            print(f"  1. .xyz file does not match HDF5 file")
            print(f"  2. Molecule is in an open-shell state (unpaired electrons)")
            print(f"  3. Valence electron count calculation error (molecule may have special electronic structure)")
            print(f"  4. Molecule name mismatch")
            
            # Attempt alternative valence electron count calculation
            valence_sum = 0
            for atom in atoms:
                if atom == 'H':
                    valence_sum += 1
                elif atom == 'C':
                    valence_sum += 4
                elif atom == 'N':
                    valence_sum += 5
                elif atom == 'O':
                    valence_sum += 6
                elif atom == 'F':
                    valence_sum += 7
            
            print(f"\nCalculation details:")
            print(f"  Total valence electrons: {valence_sum}")
            print(f"  Expected nocc (valence electrons/2): {valence_sum//2}")
            print(f"  Actual nocc (from HDF5): {actual_nocc}")
            
            return False

def batch_verify_gdb_hdf5(hdf5_file, xyz_dir, max_molecules=50):
    import glob

    xyz_files = sorted(glob.glob(os.path.join(xyz_dir, "*.xyz")))
    print(f"Found {len(xyz_files)} .xyz files")
    selected_files = [xyz_files[i] for i in range(9, len(xyz_files), 10)]
    print(f"Will check every 10th file: {len(selected_files)} files\n")

    results = []
    failed_logs = []

    with h5py.File(hdf5_file, 'r') as f:
        # Get all molecule names in HDF5
        hdf5_molecules = set()
        for group in ['diag', 'offdiag_close', 'offdiag_remote']:
            if group in f:
                hdf5_molecules.update(f[group].keys())

    for idx, xyz_file in enumerate(selected_files):
        mol_name = os.path.basename(xyz_file).replace('.xyz', '')

        if mol_name not in hdf5_molecules:
            msg = f"[{idx+1}] {mol_name}: ❌ Not in HDF5"
            print(msg)
            results.append((mol_name, False, "missing"))
            failed_logs.append(msg)
            continue

        try:
            # Capture output of verify_gdb_with_hdf5
            from io import StringIO
            import sys

            old_stdout = sys.stdout
            sys.stdout = mystdout = StringIO()
            success = verify_gdb_with_hdf5(hdf5_file, xyz_file, mol_name)
            output = mystdout.getvalue()
            sys.stdout = old_stdout

            results.append((mol_name, success, ""))

            if success:
                pass
            else:
                # Only output detailed information for failures
                print(f"[{idx+1}] {mol_name}: ❌ Failed\n{output}")
                failed_logs.append(f"[{idx+1}] {mol_name}: ❌ Failed\n{output}")

        except Exception as e:
            msg = f"[{idx+1}] {mol_name}: ⚠ Error: {e}\n"
            print(msg)
            results.append((mol_name, False, str(e)))
            failed_logs.append(msg)

    # Summarize results
    total = len(results)
    passed = sum(1 for r in results if r[1])

    print("=" * 60)
    print("Verification Summary:")
    print("=" * 60)
    print(f"Total verifications: {total}")
    print(f"Passed: {passed} ({passed/total*100:.1f}%)")
    print(f"Failed: {total - passed}")
    # Save failed logs
    if failed_logs:
        with open("check_nocc_hdf5_xyz.log", "w") as f:
            for line in failed_logs:
                f.write(line)
                if not line.endswith('\n'):
                    f.write('\n')
        print(f"\nAll failure information has been saved to check_nocc_hdf5_xyz.log")

    return results

if __name__ == "__main__":
    hdf5_file = "/home/ubuntu/Shiwei/tdnn/descriptors/dsgdb9nsd/mp2int_boys_8_locj_lock_locf_fenemat_wpnv2_ccpvtz/descriptors.hdf5" 
    xyz_dir = "/home/ubuntu/Shiwei/qm9_reaction_eng/qm9_xyz_files" 
    
    batch_verify_gdb_hdf5(hdf5_file, xyz_dir)