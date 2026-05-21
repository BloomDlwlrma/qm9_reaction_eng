import pandas as pd
import os
import re
import numpy as np
from ase import Atoms
import xyz2mol
import argparse
import logging
from rdkit import Chem
from rdkit.Chem import Descriptors
import csv
import glob

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")


def _get_project_root() -> str:
    """Return qm9_reaction_eng directory path (parent of scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Create QM9 chem table from xyz + merge with energy logs."
    )
    parser.add_argument(
        "run_dir",
        help="Path to the run directory containing ene_total_withhf.log."
    )
    return parser.parse_args()


def _infer_basis_and_method_from_run_dir(run_dir: str):
    """Infer basis and method tokens from a run directory name.
    Example run folder name:
      ..._631g_osvccsd_...
    Returns
    (basis, method)
    """
    if not run_dir:
        return None, None
    name = os.path.basename(os.path.normpath(run_dir))
    tokens = [t.lower() for t in name.split('_') if t]

    basis = None
    method = None
    method_map = {
        "osvccsd": "OSVCCSD",
        "osvmp2": "OSVMP2",
        "ccsd": "OSVCCSD", 
        "mp2": "OSVMP2",   
    }
    basis_map = {
        "631g": "6-31G",
        "631gs": "6-31G*",
        "631g**": "6-31G**",
        "631+g**": "6-31+G**",
        "def2svp": "def2-SVP",
        "def2tzvp": "def2-TZVP",
        "def2qzvpp": "def2-QZVPP",
        "ccpvdz": "cc-pVDZ",
        "ccpvtz": "cc-pVTZ",
        "augccpvdz": "aug-cc-pVDZ",
        "321g": "3-21G"
    }

    for t in tokens:
        # Check method
        if t in method_map:
            method = method_map[t]
        if t in basis_map:
            basis = basis_map[t]
        elif re.fullmatch(r"\d{3,4}g\*?", t):
            basis = t 

    return basis, method


def _select_run_dir(test_root: str, target: str, explicit_run_dir: str = None):
    """Pick a run directory.

    - If explicit_run_dir is provided: use it.
    - Else: select the latest subfolder under test_root matching target:
        target='ccsd' -> contains 'osvccsd'
        target='mp2'  -> contains 'osvmp2'
    - If none found: fall back to test_root.
    """
    if explicit_run_dir:
        return explicit_run_dir

    want_token = "osvccsd" if target == "ccsd" else "osvmp2"
    try:
        candidates = []
        for child in os.listdir(test_root):
            full = os.path.join(test_root, child)
            if not os.path.isdir(full):
                continue
            if want_token in child:
                candidates.append(full)
        if candidates:
            # folder names begin with timestamps, so lexicographic sort works well
            return sorted(candidates)[-1]
    except Exception as e:
        logging.warning(f"Failed to scan test_root={test_root}: {e}")
    return test_root

def update_failed_indices(outputfile, indices):
    indices = sorted(list(set(indices))) 
    fail_count = len(indices)
    logging.info("="*50)
    logging.info(f"Number of failed indices (failed to read .log data) = {fail_count}")
    if not indices:
        logging.info("No failed indices, skip creating .out file.")
        logging.info("="*50)
        if os.path.exists(outputfile):
            os.remove(outputfile) 
        return

    # Ensure parent dir exists if a path is provided.
    try:
        parent = os.path.dirname(os.path.abspath(outputfile))
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(outputfile, 'w', encoding='utf-8') as fp:
            for index in indices:
                fp.write(f"{index}\n")
        logging.info(f"Failed indices have been written to: {outputfile}")
    except Exception as e:
        logging.error(f"Failed to write failed indices to {outputfile}: {str(e)}")
    logging.info("="*50)
    return

def get_inchi_from_xyz(ixyzfile):
    """Read an xyz file whose last line contains InChI text and return that InChI."""
    try:
        with open(ixyzfile, 'r') as fp:
            lines = [ln.strip() for ln in fp if ln.strip()]
        # Traverse from the end to find the first token starting with "InChI"
        for ln in reversed(lines):
            for token in re.split(r"\s+", ln):
                if token.startswith("InChI"):
                    if token.startswith("InChI="):
                        return token[len("InChI="):]
                    return token
        logging.warning(f"No InChI found in {ixyzfile}")
    except Exception as e:
        logging.warning(f"Failed to read InChI from {ixyzfile}: {e}")
    return None

def _strip_inchi_prefix_in_csv(csv_path):
    """In-place cleanup: if InChI values start with 'InChI=', strip the prefix.

    Returns True if the file was modified.
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logging.warning(f"Failed to read CSV for InChI cleanup: {csv_path}: {e}")
        return False

    if 'InChI' not in df.columns:
        return False

    s = df['InChI']
    mask = s.notna() & s.astype(str).str.startswith('InChI=')
    if not mask.any():
        return False

    df.loc[mask, 'InChI'] = s[mask].astype(str).str.replace(r'^InChI=', '', regex=True)
    try:
        df.to_csv(csv_path, index=False)
    except Exception as e:
        logging.error(f"Failed to write cleaned CSV: {csv_path}: {e}")
        return False
    logging.info(f"Stripped 'InChI=' prefix in existing CSV: {csv_path}")
    return True

def get_ene_csv_from_log(ene_dir, xyz_dir, failed_indices, target, energy_log):
    '''
    This function reads energy values from a log file and
    creates a CSV file with energy columns: 'index', 'qm9_index', '<method>_<basis>', 'Accurate_eng_{ccsd|mp2}', 'error'
    '''
    logging.info(f"Reading energy values from log file: {ene_dir}")

    basis, method = _infer_basis_and_method_from_run_dir(ene_dir)
    if not basis:
        basis = "unknown"
    if not method:
        method = "unknown"
    method_energy_col = f"{method}_{basis}"
    accurate_col = f"Accurate_eng_{target}"

    out_csv_dir = os.path.join(ene_dir, "csv")
    if not os.path.exists(out_csv_dir):
        os.makedirs(out_csv_dir)
        logging.info(f"Created directory: {out_csv_dir}")
    out_csv = os.path.join(out_csv_dir, "molecules_energy_values.csv")
    
    if os.path.isfile(out_csv):
        # If an old CSV exists without the new 'InChI' column, rebuild it.
        try:
            existing_cols = pd.read_csv(out_csv, nrows=0).columns.tolist()
        except Exception as e:
            logging.warning(f"Failed to read existing CSV header, will rebuild: {e}")
            existing_cols = []

        if 'InChI' in existing_cols:
            # If the column exists but values still have 'InChI=' prefix, clean in-place.
            _strip_inchi_prefix_in_csv(out_csv)
            logging.info(f"CSV file already exists with InChI: {out_csv}")
            return out_csv

        logging.info(f"Existing CSV missing InChI column; rebuilding: {out_csv}")
        try:
            os.remove(out_csv)
        except OSError as e:
            logging.error(f"Cannot remove old CSV {out_csv}: {e}")
            return None
    logfile = os.path.join(ene_dir, energy_log)
    if not os.path.exists(logfile):
        logging.error(f"Log file does not exist: {logfile}")
        return None

    with open(out_csv, 'w', newline='') as csvf:
        writer = None
        line_counter = 0
        test_counter = 0
        # InChI_counter = 0
        try:
            with open(logfile, 'r') as f:
                for line in f:
                    line = line.strip()
                    line_counter += 1
                    # if not line.startswith('test'):
                    #     continue
                    parts = line.split()
                    if len(parts) < 6:
                        logging.warning(f"Unexpected format in line: {line}")
                        continue
                    qm9_index = parts[2]
                    try:
                        index = int(qm9_index.split("_")[-1])
                    except ValueError:
                        logging.warning(f"Invalid qm9_index format: {qm9_index}")
                        continue
                    # try:
                    #     inchi = get_inchi_from_xyz(
                    #         os.path.join(xyz_dir, f"{qm9_index}.xyz")
                    #     )
                    #     if inchi:
                    #         InChI_counter += 1
                    # except Exception as e:
                    #     logging.error(f"Error getting InChI for {qm9_index}: {e}")
                    #     inchi = None
                    try:
                        method_energy = float(parts[4])
                        accurate_energy = float(parts[3])
                        error = float(parts[5])
                    except ValueError as e:
                        logging.warning(f"Invalid numeric value in line: {line}, error: {e}")
                        failed_indices.append(index)
                        continue
                    result_data = {
                        'index': index,
                        'qm9_index': qm9_index,
                        # 'InChI': inchi,
                        method_energy_col: method_energy,
                        accurate_col: accurate_energy,
                        'error': error
                    }
                    if writer is None:  # initialize writer and write header
                        writer = csv.DictWriter(csvf, fieldnames=result_data.keys())
                        writer.writeheader()
                    writer.writerow(result_data)
                    test_counter += 1
                    if test_counter % 10000 == 0:
                        logging.info(f"Processed {test_counter} entries.")
        except Exception as e:
            logging.error(f"Error reading log file {logfile}: {e}")
    logging.info(f"Energy CSV created at {out_csv}")
    logging.info(f"Total lines processed: {line_counter}, total test entries: {test_counter}")
    # logging.info(f"Total lines processed: {line_counter}, total test entries: {test_counter}, total InChI extracted: {InChI_counter}")
    return out_csv

def get_chem_df_from_xyz(xyz_dir: str = None, out_csv_dir: str = None) -> tuple[pd.DataFrame, list[int]]:
    """Create a Chem/index/qm9_index table from QM9 xyz files.

    It extracts the molecular formula (e.g., C4H10O) from the trailing InChI line
    in each xyz file. If InChI is missing/malformed, it falls back to counting
    element symbols in the atom block.

    Returns
    -------
    (df_Chem, failed_indices)
        df_Chem has columns: index, Chem, qm9_index, atom_composition, C_num/H_num/O_num/N_num/F_num.
        failed_indices contains indices where both InChI parsing and fallback failed.
    """

    def _inchi_to_formula(inchi):
        if not inchi:
            return None
        s = str(inchi).strip()
        if not s:
            return None
        if s.startswith("InChI="):
            s = s[len("InChI="):]

        # Expected: 1S/<formula>/<rest...>
        parts = s.split("/")
        if len(parts) < 2:
            return None

        # Some data may omit the leading version; handle both.
        if parts[0] in ("1S", "1"):
            formula = parts[1]
        else:
            formula = parts[0]

        formula = formula.strip()
        if not formula:
            return None

        # Basic sanity check: element+count tokens.
        if not re.fullmatch(r"([A-Z][a-z]?\d*)+", formula):
            return None
        return formula

    def _hill_formula_from_counts(counts) : # dict[str, int] -> str | None:
        if not counts:
            return None
        ordered: list[tuple[str, int]] = []
        if 'C' in counts:
            ordered.append(('C', counts['C']))
        if 'H' in counts:
            ordered.append(('H', counts['H']))
        if 'O' in counts:
            ordered.append(('O', counts['O']))
        if 'N' in counts:
            ordered.append(('N', counts['N']))
        if 'F' in counts:
            ordered.append(('F', counts['F']))
        for elem in sorted([e for e in counts.keys() if e not in {'C', 'H', 'O', 'N', 'F'}]):
            logging.debug(f"Non-standard element found in fallback formula: {elem}")
            ordered.append((elem, counts[elem]))
        out = "".join([f"{e}{n if n != 1 else ''}" for e, n in ordered if n > 0]) # Hill notation
        return out or None

    def _fallback_formula_from_xyz_lines(lines): # : list[str] -> str | None
        try:
            natoms = int(lines[0].strip()) 
        except Exception:
            return None
        if natoms <= 0:
            return None
        if len(lines) < 2 + natoms:
            return None
        counts: dict[str, int] = {}
        for ln in lines[2:2 + natoms]:
            ln = ln.strip()
            if not ln:
                continue
            parts = re.split(r"\s+", ln)
            if not parts:
                continue
            elem = parts[0]
            if not re.fullmatch(r"[A-Z][a-z]?", elem):
                continue
            counts[elem] = counts.get(elem, 0) + 1
        return _hill_formula_from_counts(counts)

    out_csv_path = os.path.join(out_csv_dir, "index_Chem_composition.csv")
    xyz_files = sorted(glob.glob(os.path.join(xyz_dir, "dsgdb9nsd_*.xyz")))
    if not xyz_files:
        raise FileNotFoundError(f"No xyz files found under: {xyz_dir}")

    rows = []
    failed_indices: list[int] = []
    for fpath in xyz_files:
        fname = os.path.basename(fpath)
        m = re.search(r"dsgdb9nsd_(\d{6})\.xyz$", fname)
        if not m:
            logging.warning(f"Skip unexpected xyz filename: {fname}")
            continue
        index = int(m.group(1))
        qm9_index = f"dsgdb9nsd_{index:06d}"

        try:
            with open(fpath, 'r', encoding='utf-8', errors='replace') as fp:
                raw_lines = [ln.rstrip("\n") for ln in fp]
        except Exception as e:
            logging.warning(f"Failed to read xyz file: {fpath}: {e}")
            failed_indices.append(index)
            continue

        # Try InChI token(s) from the end.
        inchi_tokens: list[str] = []
        for ln in reversed(raw_lines):
            if 'InChI' not in ln:
                continue
            inchi_tokens = re.findall(r"InChI=[^\s]+", ln)
            if inchi_tokens:
                break

        chem_formula = None
        if inchi_tokens:
            chem_formula = _inchi_to_formula(inchi_tokens[0])
        else:
            # fallback to the earlier helper that looks for "InChI" tokens without regex
            inchi = get_inchi_from_xyz(fpath)
            chem_formula = _inchi_to_formula(inchi)

        if not chem_formula:
            chem_formula = _fallback_formula_from_xyz_lines(raw_lines)

        if not chem_formula:
            failed_indices.append(index)
            continue

        try:
            atom_comp = parse_atom_composition(chem_formula)
        except Exception:
            atom_comp = {}
        if not atom_comp:
            failed_indices.append(index)
            continue

        rows.append({
            'index': index,
            'Chem': chem_formula,
            'qm9_index': qm9_index,
            'atom_composition': atom_comp,
        })

    df_Chem = pd.DataFrame(rows)
    if df_Chem.empty:
        raise RuntimeError("No Chem rows were extracted from xyz files.")

    df_Chem['index'] = df_Chem['index'].astype(int)
    df_Chem = df_Chem.sort_values('index').reset_index(drop=True)
    for elem in ['C', 'H', 'O', 'N', 'F']:
        df_Chem[f'{elem}_num'] = df_Chem['atom_composition'].apply(lambda x: x.get(elem, 0) if isinstance(x, dict) else 0)

    os.makedirs(out_csv_dir, exist_ok=True)
    if os.path.exists(out_csv_path):
        os.remove(out_csv_path)
    df_Chem.to_csv(out_csv_path, index=False)
    logging.info(f"Chem CSV extracted from xyz saved to: {out_csv_path} (rows={len(df_Chem)})")
    if failed_indices:
        logging.warning(f"Chem extraction failed for {len(set(failed_indices))} indices")
    return df_Chem, failed_indices

def parse_atom_composition(chem_formula):
    pattern = r'([A-Z][a-z]?)(\d*)'
    matches = re.findall(pattern, chem_formula)
    comp = {}
    for elem, num in matches:
        comp[elem] = int(num) if num else 1
    return comp

def clean_xyz_files(src_dir):
    with open(src_dir, 'r') as f:
        lines = f.readlines()
    if not lines:
        raise ValueError(f"Empty xyz file: {src_dir}")
    try:
        natoms = int(lines[0].strip())
    except Exception as e:
        raise ValueError(f"Invalid atom count in line 1: {lines[0]}")
    
    lines_need = natoms + 2
    if len(lines) < lines_need:
        raise ValueError(f"File has {len(lines)} lines but expected at least {lines_need} for {natoms} atoms.")
    else:
        out_lines = lines[:lines_need]
        if out_lines[1].strip() == "":
            pass
        else:   
            out_lines[1] = "\n"

        for i in range(2, len(out_lines)):
            l = out_lines[i]
            parts = l.strip().split()
            if len(parts) >= 4:
                new_line = f"{parts[0]:<2} {parts[1]:>15} {parts[2]:>15} {parts[3]:>15}\n"
                out_lines[i] = new_line
            elif len(parts) < 3:
                raise ValueError(f"Invalid atom line format: {l}")
            else:
                pass
    
    with open(src_dir, 'w') as f:
        f.writelines(out_lines)

def main():
    # ====== Parse Arguments ======
    '''
    usage: 01_create_mp2csv.py [-h] run_dir
    Create QM9 chem table from xyz + merge with energy logs.

    positional arguments:
        run_dir     Path to the run directory containing ene_total_withhf.log.

    optional arguments:
        -h, --help  show this help message and exit
    '''
    args = parse_arguments()
    # ====================================
    # ====== Initial DataFrame Load ======
    # ====================================
    xyz_dir = os.path.join(_get_project_root(), "src", "qm9_xyz_files")
    for f in os.listdir(xyz_dir):
        if f.endswith(".xyz"):
            try:
                clean_xyz_files(os.path.join(xyz_dir, f))
            except Exception as e:
                logging.warning(f"Failed to clean xyz file {f}: {e}")

    out_dir = os.path.join(_get_project_root(), "src", "csv")
    failed_out_dir = out_dir
    tdnn_source_dir = args.run_dir
    basis, target = _infer_basis_and_method_from_run_dir(tdnn_source_dir)
    filename = os.path.basename(os.path.normpath(tdnn_source_dir))
    
    method_energy_col = f"{target}_{basis}" # e.g. OSVCCSD_6-31G or clean version?
    
    accurate_col = f"Accurate_eng_{target}"
    
    failed_out_dir_name = f"failed_energy_extract_{basis}_{target}_{filename}.out"
    failed_out_path = os.path.join(failed_out_dir, failed_out_dir_name)

    df_Chem, failed_indices = get_chem_df_from_xyz(xyz_dir=xyz_dir, out_csv_dir=out_dir)
    # ==============================================
    # ======= Create energy csv from log file ======
    # ==============================================
    out_csv_path = get_ene_csv_from_log(tdnn_source_dir, xyz_dir, failed_indices, target=target, energy_log=os.path.join(tdnn_source_dir, "ene_total_withhf.log") )
    if not out_csv_path or not os.path.isfile(out_csv_path):
        logging.error("Failed to create energy CSV. Exiting.")
        update_failed_indices(failed_out_path, failed_indices)
        return

    df_ene = pd.read_csv(out_csv_path)

    if method_energy_col not in df_ene.columns:
        # Safety: if run_dir couldn't be parsed, fall back to the first non-standard energy column.
        candidate_cols = [c for c in df_ene.columns if c not in {'index', 'qm9_index', accurate_col, 'error'}]
        if candidate_cols:
            method_energy_col = candidate_cols[0]
            logging.warning(f"Using inferred method energy column from CSV: {method_energy_col}")
        else:
            logging.error("No method energy column found in energy CSV.")
            update_failed_indices(failed_out_path, failed_indices)
            return

    if accurate_col not in df_ene.columns:
        logging.error(f"Missing required accurate energy column in energy CSV: {accurate_col}")
        update_failed_indices(failed_out_path, failed_indices)
        return

    df_merged = pd.merge(
        df_Chem,
        df_ene[['qm9_index', method_energy_col, accurate_col, 'error']],
        on='qm9_index',
        how='left'
    )

    num_cols = [method_energy_col, accurate_col, 'error']
    df_merged[num_cols] = df_merged[num_cols].apply(pd.to_numeric, errors='coerce').astype(float)
    # df_merged['InChI'] = df_merged['InChI'].fillna('').astype(object)
    df_merged['index'] = df_merged['index'].astype(int)

    no_energy_mask = df_merged[num_cols].isna().any(axis=1)
    no_energy_indices = df_merged.loc[no_energy_mask, 'index'].tolist()
    failed_indices.extend(no_energy_indices)

    matched_count = df_merged[method_energy_col].notna().sum()
    total_count = len(df_merged)
    logging.info(f"Successfully matched {matched_count}/{total_count} molecules.")

    os.makedirs(out_dir, exist_ok=True)
    out_name = f"source_energies_{basis}_{target}_{filename}.csv"
    out_path = os.path.join(out_dir, out_name)
    df_merged.to_csv(out_path, index=False)
    logging.info(f"Merged data with energies saved to: {out_path}")

    # out_h5_name = f"qm9_bonds_energies_{basis}_{target}.h5"
    # out_h5_path = os.path.join(out_dir, out_h5_name)
    # df_merged.to_hdf(out_h5_path, key='data', mode='w')
    # logging.info(f"Merged data with energies saved to: {out_h5_path}")

    update_failed_indices(failed_out_path, failed_indices)

if __name__ == "__main__":
    main()


