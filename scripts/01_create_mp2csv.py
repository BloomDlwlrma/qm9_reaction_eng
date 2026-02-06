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
    project_root = _get_project_root()
    default_test_root = os.path.join(project_root, "test")
    default_xyz_dir = os.path.join(project_root, "qm9_xyz_files")
    default_out_dir = os.path.join(project_root, "csv")

    parser = argparse.ArgumentParser(
        description=(
            "Create QM9 chem table from xyz + merge with energy logs. "
            "Energy run directory is inferred from test/ unless --run_dir is provided."
        )
    )
    parser.add_argument(
        "-t", "--target",
        choices=["ccsd", "mp2"],
        default="ccsd",
        help="Output flavor: writes qm9_bonds_energies_{ccsd|mp2}.csv and selects a matching run_dir automatically.",
    )
    parser.add_argument(
        "-b", "--basis",
        choices=[
            "def2-SVP", "def2-TZVP", "def2-QZVPP", "6-31G*", "cc-pVDZ", 
            "cc-pVTZ", "aug-cc-pVDZ", "6-31G", "3-21G", "6-31G**", "6-31+G**"
        ],
        default="cc-pVTZ",
        help="Basis set used for calculation."
    )
    parser.add_argument(
        "--run_dir",
        default=None,
        help=(
            "Path to a specific run directory under test/ (must contain ene_total_withhf.log). "
            "If omitted, the latest matching run folder under test/ is used."
        ),
    )
    parser.add_argument(
        "-x", "--xyz_dir",
        default=default_xyz_dir,
        help="Directory containing dsgdb9nsd_*.xyz files.",
    )
    parser.add_argument(
        "-o", "--out_dir",
        default=default_out_dir,
        help="Directory to write final merged qm9_bonds_energies_*.csv.",
    )

    # Legacy / power-user knobs (hidden to keep CLI clean)
    parser.add_argument("--test_root", default=default_test_root, help=argparse.SUPPRESS)
    parser.add_argument("--energy_log", default="ene_total_withhf.log", help=argparse.SUPPRESS)

    return parser.parse_args()


def _infer_basis_and_method_from_run_dir(run_dir: str):
    """Infer basis and method tokens from a run directory name.

    Example run folder name:
      20251222215328_..._631g_osvccsd_...

    Returns
    -------
    (basis, method)
      basis like '631g' (or None), method like 'osvccsd'/'osvmp2' (or None)
    """
    if not run_dir:
        return None, None
    name = os.path.basename(os.path.normpath(run_dir))
    tokens = [t for t in name.split('_') if t]

    basis = None
    method = None
    for t in tokens:
        if re.fullmatch(r"\d{3,4}g", t):
            basis = t
    for t in tokens:
        if t in {"osvccsd", "osvmp2"}:
            method = t
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

def _strip_inchi_prefix_in_csv(csv_path: str) -> bool:
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

def get_ene_csv_from_log(
    ene_dir,
    xyz_dir,
    failed_indices,
    target: str = "ccsd",
    energy_log: str = "ene_total_withhf.log",
):
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
                    if not line.startswith('test'):
                        continue
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

def main():
    # ====== Parse Arguments ======
    '''
    Docstring for main
    usage: 01_create_mp2csv.py [-h] [--target {ccsd,mp2}] [--run_dir RUN_DIR] [--test_root TEST_ROOT] [--xyz_dir XYZ_DIR] [--out_dir OUT_DIR] [--energy_log ENERGY_LOG]

    Create QM9 chem table from xyz + merge with energy logs. Energy run directory is inferred from qm9_reaction_eng/test/ unless --run_dir is provided.

    optional arguments:
    -h, --help            show this help message and exit
    --target {ccsd,mp2}   Output flavor: writes qm9_bonds_energies_{ccsd|mp2}.csv and selects a matching run_dir automatically.
    --run_dir RUN_DIR     Path to a specific run directory under test/ (must contain ene_total_withhf.log). If omitted, the latest matching run folder under --test_root is used.
    --test_root TEST_ROOT
                            Root directory that contains run folders and/or ene_total_withhf.log.
    --xyz_dir XYZ_DIR     Directory containing dsgdb9nsd_*.xyz files.
    --out_dir OUT_DIR     Directory to write final merged qm9_bonds_energies_*.csv.
    --energy_log ENERGY_LOG
                            Energy log filename inside run_dir.
    
    example usage:
        python /home/ubuntu/Shiwei/qm9_reaction_eng/scripts/01_create_mp2csv.py --target ccsd --test_root /home/ubuntu/Shiwei/qm9_reaction_eng/test --xyz_dir /home/ubuntu/Shiwei/qm9_reaction_eng/qm9_xyz_files --out_dir /home/ubuntu/Shiwei/qm9_reaction_eng/csv
        python /home/ubuntu/Shiwei/qm9_reaction_eng/scripts/01_create_mp2csv.py --target mp2 --test_root /home/ubuntu/Shiwei/qm9_reaction_eng/test --xyz_dir /home/ubuntu/Shiwei/qm9_reaction_eng/qm9_xyz_files --out_dir /home/ubuntu/Shiwei/qm9_reaction_eng/csv
    '''
    args = parse_arguments()

    # ====== Initial DataFrame Load ======
    xyz_dir = args.xyz_dir
    out_dir = args.out_dir
    failed_out_dir = out_dir
    test_root = args.test_root
    run_dir = _select_run_dir(test_root=test_root, target=args.target, explicit_run_dir=args.run_dir)

    basis, method = _infer_basis_and_method_from_run_dir(run_dir)
    if not basis:
        basis = "unknown"
    if not method:
        method = "unknown"
    method_energy_col = f"{method}_{basis}"
    accurate_col = f"Accurate_eng_{args.target}"
    failed_out_dir_name = f"failed_energy_extract_{args.target}.out"
    failed_out_path = os.path.join(failed_out_dir, failed_out_dir_name)

    df_Chem, failed_indices = get_chem_df_from_xyz(xyz_dir=xyz_dir, out_csv_dir=out_dir)
    # ======= Create energy csv from log file ======
    out_csv_path = get_ene_csv_from_log(
        run_dir,
        xyz_dir,
        failed_indices,
        target=args.target,
        energy_log=args.energy_log,
    )
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

# need to correct data types after merge =============================== time: 2026-1-14 11:35am
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
    basis_clean = args.basis.lower().replace("-", "")
    out_name = f"qm9_bonds_energies_{args.target}_{basis_clean}.csv"
    out_path = os.path.join(out_dir, out_name)
    df_merged.to_csv(out_path, index=False)
    logging.info(f"Merged data with energies saved to: {out_path}")

    out_h5_name = f"qm9_bonds_energies_{args.target}_{basis_clean}.h5"
    out_h5_path = os.path.join(out_dir, out_h5_name)
    df_merged.to_hdf(out_h5_path, key='data', mode='w')
    logging.info(f"Merged data with energies saved to: {out_h5_path}")

    update_failed_indices(failed_out_path, failed_indices)

# def main():
#     token = get_inchi_from_xyz("/home/ubuntu/Shiwei/qm9_reaction_eng/qm9_xyz_files/dsgdb9nsd_000001.xyz")
#     print(token)

if __name__ == "__main__":
    main()


