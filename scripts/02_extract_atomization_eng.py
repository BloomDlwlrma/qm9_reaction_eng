import argparse
import logging
import os
import re
from typing import Optional

import pandas as pd


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

HARTREE_TO_KCALMOL = 627.50956  # 1 Hartree = 627.50956 kcal/mol


def _get_project_root() -> str:
    """Return qm9_reaction_eng directory path (parent of scripts/)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Compute atomization energy (kcal/mol) for QM9 molecules."
    )
    parser.add_argument(
        "run_dir",
        help="Path to the run directory containing ene_total_withhf.log."
    )
    return parser.parse_args()


def _infer_basis_and_method_from_run_dir(run_name: str):
    """Infer basis and method tokens from a run directory name.

    Example run folder name:
      ..._631g_osvccsd_...

    Returns
    -------
    (basis, method)
    """
    if not run_name:
        return None, None
    tokens = [t.lower() for t in run_name.split('_') if t]

    basis = None
    method = None

    # Method mapping to standard format
    method_map = {
        "osvccsd": "OSVCCSD",
        "osvmp2": "OSVMP2",
        "ccsd": "OSVCCSD",
        "mp2": "OSVMP2",
    }

    # Basis mapping
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
        if t in method_map:
            method = method_map[t]
        if t in basis_map:
            basis = basis_map[t]
        elif re.fullmatch(r"\d{3,4}g\*?", t):
            if t == '631g': basis = "6-31G"
            else: basis = t

    return basis, method


def _load_atom_energies_kcal(atom_csv: str) -> dict[str, float]:
    """Load single-atom energies from ORCA summary CSV and convert Hartree -> kcal/mol."""
    try:
        atom_energy_df = pd.read_csv(atom_csv)
    except Exception as e:
        raise RuntimeError(f"Failed to read atom energy CSV: {atom_csv}: {e}")

    required = {"ATOM", "ENERGY(Hartree)"}
    missing = required - set(atom_energy_df.columns)
    if missing:
        raise RuntimeError(f"Atom energy CSV missing columns: {sorted(missing)}")

    atom_energy_dict: dict[str, float] = {}
    for _, row in atom_energy_df.iterrows():
        atom = str(row["ATOM"]).strip()
        try:
            energy_hartree = float(row["ENERGY(Hartree)"])
        except Exception:
            continue
        atom_energy_dict[atom] = energy_hartree * HARTREE_TO_KCALMOL

    for need in ["C", "H", "O", "N", "F"]:
        if need not in atom_energy_dict:
            raise RuntimeError(f"Atom energy CSV missing energy for required atom: {need}")
    return atom_energy_dict


def _infer_method_col(df: pd.DataFrame, target: str) -> Optional[str]:
    want = "osvccsd_" if target == "ccsd" else "osvmp2_"
    for c in df.columns:
        if want in str(c):
            return c
    return None


def _calculate_atomization_energy_row(row: pd.Series, atom_energy_dict: dict[str, float], method_col: str, accurate_col: str):
    c = row["C_num"]
    h = row["H_num"]
    o = row["O_num"]
    n = row["N_num"]
    f = row["F_num"]

    total_atom_energy = (
        c * atom_energy_dict["C"]
        + h * atom_energy_dict["H"]
        + o * atom_energy_dict["O"]
        + n * atom_energy_dict["N"]
        + f * atom_energy_dict["F"]
    )

    e_mol_method = row[method_col]
    e_mol_acc = row[accurate_col]

    # Keep the original definition from the previous script.
    atom_energy_method = e_mol_method - total_atom_energy
    atom_energy_acc = e_mol_acc - total_atom_energy
    return atom_energy_method, atom_energy_acc


def main():
    args = parse_arguments()

    # Get project paths
    project_root = _get_project_root()
    out_dir = os.path.join(project_root, "src", "csv")
    
    # Extract run directory info
    source_dir = os.path.basename(os.path.normpath(args.run_dir))
    basis, method = _infer_basis_and_method_from_run_dir(source_dir)
    
    if not basis or not method:
        logging.error(f"Cannot infer basis/method from run_dir: {args.run_dir}")
        return
    
    # Determine target (ccsd or mp2) from method
    target = "ccsd" if "CCSD" in method else "mp2"
    
    # Set file paths
    atom_csv = os.path.join(project_root, "qm9_orca_work", "qm9_orca_work_elem", "QM9_single_atom_energies_summary.csv")
    in_csv = os.path.join(out_dir, f"source_energies_{basis}_{target}_{source_dir}.csv")
    out_csv = os.path.join(out_dir, f"label_energies_{basis}_{target}_{source_dir}.csv")
    accurate_col = f"Accurate_eng_{target}"

    os.makedirs(out_dir, exist_ok=True)

    logging.info(f"Loading atom energies from: {atom_csv}")
    atom_energy_dict = _load_atom_energies_kcal(atom_csv)
    logging.info(f"Single-atom energies loaded (kcal/mol): {atom_energy_dict}")

    logging.info(f"Loading molecule CSV: {in_csv}")
    try:
        mol_df = pd.read_csv(in_csv)
    except Exception as e:
        logging.error(f"Failed to read molecule CSV: {in_csv}: {e}")
        return
    logging.info(f"Molecule rows loaded: {len(mol_df)}")

    method_col = _infer_method_col(mol_df, target)
    if not method_col:
        logging.error(
            f"Cannot infer method column for target={target}. Available columns: {mol_df.columns.tolist()}"
        )
        return

    required_cols = [method_col, accurate_col, "C_num", "H_num", "O_num", "N_num", "F_num"]
    missing = [c for c in required_cols if c not in mol_df.columns]
    if missing:
        logging.error(f"Input CSV missing required columns: {missing}")
        logging.error(f"Available columns: {mol_df.columns.tolist()}")
        return

    # Filter invalid rows
    before = len(mol_df)
    mol_df = mol_df.dropna(subset=required_cols)
    after = len(mol_df)
    logging.info(f"Filtered NA rows: {before} -> {after}")

    logging.info(
        f"Computing atomization energies using method_col={method_col}, accurate_col={accurate_col} (units: kcal/mol)"
    )
    mol_df[["atomization_energy_osv", "atomization_energy_acc"]] = mol_df.apply(
        lambda x: pd.Series(_calculate_atomization_energy_row(x, atom_energy_dict, method_col, accurate_col)),
        axis=1,
    )

    try:
        mol_df.to_csv(out_csv, index=False, encoding="utf-8")
    except Exception as e:
        logging.error(f"Failed to write output CSV: {out_csv}: {e}")
        return

    logging.info(f"Done. Output written to: {out_csv}")
    if "qm9_index" in mol_df.columns:
        logging.info(f"Preview columns: index/Chem/qm9_index/atomization_energy_osv/atomization_energy_acc")
    else:
        logging.info(f"Preview columns: atomization_energy_osv/atomization_energy_acc")


if __name__ == "__main__":
    main()