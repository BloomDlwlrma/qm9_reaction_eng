import argparse
import logging
import os

from typing import Optional

import pandas as pd


logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")

HARTREE_TO_KCALMOL = 627.50956  # 1 Hartree = 627.50956 kcal/mol


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Compute atomization energy (kcal/mol) for QM9 molecules by subtracting single-atom energies "
            "from molecular energies in qm9_bonds_energies_{ccsd|mp2}.csv."
        )
    )
    parser.add_argument(
        "-T", "--target",
        choices=["ccsd", "mp2"],
        default="ccsd",
        help="Select which accurate-energy column to use: Accurate_eng_{ccsd|mp2}.",
    )
    parser.add_argument(
        "-b", "--basis",
        choices=[
            "def2-SVP", "def2-TZVP", "def2-QZVPP", "6-31G*", "cc-pVDZ", 
            "cc-pVTZ", "aug-cc-pVDZ", "6-31G", "3-21G", "6-31G**", "6-31+G**"
        ],
        default="cc-pVTZ",
        help="Basis set used for calculation. This is only used for inferring the method column if --method_col is not provided. The script looks for a column containing 'osvccsd_{basis}' or 'osvmp2_{basis}' based on --target.",
    )
    parser.add_argument(
        "-O", "--out_dir",
        default="/home/ubuntu/Shiwei/qm9_reaction_eng/csv",
        help=(
            "Directory containing qm9_bonds_energies_{target}.csv and where output will be written. "
            "Output defaults to qm9_bonds_energies_{target}_final.csv."
        ),
    )
    parser.add_argument(
        "--atom_csv",
        default="/home/ubuntu/Shiwei/qm9_reaction_eng/qm9_orca_work/QM9_single_atom_energies_summary.csv",
        help="CSV containing columns: ATOM, ENERGY(Hartree).",
    )
    parser.add_argument(
        "--method_col",
        default=None,
        help=(
            "Column name for the model energy (osv) in the input CSV. "
            "If omitted, the script tries to infer: first column containing 'osvccsd_' or 'osvmp2_' based on --target."
        ),
    )
    return parser.parse_args()


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

    os.makedirs(args.out_dir, exist_ok=True)
    in_csv = os.path.join(args.out_dir, f"qm9_bonds_energies_{args.target}.csv")
    out_csv = os.path.join(args.out_dir, f"qm9_bonds_energies_{args.target}_final.csv")
    accurate_col = f"Accurate_eng_{args.target}"

    logging.info(f"Loading atom energies from: {args.atom_csv}")
    atom_energy_dict = _load_atom_energies_kcal(args.atom_csv)
    logging.info(f"Single-atom energies loaded (kcal/mol): {atom_energy_dict}")

    logging.info(f"Loading molecule CSV: {in_csv}")
    try:
        mol_df = pd.read_csv(in_csv)
    except Exception as e:
        logging.error(f"Failed to read molecule CSV: {in_csv}: {e}")
        return
    logging.info(f"Molecule rows loaded: {len(mol_df)}")

    method_col = args.method_col or _infer_method_col(mol_df, args.target)
    if not method_col:
        logging.error(
            "Cannot infer --method_col. Please pass --method_col explicitly (e.g., osvccsd_631g or osvmp2_631g)."
        )
        logging.error(f"Available columns: {mol_df.columns.tolist()}")
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