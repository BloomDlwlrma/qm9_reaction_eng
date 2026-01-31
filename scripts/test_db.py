#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import logging
import argparse
import os
from typing import Optional
from itertools import combinations
from tqdm import tqdm
import ase.db
from ase import Atoms

# ===================== Initial =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)
logger = logging.getLogger(__name__)

LOG_FILE = "qm9_reaction_eng/scripts/create_reactioncsv.log"
REQUIRED_COLS = {
    "index", "Chem", "InChI",
    "osvccsd_6_31g", "G4MP2", "error"
}
FLOAT_COLS = ["osvccsd_6_31g", "G4MP2", "error"]
REACTION_COLS = [
    "rxnindex", "reactant_index", "product_index",
    "deltaE_osvccsd_6_31g", "deltaE_G4MP2", "deltaE_error"
]
BATCH_WRITE_SIZE = 500000  # Number of reactions to buffer before writing to CSV


def load_qm9_data(input_path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            input_path,
            dtype={
                "index": int,
                "Chem": str,
                "InChI": str,
            },
            encoding="utf-8",
            na_values=["", " "],
            keep_default_na=True
        )
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {input_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV: {str(e)}")

    # 2. check required columns
    missing_cols = REQUIRED_COLS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 3. data cleaning: coerce FLOAT_COLS and drop rows with missing values
    logging.info(f"Starting data cleaning...{FLOAT_COLS}")
    for col in FLOAT_COLS:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=["index", "Chem", "InChI", *FLOAT_COLS])
    df = df.drop_duplicates(subset=["index", "InChI"])  
    df = df.sort_values(by=["Chem", "index"]).reset_index(drop=True)  

    logger.info(f"Data loaded successfully | Valid molecules: {len(df):,} | Chemical formula types: {df['Chem'].nunique()}")
    return df


def calculate_reaction_energy(reactant_row: pd.Series, product_row: pd.Series) -> dict:
    """
    Calculate three types of reaction energies for a single reaction pair using the standard quantum chemistry formula: product value - reactant value
    :param reactant_row: reactant molecule row data
    :param product_row: product molecule row data
    :return: dictionary of reaction energy calculation results
    """
    def _get_val(row, key: str):
        if hasattr(row, key):
            return getattr(row, key)
        return row[key]

    return {
        "deltaE_osvccsd_6_31g": _get_val(product_row, "osvccsd_6_31g") - _get_val(reactant_row, "osvccsd_6_31g"),
        "deltaE_G4MP2": _get_val(product_row, "G4MP2") - _get_val(reactant_row, "G4MP2"),
        "deltaE_error": _get_val(product_row, "error") - _get_val(reactant_row, "error"),
    }


def generate_full_isomer_reactions(df: pd.DataFrame, output_path: str, output_db_path: Optional[str] = None) -> None:
    """
    Core logic: group by Chem to generate all reaction pairs + calculate reaction energies + incrementally write to CSV
    For n isomers under the same Chem → generate n*(n-1)/2 reaction pairs, including all combinations A→B, A→C... B→C...
    :param df: cleaned QM9 molecule data
    :param output_path: output reaction CSV file path
    """
    # Initialize output file, write header
    pd.DataFrame(columns=REACTION_COLS).to_csv(
        output_path, index=False, encoding="utf-8", mode="w"
    )

    db = None
    if output_db_path:
        os.makedirs(os.path.dirname(output_db_path) or ".", exist_ok=True)
        if os.path.exists(output_db_path):
            os.remove(output_db_path)
        db = ase.db.connect(output_db_path, append=False)

    reaction_buffer = []  
    rxn_index = 0  

    chem_groups = df.groupby("Chem")
    for chem, group_df in tqdm(chem_groups, desc="Generate reaction pairs + calculate reaction energies by chemical formula group", unit="group"):
        isomer_count = len(group_df)
        if isomer_count < 2:
            continue  

        total_pairs = int(isomer_count * (isomer_count - 1) / 2)
        logger.debug(f"Chemical formula {chem} | Isomer count: {isomer_count} | Expected reaction pairs: {total_pairs:,}")

        for reactant, product in combinations(group_df.itertuples(index=False), 2):
            # Assemble single reaction data
            reaction_data = {
                "rxnindex": rxn_index,
                "reactant_index": reactant.index,
                "product_index": product.index,
            }
            # Calculate and append reaction energy data
            reaction_data.update(calculate_reaction_energy(reactant, product))
            reaction_buffer.append(reaction_data)

            rxn_index += 1

            # Batch write to CSV/DB to optimize performance
            if len(reaction_buffer) >= BATCH_WRITE_SIZE:
                if db is not None:
                    for row in reaction_buffer:
                        db.write(Atoms(), key_value_pairs=row)
                pd.DataFrame(reaction_buffer)[REACTION_COLS].to_csv(
                    output_path, index=False, encoding="utf-8", mode="a", header=False
                )
                reaction_buffer.clear()

    # Write the last batch of buffer data to ensure no omissions
    if reaction_buffer:
        if db is not None:
            for row in reaction_buffer:
                db.write(Atoms(), key_value_pairs=row)
        pd.DataFrame(reaction_buffer)[REACTION_COLS].to_csv(
            output_path, index=False, encoding="utf-8", mode="a", header=False
        )

    if db is not None:
        db.close()

    logger.info(f"Final statistics | Total reaction pairs generated: {rxn_index:,} | Max reaction index: {rxn_index - 1}")
    logger.info(f"Output file path: {output_path}")
    if output_db_path:
        logger.info(f"Output db path: {output_db_path}")


def main():
    """Command-line argument parsing + main process orchestration, standardized design"""
    parser = argparse.ArgumentParser(
        description="QM9 full isomer reaction pair generator [with reaction energy calculation], generates n*(n-1)/2 reaction pairs grouped by Chem",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-i", "--input_csv",
        default="/home/ubuntu/Shiwei/qm9_reaction_eng/csv/qm9_bonds_energies.csv",
        required=True,
        type=str,
        help="Input QM9 molecule CSV file path"
    )
    parser.add_argument(
        "-o", "--output_csv",
        default="/home/ubuntu/Shiwei/qm9_reaction_eng/csv/reactions_test.csv",
        type=str,
        help="Output reaction pair CSV file path"
    )
    parser.add_argument(
        "--output_db",
        default=None,
        type=str,
        help="Optional output ASE db (.db) file path to store reactions as key_value_pairs"
    )
    args = parser.parse_args()

    # Main process: load data → generate reaction pairs + calculate reaction energies → write to file
    try:
        qm9_df = load_qm9_data(args.input_csv)
        generate_full_isomer_reactions(qm9_df, args.output_csv, args.output_db)
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}", exc_info=True) # Log full traceback on error


if __name__ == "__main__":
    main()