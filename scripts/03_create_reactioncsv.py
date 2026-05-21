#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from ast import arg
import pandas as pd
import logging
import os
import argparse
from itertools import combinations
from tqdm import tqdm

# ===================== Initial =====================
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LOG_FILE = "qm9_reaction_eng/scripts/create_reactioncsv.log"
BATCH_WRITE_SIZE = 500000  # Number of reactions to buffer before writing to CSV

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="QM9 full isomer reaction pair generator [with reaction energy calculation], generates n*(n-1)/2 reaction pairs grouped by Chem",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-t", "--target",
        choices=["ccsd", "mp2"],
        default="ccsd",
        help="Select which accurate-energy column to use: Accurate_eng_{ccsd|mp2}.",
    )
    parser.add_argument(
        "-i", "--input_path",
        default="/home/ubuntu/Shiwei/qm9_reaction_eng/csv",
        required=True,
        type=str,
        help="Input QM9 molecule CSV file path, the input csv(qm9_bonds_energies_{target}.csv) should locate in this folder.",
    )
    parser.add_argument(
        "-o", "--output_path",
        default="/home/ubuntu/Shiwei/qm9_reaction_eng/csv",
        type=str,
        help=("Output reaction pair CSV file path, defaults reactions_{target}.csv. will be saved in this folder"),
    )
    return parser.parse_args()

# def _infer_method_from_run_dir(target: str):
#     if not target:
#         return None
#     name = os.path.basename(os.path.normpath(target))
#     tokens = [t for t in name.lower().split('_') if t]

#     method = None
#     for t in tokens:
#         if t in {"ccsd", "mp2"}:
#             method = t
#     return method

def get_qm9_data(input_path: str, target: str) -> pd.DataFrame:
    # 1. load CSV
    logging.info(f"Loading input CSV from: {input_path} ...")
    input_csv = os.path.join(input_path, f"qm9_bonds_energies_{target}.csv")
    try:
        df = pd.read_csv(
            input_csv,
            dtype={
                "index": int,
                "qm9_index": str,
                "Chem": str,
                # "InChI": str,
            },
            encoding="utf-8",
            na_values=["", " "],
            keep_default_na=True
        )
        logging.info(f"Input CSV loaded from: {input_csv}")
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {input_csv}")
    except Exception as e:
        raise RuntimeError(f"Failed to load CSV: {str(e)}")

    # 2. check required columns
    logging.info("Checking required columns in input CSV...")
    REQUIRED_COLS = {"index", "qm9_index", "Chem", f"osv{target}_631g", f"Accurate_eng_{target}", "error"}   
    missing_cols = REQUIRED_COLS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 3. data cleaning: coerce FLOAT_COLS and drop rows with missing values
    FLOAT_COLS = [f"osv{target}_631g", f"Accurate_eng_{target}", "error"]
    logging.info(f"Starting data cleaning...{FLOAT_COLS}")
    for col in FLOAT_COLS:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    df = df.dropna(subset=["index", "Chem", "qm9_index", *FLOAT_COLS])
    df = df.drop_duplicates(subset=["index", "qm9_index"])  
    df = df.sort_values(by=["Chem", "index"]).reset_index(drop=True)  

    logger.info(f"Data loaded successfully | Valid molecules: {len(df):,} | Chemical formula types: {df['Chem'].nunique()}")
    return df


def calculate_reaction_energy(reactant_row: pd.Series, product_row: pd.Series, input_path: str, target: str) -> dict:
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
        f"deltaE_osv{target}_631g": _get_val(product_row, f"osv{target}_631g") - _get_val(reactant_row, f"osv{target}_631g"),
        f"deltaE_Accurate_eng_{target}": _get_val(product_row, f"Accurate_eng_{target}") - _get_val(reactant_row, f"Accurate_eng_{target}"),
        "deltaE_error": _get_val(product_row, "error") - _get_val(reactant_row, "error"),
    }


def generate_full_isomer_reactions(df: pd.DataFrame, input_path: str, output_path: str, target: str) -> None:
    """
    Core logic: group by Chem to generate all reaction pairs + calculate reaction energies + incrementally write to CSV
    For n isomers under the same Chem → generate n*(n-1)/2 reaction pairs, including all combinations A→B, A→C... B→C...
    :param df: cleaned QM9 molecule data
    :param output_path: output reaction CSV file path
    """
    out_csv = os.path.join(output_path, f"reactions_{target}.csv")
    REACTION_COLS = ["rxnindex", "reactant_index", "product_index", f"deltaE_osv{target}_631g", f"deltaE_Accurate_eng_{target}", "deltaE_error"]
    # Initialize output file, write header
    pd.DataFrame(columns=REACTION_COLS).to_csv(
        out_csv, index=False, encoding="utf-8", mode="w"
    )

    reaction_buffer = []  
    rxn_index = 0  

    chem_groups = df.groupby("Chem")
    for chem, group_df in tqdm(chem_groups, desc="Generate reaction pairs and calculate reaction energies by chemical formula group", unit="group"):
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
            reaction_data.update(calculate_reaction_energy(reactant, product, input_path, target))
            reaction_buffer.append(reaction_data)

            rxn_index += 1

            # Batch write to CSV to optimize performance
            if len(reaction_buffer) >= BATCH_WRITE_SIZE:
                pd.DataFrame(reaction_buffer)[REACTION_COLS].to_csv(
                    out_csv, index=False, encoding="utf-8", mode="a", header=False
                )
                reaction_buffer.clear()

    # Write the last batch of buffer data to ensure no omissions
    if reaction_buffer:
        pd.DataFrame(reaction_buffer)[REACTION_COLS].to_csv(
            out_csv, index=False, encoding="utf-8", mode="a", header=False
        )

    logger.info(f"Final statistics | Total reaction pairs generated: {rxn_index:,} | Max reaction index: {rxn_index - 1}")
    logger.info(f"Output file path: {out_csv}")


def main():
    '''
    Docstring for main
    usage: 03_create_reactioncsv.py [-h] [-t {ccsd,mp2}] -i INPUT_PATH [-o OUTPUT_PATH]
    QM9 full isomer reaction pair generator [with reaction energy calculation], generates n*(n-1)/2 reaction pairs grouped by Chem

    optional arguments:
    -h, --help            show this help message and exit
    -t {ccsd,mp2}, --target {ccsd,mp2}
                            Select which accurate-energy column to use: Accurate_eng_{ccsd|mp2}. (default: ccsd)
    -i INPUT_PATH, --input_path INPUT_PATH
                            Input QM9 molecule CSV file path, the input csv(qm9_bonds_energies_{target}.csv) should locate in this folder. (default: /home/ubuntu/Shiwei/qm9_reaction_eng/csv)
    -o OUTPUT_PATH, --output_path OUTPUT_PATH
                            Output reaction pair CSV file path, defaults reactions_{target}.csv. will be saved in this folder (default: /home/ubuntu/Shiwei/qm9_reaction_eng/csv)
    
    example usage:
        python /home/ubuntu/Shiwei/qm9_reaction_eng/scripts/03_create_reactioncsv.py -t mp2 -i /home/ubuntu/Shiwei/qm9_reaction_eng/csv -o /home/ubuntu/Shiwei/qm9_reaction_eng/csv
        python /home/ubuntu/Shiwei/qm9_reaction_eng/scripts/03_create_reactioncsv.py -t ccsd -i /home/ubuntu/Shiwei/qm9_reaction_eng/csv -o /home/ubuntu/Shiwei/qm9_reaction_eng/csv
    
    ''' 
    # Parse command-line arguments
    args = parse_arguments()
    target = args.target
    
    # Main process: load data → generate reaction pairs + calculate reaction energies → write to file
    try:
        qm9_df = get_qm9_data(args.input_path, target)
        generate_full_isomer_reactions(qm9_df, args.input_path, args.output_path, target)
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}", exc_info=True) # Log full traceback on error


if __name__ == "__main__":
    main()