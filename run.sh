#!/bin/bash
set -e

# =============================================================================
# Configuration
# =============================================================================

# Paths
export PROJECT_ROOT=$(pwd)
export SCRIPTS_DIR="$PROJECT_ROOT/scripts"
export CSV_DIR="$PROJECT_ROOT/csv"
export HDF5_DIR="$PROJECT_ROOT/hdf5"
export ORCA_OUT_DIR="$PROJECT_ROOT/qm9_orca_work/qm9_orca_work_molecu/out"
export XYZ_DIR="$PROJECT_ROOT/qm9_xyz_files"

# Output files
export PAIR_CSV="$CSV_DIR/testing_pair.csv"
export TRIPLES_CSV="$CSV_DIR/testing_triples.csv"
export PAIR_H5="$HDF5_DIR/pair.h5"        # Basic name, script handles path
export TRIPLES_H5="$HDF5_DIR/triples.h5"  # Basic name, script handles path

# Python Environment
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Settings
export OVERWRITE_CSV=1          # Set to 1 to overwrite existing CSVs

# =============================================================================
# Execution
# =============================================================================

mkdir -p "$CSV_DIR" "$HDF5_DIR"

echo "====================================================================="
echo "Starting QM9 Reaction Engineering Workflow"
echo "Project Root: $PROJECT_ROOT"
echo "====================================================================="

# 1. Extract Pair Correlation Energies
echo "[1/3] Extracting Pair Correlation Energies..."
if [ "$OVERWRITE_CSV" -eq 1 ] || [ ! -f "$PAIR_CSV" ]; then
    python "$SCRIPTS_DIR/_extract_pair_corr_eng.py" \
        "$ORCA_OUT_DIR"/*.out \
        --output "$PAIR_CSV" \
        --mode write
else
    echo "  Skipping extraction (file exists and OVERWRITE_CSV!=1)"
fi

# 2. Extract Triples Correction (Pair Sum)
echo "[2/3] Extracting Triples Correction..."
if [ "$OVERWRITE_CSV" -eq 1 ] || [ ! -f "$TRIPLES_CSV" ]; then
    python "$SCRIPTS_DIR/_extract_triples_correction.py" \
        "$ORCA_OUT_DIR"/*.out \
        --pair-sum \
        --output "$TRIPLES_CSV" \
        --mode write
else
    echo "  Skipping extraction (file exists and OVERWRITE_CSV!=1)"
fi

# 3. Create HDF5 Data
echo "[3/3] Creating HDF5 Database..."
# Note: The script 05_create_pair_hdf5.py now enforces saving to HDF5_DIR internally
python "$SCRIPTS_DIR/05_create_pair_hdf5.py" \
    --pair-csv "$PAIR_CSV" \
    --pair-h5 "$PAIR_H5" \
    --triples-csv "$TRIPLES_CSV" \
    --triples-h5 "$TRIPLES_H5" \
    --xyz-dir "$XYZ_DIR"

echo "====================================================================="
echo "Success, HDF5 outputs saved to: $HDF5_DIR"
echo "====================================================================="
