#!/usr/bin/env python3
import os
import glob
import shutil
import sys

# Configuration
WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
ORCA_FILES_DIR = os.path.join(WORK_ROOT, "orca_files")
# These are where final results go, we generally DON'T want to delete these unless specified
FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_out")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_mkl")
FINAL_RUNDIR_DIR = os.path.join(WORK_ROOT, "orca_rundir_info")
def clean_orca_files():
    """
    Cleans the 'orca_files' working directory.
    This directory is used for temporary ORCA execution files.
    """
    print(f"Cleaning working directory: {FINAL_OUT_DIR}")
    if not os.path.exists(FINAL_OUT_DIR):
        print("Directory does not exist.")
        return

    # List files to be deleted
    files = glob.glob(os.path.join(FINAL_OUT_DIR, "*"))
    
    if not files:
        print("Directory is already empty.")
        return

    print(f"Found {len(files)} files/directories to delete.")
    
    # Confirm with user if running interactively
    if sys.stdin.isatty():
        response = input("Are you sure you want to delete them? (y/N): ")
        if response.lower() != 'y':
            print("Operation cancelled.")
            return

    deleted_count = 0
    errors = 0
    
    for f in files:
        try:
            if os.path.isfile(f) or os.path.islink(f):
                os.unlink(f)
            elif os.path.isdir(f):
                shutil.rmtree(f)
            deleted_count += 1
        except Exception as e:
            print(f"Failed to delete {f}. Reason: {e}")
            errors += 1
            
    print(f"Cleanup complete. Deleted: {deleted_count}, Errors: {errors}")

if __name__ == "__main__":
    print("=== ORCA Working Directory Cleanup Tool ===")
    print("Warning: This will delete ALL files in the temporary working directory.")
    print("Running this while jobs are active will cause them to FAIL.")
    clean_orca_files()
