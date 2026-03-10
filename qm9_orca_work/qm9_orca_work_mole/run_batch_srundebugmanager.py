#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
import glob
import multiprocessing
import argparse
import logging
import sqlite3
import re
import time
import queue # Import queue for thread-safe/process-safe communication

# ==============================================================================
# Logger & Config
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

parser = argparse.ArgumentParser(description="ORCA SPE Batch Runner")
parser.add_argument("start_id", type=int)
parser.add_argument("end_id", type=int)
parser.add_argument("concurrency", type=int)
parser.add_argument("basis", type=str)
parser.add_argument("methods", type=str)
parser.add_argument("--work-subdir", type=str, required=True, 
                    help="Fixed path for this chunk")

args = parser.parse_args()

START_ID    = args.start_id
END_ID      = args.end_id
CONCURRENCY = args.concurrency
BASIS       = args.basis
METHODS     = args.methods.split(',')
WORK_SUBDIR = args.work_subdir

# =============================================================================
# Error Patterns
# =============================================================================
ERROR_PATTERNS = [
    "aborting the run",
    "abnormal termination",
    "aborted",
    "the job aborted",
    "killed",
    "segfault",
    "segmentation fault",
    "mpirun detected",
    "out of memory",
    "execution failed",
    "error termination",
    "the mdci module",
    "mdci_state.cpp",
    "orca_mdci_mpi",
    "not enough slots",
    "there are not enough slots available",
    "illegal state",
    "signal: aborted",
    "received signal",
    "\*\*\* Process.*received signal",
    "primary job .* non-zero exit code",
    "non-zero exit code",
    "primary job",
    "End of error message",
    "Invalid argument",
    "Wrong syntax in xyz coordinates"
]
COMPILED_ERRORS = [re.compile(p, re.IGNORECASE) for p in ERROR_PATTERNS]

# =============================================================================
# Setup Environment & Paths
# =============================================================================
WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
ORCA_FILES_BASE = os.path.join(WORK_SUBDIR, "orca_files")
methods_str = '_'.join(sorted(METHODS))

FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_out_{methods_str}_{BASIS}")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_mkl_{methods_str}_{BASIS}")
FAILED_LOG_DIR = os.path.join(WORK_ROOT, "orca_output", "failed_logs")

DB_PARENT_DIR = os.path.join(WORK_ROOT, "checkpoints", f"checkpoint_{BASIS}_{methods_str}")
DB_FILE = os.path.join(DB_PARENT_DIR, f"run_chunk_{START_ID}_{END_ID}.db")

ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN  = os.path.join(ORCA_HOME, "bin", "orca")
SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources"

# ==============================================================================
# Helper Functions
# ==============================================================================
def init_dirs_and_db():
    os.makedirs(ORCA_FILES_BASE, exist_ok=True)
    os.makedirs(FINAL_OUT_DIR, exist_ok=True)
    os.makedirs(FINAL_MKL_DIR, exist_ok=True)
    os.makedirs(FAILED_LOG_DIR, exist_ok=True)
    os.makedirs(DB_PARENT_DIR, exist_ok=True)
    
    for i in range(CONCURRENCY):
        os.makedirs(get_slot_dir(i), exist_ok=True)

    conn = sqlite3.connect(DB_FILE, timeout=60.0)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS completed (
            mol_id TEXT, method TEXT, done INTEGER DEFAULT 1,
            PRIMARY KEY (mol_id, method))''')
    conn.commit()
    conn.close()

def is_completed(mol_id, method):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60.0)
        c = conn.cursor()
        c.execute("SELECT 1 FROM completed WHERE mol_id=? AND method=?", (str(mol_id), method))
        res = c.fetchone()
        conn.close()
        return res is not None
    except:
        return False

def mark_completed(mol_id, method):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60.0)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO completed (mol_id, method, done) VALUES (?, ?, 1)", (str(mol_id), method))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB Error] {mol_id}: {e}")

def check_file_for_errors(filepath):
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        return True 
    try:
        with open(filepath, 'r', errors='ignore') as f:
            content = f.read()
        for pattern in COMPILED_ERRORS:
            if pattern.search(content):
                return True 
        return False 
    except Exception:
        return True

def get_slot_dir(slot_id):
    return os.path.join(ORCA_FILES_BASE, f"slot_{slot_id}")

def prepare_spe_restart_input(original_inp, restart_gbw_filename):
    with open(original_inp, 'r') as f:
        lines = f.readlines()
    
    new_lines = []
    header_processed = False
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('!') and not header_processed:
            clean_line = re.sub(r'\s+MOREAD', '', line, flags=re.IGNORECASE).strip()
            new_lines.append(f"{clean_line} MOREAD\n")
            new_lines.append(f'%moinp "{restart_gbw_filename}"\n')
            header_processed = True
        elif "%moinp" in line.lower():
            continue
        else:
            new_lines.append(line)
            
    with open(original_inp, 'w') as f:
        f.writelines(new_lines)

# ==============================================================================
# Task Execution
# ==============================================================================
def run_task(task_info, slot_dir, lock):
    # Unpack task info
    mol_id, method, nprocs, slot_id = task_info
    
    # Define file names
    inp_file_name = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    src_path = os.path.join(SOURCE_ROOT, method, f"{BASIS}_{method}", inp_file_name)
    
    # We copy input file ON DEMAND here, inside the worker
    # This prevents the "Distributed 0 files" error and ensures fresh inputs
    if not os.path.exists(src_path):
        print(f"[Slot {slot_id}] Source not found: {src_path}")
        return

    work_inp = os.path.join(slot_dir, inp_file_name)
    shutil.copy2(src_path, work_inp)

    job_base = inp_file_name.replace(".inp", "")
    current_out = os.path.join(slot_dir, f"{job_base}.out")
    current_gbw = os.path.join(slot_dir, f"{job_base}.gbw")
    mkl_path    = os.path.join(slot_dir, f"{job_base}.mkl")
    
    # Unique restart name
    restart_gbw_name = f"{job_base}_restart.gbw"
    restart_gbw      = os.path.join(slot_dir, restart_gbw_name)
    
    prefix = f"[Slot {slot_id}|{mol_id}]"

    # --- 1. Resume Check (Pre-run) ---
    is_restart = False
    if os.path.exists(restart_gbw):
        print(f"{prefix} Found valid restart GBW. Configuring input.")
        try:
            prepare_spe_restart_input(work_inp, restart_gbw_name)
            is_restart = True
        except Exception as e:
            print(f"{prefix} Restart prep failed: {e}. Starting fresh.")
            if os.path.exists(restart_gbw): os.remove(restart_gbw)

    # --- 2. Clean Temp Files ---
    for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
        abs_f = os.path.abspath(f)
        if abs_f == os.path.abspath(work_inp): continue 
        if abs_f == os.path.abspath(restart_gbw): continue 
        try: os.remove(f)
        except: pass

    # --- 3. Set %pal ---
    with open(work_inp, 'r') as f: lines = f.readlines()
    with open(work_inp, 'w') as f:
        f.write(f"%pal nprocs {nprocs} end\n")
        for line in lines:
            if "%pal" not in line.lower(): f.write(line)

    # --- 4. Run ORCA ---
    env = os.environ.copy()
    env["PATH"] = f"{ORCA_HOME}/bin:{env.get('PATH','')}"
    env["LD_LIBRARY_PATH"] = f"{ORCA_HOME}/lib:{env.get('LD_LIBRARY_PATH','')}"
    env["OMPI_MCA_rmaps_base_oversubscribe"] = "true"

    # NO CPU BINDING to allow OS scheduler to optimize utilization
    cmd = [ORCA_BIN, work_inp]

    try:
        with open(current_out, "w") as outf:
            # Shorten timeout slightly to allow cleanup before SLURM kills it? 
            # Actually 23h is fine if SLURM job is 24h. 
            subprocess.run(cmd, stdout=outf, stderr=subprocess.STDOUT, env=env, timeout=166*3600) # 7 days is 168h
    except subprocess.TimeoutExpired:
        print(f"{prefix} Timeout detected.")
        pass 
    except Exception as e:
        print(f"{prefix} Exec Error: {e}")
        return

    # --- 5. Post-process (MKL) ---
    if os.path.exists(current_gbw):
        try:
            subprocess.run(["orca_2mkl", job_base, "-mkl"], cwd=slot_dir, env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except: pass

    # --- 6. Status Check ---
    is_success = False
    if os.path.exists(current_out):
        try:
            with open(current_out, 'rb') as f:
                f.seek(-2048, 2)
                tail = f.read().decode(errors='ignore')
            if "ORCA TERMINATED NORMALLY" in tail:
                is_success = True
        except: pass
    
    has_explicit_error = check_file_for_errors(current_out)

    # --- 7. Cleanup Logic ---

    if is_success:
        shutil.copy2(current_out, os.path.join(FINAL_OUT_DIR, f"{job_base}.out"))
        if os.path.exists(mkl_path):
            shutil.copy2(mkl_path, os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl"))
        
        with lock: mark_completed(mol_id, method)
        print(f"{prefix} DONE.")
        
        # Cleanup ALL
        for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
            try: os.remove(f)
            except: pass

    elif has_explicit_error:
        print(f"{prefix} FAILED (Explicit Error). Cleaning up for fresh restart.")
        fail_log_path = os.path.join(FAILED_LOG_DIR, f"{job_base}_failed.out")
        try: shutil.copy2(current_out, fail_log_path)
        except: pass

        # Cleanup ALL (Force fresh start)
        for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
            try: os.remove(f)
            except: pass

    else:
        print(f"{prefix} INTERRUPTED. Saving GBW for resume.")
        if os.path.exists(current_gbw):
            shutil.move(current_gbw, restart_gbw)
        
        # Delete everything else (including .inp) to ensure fresh copy next time
        for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
            if os.path.basename(f) == restart_gbw_name: continue 
            try: os.remove(f)
            except: pass

def worker(slot_id, cores_per_slot, task_queue, lock):
    """
    Worker process that grabs tasks from a shared Queue.
    This ensures no slot sits idle if other molecules are waiting.
    """
    slot_dir = get_slot_dir(slot_id)
    
    while True:
        try:
            # Non-blocking get? No, blocking is better here.
            # Get a task: (mol_id, method)
            task = task_queue.get(timeout=5) 
        except queue.Empty:
            # Queue is empty, worker is done
            break
            
        mol_id, method = task
        
        # Double check DB before running
        if is_completed(mol_id, method): 
            task_queue.task_done()
            continue
            
        # Run
        try:
            run_task((mol_id, method, cores_per_slot, slot_id), slot_dir, lock)
        except Exception as e:
            print(f"[Slot {slot_id}] Critical Error: {e}")
        
        task_queue.task_done()

# ==============================================================================
# Main
# ==============================================================================
def main():
    init_dirs_and_db()

    # --- 1. Identify all tasks ---
    print("[Setup] identifying tasks...")
    all_tasks = []
    
    # Pre-check existing files to populate DB
    synced_count = 0
    
    for mol_id in range(START_ID, END_ID + 1):
        for method in METHODS:
            # Check DB
            if is_completed(mol_id, method): continue
            
            # Check Disk
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            final_out = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
            
            if os.path.exists(final_out):
                has_error = check_file_for_errors(final_out)
                is_valid = False
                if not has_error:
                    try:
                        with open(final_out, 'r', errors='ignore') as f:
                            if "ORCA TERMINATED NORMALLY" in f.read():
                                is_valid = True
                    except: pass
                
                if is_valid:
                    mark_completed(mol_id, method)
                    synced_count += 1
                else:
                    # If file exists but invalid, we add to queue to rerun
                    all_tasks.append((mol_id, method))
            else:
                # Not in DB, not on disk -> Add to queue
                all_tasks.append((mol_id, method))

    print(f"[Pre-check] Synced {synced_count} valid jobs from disk.")
    print(f"[Queue] Added {len(all_tasks)} tasks to queue.")

    # --- 2. Configure Resources ---
    slurm_cpus = os.environ.get('SLURM_CPUS_PER_TASK')
    total_cores = int(slurm_cpus) if slurm_cpus else multiprocessing.cpu_count()
    cores_per_slot = total_cores // CONCURRENCY

    print(f"CONFIG: {total_cores} cores | {CONCURRENCY} slots | {cores_per_slot} cores/slot (Dynamic Queue)")
    
    # --- 3. Start Workers ---
    # Use Manager.Queue for process-safe queue
    manager = multiprocessing.Manager()
    task_queue = manager.Queue()
    lock = manager.Lock()
    
    for t in all_tasks:
        task_queue.put(t)
        
    procs = []
    for i in range(CONCURRENCY):
        p = multiprocessing.Process(target=worker, args=(i, cores_per_slot, task_queue, lock))
        p.start()
        procs.append(p)
    
    for p in procs: p.join()
    print("=== CHUNK DONE ===")

if __name__ == "__main__":
    main()