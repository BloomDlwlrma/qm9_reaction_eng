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

ERROR_PATTERNS = [
    "aborting the run",
    "abnormal termination",
    "aborted",
    "the job aborted",
    "killed",
    "segfault",
    "segmentation fault",
    "out of memory",
    "execution failed",
    "error termination",
    "not enough slots",
    "illegal state",
    "signal: aborted",
    "non-zero exit code",
    "End of error message",
    "Invalid argument",
    "Wrong syntax in xyz coordinates",
    "command not found",
    "error",
    "FATAL ERROR ENCOUNTERED"
]
COMPILED_ERRORS = [re.compile(p, re.IGNORECASE) for p in ERROR_PATTERNS]

WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
methods_str = '_'.join(sorted(METHODS))
if args.work_subdir:
    ORCA_FILES_BASE = os.path.join(args.work_subdir, "orca_files")
    print(f"[Isolation] Using job-specific ORCA_FILES_BASE: {ORCA_FILES_BASE}")
else:
    ORCA_FILES_BASE = os.path.join(WORK_ROOT, "orca_files", "orca_files_debug")
    print("Warning: No --work-subdir provided, using shared directory (risk of conflict)")

FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_out_{methods_str}_{BASIS}")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_mkl_{methods_str}_{BASIS}")
FAILED_LOG_DIR = os.path.join(WORK_ROOT, "orca_output", "failed_logs")

# ================ 新增：SQLite 資料庫設定 ================
DB_PARENT_DIR = os.path.join(WORK_ROOT, "checkpoints", f"checkpoint_{BASIS}_{methods_str}")
DB_FILE = os.path.join(DB_PARENT_DIR, f"run_chunk_{START_ID}_{END_ID}.db")
# =======================================================

ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN  = os.path.join(ORCA_HOME, "bin", "orca")

SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources"
NEW_XYZ_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/xyz_files/"


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

def run_task(task_info, slot_dir, lock):
    mol_id, method, nprocs, slot_id = task_info
    inp_file_name = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    src_path = os.path.join(SOURCE_ROOT, method, f"{BASIS}_{method}", inp_file_name)
    if not os.path.exists(src_path):
        print(f"[Slot {slot_id}] Source not found: {src_path}")
        return
    work_inp = os.path.join(slot_dir, inp_file_name)
    shutil.copy2(src_path, work_inp)
    job_base = inp_file_name.replace(".inp", "")
    current_out = os.path.join(slot_dir, f"{job_base}.out")
    current_gbw = os.path.join(slot_dir, f"{job_base}.gbw")
    mkl_path    = os.path.join(slot_dir, f"{job_base}.mkl")
    restart_gbw_name = f"{job_base}_restart.gbw"
    restart_gbw      = os.path.join(slot_dir, restart_gbw_name)
    prefix = f"[Slot {slot_id}|{mol_id}]"

    is_restart = False
    if os.path.exists(restart_gbw):
        print(f"{prefix} Found valid restart GBW. Configuring input.")
        try:
            prepare_spe_restart_input(work_inp, restart_gbw_name)
            is_restart = True
        except Exception as e:
            print(f"{prefix} Restart prep failed: {e}. Starting fresh.")
            if os.path.exists(restart_gbw): os.remove(restart_gbw)
    for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
        abs_f = os.path.abspath(f)
        if abs_f == os.path.abspath(work_inp): continue 
        if abs_f == os.path.abspath(restart_gbw): continue 
        try: os.remove(f)
        except: pass
    with open(work_inp, 'r') as f: lines = f.readlines()
    with open(work_inp, 'w') as f:
        f.write(f"%pal nprocs {nprocs} end\n")
        for line in lines:
            if "%pal" in line.lower(): continue
            if line.strip().lower().startswith("*xyzfile"):
                parts = line.split()
                if len(parts) >= 4:
                    old_path = parts[-1]
                    xyz_filename = os.path.basename(old_path)
                    new_path = os.path.join(NEW_XYZ_ROOT, xyz_filename)
                    line = f"* xyzfile {parts[1]} {parts[2]} {new_path}\n"
            f.write(line)    

    env = os.environ.copy()
    env["OMPI_MCA_btl"] = "^openib"
    env["PATH"] = f"{ORCA_HOME}/bin:{env.get('PATH','')}"
    env["LD_LIBRARY_PATH"] = f"{ORCA_HOME}/lib:{env.get('LD_LIBRARY_PATH','')}"
    #env["OMPI_MCA_rmaps_base_oversubscribe"] = "true"
    env["OMPI_MCA_hwloc_base_binding_policy"] = "core"
    env["OMPI_MCA_rmaps_base_mapping_policy"] = "slot"
    #skip_binding = os.environ.get("ORCA_SKIP_CPU_BIND", "0") == "1"
    
    cmd = [ORCA_BIN, work_inp]
    try:
        with open(current_out, "w") as outf:
            subprocess.run(cmd, stdout=outf, stderr=subprocess.STDOUT, env=env, timeout=4*3600) # 7 days is 168h
    except subprocess.TimeoutExpired:
        print(f"{prefix}-{job_base} Timeout detected.")
        pass 
    except Exception as e:
        print(f"{prefix}-{job_base} Exec Error: {e}")
        return
    if os.path.exists(current_gbw):
        try:
            subprocess.run(["orca_2mkl", job_base, "-mkl"], cwd=slot_dir, env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except: pass

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
    if is_success:
        shutil.copy2(current_out, os.path.join(FINAL_OUT_DIR, f"{job_base}.out"))
        if os.path.exists(mkl_path):
            shutil.copy2(mkl_path, os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl"))
        
        with lock: mark_completed(mol_id, method)
        print(f"{prefix}-{job_base} DONE.")
        for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
            try: os.remove(f)
            except: pass
    elif has_explicit_error:
        print(f"{prefix} FAILED (Explicit Error). Cleaning up for fresh restart.")
        
        # [MODIFIED] Dump tail -n 30 to stderr (which SLURM captures in .err file)
        try:
             sys.stderr.write(f"\n=== [Explicit Error Info] Last 30 lines of {job_base}.out ===\n")
             sys.stderr.flush()
             subprocess.run(["tail", "-n", "30", current_out], stdout=sys.stderr)
             sys.stderr.write("\n============================================================\n")
             sys.stderr.flush()
        except Exception as e:
             sys.stderr.write(f"Failed to tail error log: {e}\n")

        fail_log_path = os.path.join(FAILED_LOG_DIR, f"{job_base}_failed.out")
        try: shutil.copy2(current_out, fail_log_path)
        except: pass
        for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
            try: os.remove(f)
            except: pass
    else:
        print(f"{prefix} INTERRUPTED. Saving GBW for resume.")
        if os.path.exists(current_gbw):
            shutil.move(current_gbw, restart_gbw)
        for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
            if os.path.basename(f) == restart_gbw_name: continue 
            try: os.remove(f)
            except: pass

def worker(slot_id, cores_per_slot, task_queue, lock):
    slot_dir = get_slot_dir(slot_id)
    while True:
        try:
            task = task_queue.get(timeout=5) 
        except queue.Empty:
            break
        mol_id, method = task
        
        # [MODIFIED] Check file existence immediately
        job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
        final_out_check = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
        
        if os.path.exists(final_out_check):
             task_queue.task_done()
             continue
        # Run
        try:
            run_task((mol_id, method, cores_per_slot, slot_id), slot_dir, lock)
        except Exception as e:
            print(f"[Slot {slot_id}] Critical Error: {e}")
        task_queue.task_done()

def main():
    init_dirs_and_db()
    print("[Setup] identifying tasks...")
    all_tasks = []
    skipped_count = 0
    
    # [MODIFIED] Loop priorities file existence over DB
    for mol_id in range(START_ID, END_ID + 1):
        for method in METHODS:
            # We skip checking is_completed() from DB first
            
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            final_out = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
            
            # Check if file exists -> Skip if so
            if os.path.exists(final_out):
                skipped_count += 1
                continue
            
            # If not found, add to queue
            all_tasks.append((mol_id, method))
            
    print(f"[Pre-check] Skipped {skipped_count} jobs (files exist).")
    print(f"[Queue] Added {len(all_tasks)} tasks to queue.")

    slurm_cpus = os.environ.get('SLURM_NTASKS_PER_NODE')
    total_cores = int(slurm_cpus) if slurm_cpus else multiprocessing.cpu_count()
    cores_per_slot = total_cores // CONCURRENCY
    print(f"CONFIG: {total_cores} cores | {CONCURRENCY} slots | {cores_per_slot} cores/slot (Dynamic Queue)")
    
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
