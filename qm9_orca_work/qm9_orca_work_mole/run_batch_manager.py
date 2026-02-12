#!/usr/bin/env python3
from itertools import count
import os
import sys
import shutil
import subprocess
import glob
import multiprocessing
import time
import json
import socket

from numpy import rint # For hostname in rankfile

#################################################################################
# Setup Environment
#################################################################################
SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/source_files"
WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
ORCA_FILES_DIR = os.path.join(WORK_ROOT, "orca_files")
FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_out")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_mkl")
CHECKPOINT_FILE = os.path.join(WORK_ROOT, "checkpoint.json")

# Select ORCA Version 
ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN = os.path.join(ORCA_HOME, "bin", "orca")

# Task settings
METHODS = ["mp2", "ccsd", "ccsdt"]
BASIS = "631gs"
#################################################################################
# Main Loop and Functions
#################################################################################
def ensure_dirs():
    ''' 
    CHECKED: Only create if not exist, to avoid accidental deletion of existing data.
    '''
    for d in [ORCA_FILES_DIR, FINAL_OUT_DIR, FINAL_MKL_DIR]:
        os.makedirs(d, exist_ok=True)

# ===============================================================================
# Checkpointing Functions (Optional)
# ===============================================================================
def load_checkpoint():
    """Load or create checkpoint dict: {mol_id: {method: bool}}"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint):
    """Atomic save checkpoint"""
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

def is_completed(mol_id, method, checkpoint):
    """Check if task is done (out file exists or checkpoint says so)"""
    mol_dir = os.path.join(SOURCE_ROOT, f"dsgdb9nsd_{mol_id:06d}")
    out_file = os.path.join(mol_dir, f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.out")
    if os.path.exists(out_file):
        return True
    return checkpoint.get(str(mol_id), {}).get(method, False)

# ===============================================================================
# CPU Binding Functions
# ===============================================================================
def get_cpu_ranges(total_cores, num_slots):
    ''' 
    FUNCTION CHECKED: Divides total cores into contiguous ranges for binding.
    Returns a list of strings like ["0-7", "8-15", ...] and the number of cores per slot.
    '''
    cores_per_slot = total_cores // num_slots
    ranges = []
    for i in range(num_slots):
        start = i * cores_per_slot
        end = total_cores - 1 if i == num_slots - 1 else (i + 1) * cores_per_slot - 1
        ranges.append(f"{start}-{end}")
    return ranges, cores_per_slot

def create_rankfile(slot_idx, cpu_range, nprocs):
    """Optimized rankfile: bind to exact socket + local cores (Intel 2-socket)"""
    hostname = socket.gethostname()
    start = int(cpu_range.split('-')[0])
    socket_id = start // 16          # 16 cores per socket on your Intel nodes
    local_start = start % 16

    rankfile_path = os.path.join(ORCA_FILES_DIR, f"rankfile_slot{slot_idx}.txt")
    with open(rankfile_path, 'w') as f:
        for r in range(nprocs):
            f.write(f"rank {r}={hostname} slot={socket_id}:{local_start + r}\n")
    return rankfile_path

def copy_inputs_sequentially(start_id, end_id):
    """Sequential copy 3 inp per molecule"""
    copied = 0
    for mol_id in range(start_id, end_id + 1):
        mol_dir = os.path.join(SOURCE_ROOT, f"dsgdb9nsd_{mol_id:06d}")
        if not os.path.isdir(mol_dir):
            continue
        for method in ["mp2", "ccsd", "ccsdt"]:
            inp = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
            src = os.path.join(mol_dir, inp)
            dst = os.path.join(ORCA_FILES_DIR, inp)
            if os.path.exists(src):
                shutil.copy2(src, dst)
                copied += 1
    print(f"✓ Copied {copied} input files (sequential)")
    return copied > 0

# ==============================================================================
# Main Task Function
# ==============================================================================
def run_task(task_info, checkpoint):
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    mol_dir = os.path.join(SOURCE_ROOT, f"dsgdb9nsd_{mol_id:06d}")
    inp_file = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    work_inp = os.path.join(ORCA_FILES_DIR, inp_file)
    job_base = inp_file.replace(".inp", "")
    work_out = os.path.join(ORCA_FILES_DIR, f"{job_base}.out")

    prefix = f"[Slot {slot_id} | {job_base}]" # Slot means the worker process handling this task
    if not os.path.exists(work_inp):
        print(f"{prefix} SKIP: inp missing")
        return

    # Modify %pal
    with open(work_inp, 'r') as f:
        lines = f.readlines()
    with open(work_inp, 'w') as f:
        f.write(f"%pal nprocs {nprocs} end\n")
        for line in lines:
            if "%pal" not in line.lower():
                f.write(line)

    # Create rankfile (NUMA-optimized)
    rankfile = create_rankfile(slot_id, cpu_range, nprocs)

    # Run ORCA with OpenMPI binding + timeout
    env = os.environ.copy()
    env["PATH"] = f"{ORCA_HOME}/bin:{env.get('PATH','')}"
    env["LD_LIBRARY_PATH"] = f"{ORCA_HOME}/lib:{env.get('LD_LIBRARY_PATH','')}"

    cmd = [
        "timeout", "4h", ORCA_BIN, work_inp,
        "--bind-to", "core",
        "--map-by", "core",
        "-rf", rankfile
    ]

    print(f"{prefix} START on cores {cpu_range} (socket-aware)")
    start_t = time.time()
    try:
        with open(work_out, "w") as outf:
            subprocess.run(cmd, stdout=outf, stderr=subprocess.STDOUT, env=env, check=False)
    except Exception as e:
        print(f"{prefix} EXEC ERROR: {e}")
        return

    print(f"{prefix} FINISHED in {time.time()-start_t:.1f}s")

    # Single-job orca_2mkl + backup out/mkl
    gbw = os.path.join(ORCA_FILES_DIR, f"{job_base}.gbw")
    if os.path.exists(gbw):
        try:
            subprocess.run(["orca_2mkl", job_base, "-mkl"], cwd=ORCA_FILES_DIR, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            mkl = os.path.join(ORCA_FILES_DIR, f"{job_base}.mkl")
            if os.path.exists(mkl):
                shutil.copy2(mkl, os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl"))
        except subprocess.CalledProcessError as e:
            print(f"{prefix} MKL ERROR: {e.stderr.decode()}")

    if os.path.exists(work_out):
        shutil.copy2(work_out, os.path.join(FINAL_OUT_DIR, f"{job_base}.out"))

    # Move ALL files back to original molecule folder
    for f in glob.glob(os.path.join(ORCA_FILES_DIR, f"{job_base}*")):
        if not f.endswith(".txt"):  # skip rankfile
            shutil.move(f, os.path.join(mol_dir, os.path.basename(f)))

    os.remove(rankfile)

    # Update checkpoint
    mol_key = str(mol_id)
    if mol_key not in checkpoint:
        checkpoint[mol_key] = {}
    checkpoint[mol_key][method] = True
    save_checkpoint(checkpoint)
    print(f"{prefix} DONE (checkpoint updated)")

def print_progress(start_id, end_id, checkpoint):
    total = (end_id - start_id + 1) * 3
    done = sum(1 for mid in range(start_id, end_id + 1) for m in METHODS if is_completed(mid, m, checkpoint))
    pct = (done / total) * 100
    print(f"Progress: {done}/{total} tasks done ({pct:.1f}%)")

def main():
    if len(sys.argv) < 3:
        print("Usage: python run_batch_manager.py <START> <END> [CONCURRENCY]")
        sys.exit(1)

    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    concurrency = int(sys.argv[3]) if len(sys.argv) >= 4 else 4

    ensure_dirs()
    checkpoint = load_checkpoint()
    print_progress(start_id, end_id, checkpoint)
    if not copy_inputs_sequentially(start_id, end_id, checkpoint):
        print("All tasks completed in range, skipping.")
        sys.exit(0)

    total_cores = multiprocessing.cpu_count()
    cpu_ranges, nprocs = get_cpu_ranges(total_cores, concurrency)
    print(f"CONFIG: {total_cores} cores → {concurrency} slots * {nprocs} cores")
    print(f"Binding: {cpu_ranges}")

    # Queue (per-molecule order)
    q = multiprocessing.Queue()
    count = 0
    for mol in range(start_id, end_id + 1):
        for m in ["mp2", "ccsd", "ccsdt"]:
            q.put((mol, m))
            count += 1
    for _ in range(concurrency):
        q.put(None)
    print(f"Queued {count} pending tasks.")
    
    def worker(sid, crange, np):
        while True:
            item = q.get()
            if item is None: break
            run_task((item[0], item[1], crange, np, sid), checkpoint)

    procs = []
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], nprocs))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

    # Final batch error check
    print_progress(start_id, end_id, checkpoint)
    subprocess.run([os.path.join(WORK_ROOT, "03_sum_errout.sh")], cwd=SOURCE_ROOT, shell=True)
    print("=== CHUNK DONE (checkpoint saved) ===")

if __name__ == "__main__":
    main()