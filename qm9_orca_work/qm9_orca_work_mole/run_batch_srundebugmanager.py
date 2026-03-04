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

# =============================================================================
# 新增：从命令行参数读取本地 scratch 目录
# =============================================================================
if len(sys.argv) < 7:
    print("Usage: python script.py <START> <END> <CONCURRENCY> <BASIS> <METHODS> <LOCAL_SCRATCH>")
    sys.exit(1)

start_id = int(sys.argv[1])
end_id = int(sys.argv[2])
concurrency = int(sys.argv[3])
BASIS = sys.argv[4]
METHODS = sys.argv[5].split(',')
LOCAL_SCRATCH = sys.argv[6]  # 节点本地目录，由 SLURM 脚本传入

# ORCA 所有临时文件都放在本地 scratch
ORCA_FILES_DIR = os.path.join(LOCAL_SCRATCH, "orca_files")
os.makedirs(ORCA_FILES_DIR, exist_ok=True)

#################################################################################
# Setup Environment
#################################################################################
SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources"
WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
skip_binding = os.environ.get("ORCA_SKIP_CPU_BIND", "0") == "1"
if skip_binding:
    FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output_debug", "orca_out_1")
    FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output_debug", "orca_mkl_1")
else:
    FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output_debug", "orca_out_0")
    FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output_debug", "orca_mkl_0")
# Select ORCA Version 
ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN = os.path.join(ORCA_HOME, "bin", "orca")
CHECKPOINT_FILE = None
FIXED_CHECKPOINT_METHODS = "mp2_ccsd_ccsdt"

#################################################################################
# Main Loop and Functions
#################################################################################
def ensure_dirs():
    ''' 
    CHECKED: Only create if not exist, to avoid accidental deletion of existing data.
    '''
    for d in [ORCA_FILES_DIR, FINAL_OUT_DIR, FINAL_MKL_DIR]:
        os.makedirs(d, exist_ok=True)
    os.makedirs(os.path.join(WORK_ROOT, "checkpoints"), exist_ok=True)

# ===============================================================================
# Checkpointing Functions (Optional)
# ===============================================================================
def init_checkpoint_file():
    global CHECKPOINT_FILE
    CHECKPOINT_FILE = os.path.join(WORK_ROOT, "checkpoints", f"checkpoint_{BASIS}_{FIXED_CHECKPOINT_METHODS}.json")
    print(f"[INFO] Checkpoint file (fixed naming): {CHECKPOINT_FILE}")

def load_checkpoint():
    """Load or create checkpoint dict: {mol_id: {method: bool}}"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint, lock=None):
    """Atomic save checkpoint with lock"""
    # lock is managed by caller now
    tmp_file = CHECKPOINT_FILE + ".tmp"
    with open(tmp_file, 'w') as f:
        json.dump(checkpoint, f, indent=2)
    os.replace(tmp_file, CHECKPOINT_FILE)

def is_orca_out_successful(out_path, max_lines=50):
    if not os.path.exists(out_path):
        return False
    try:
        with open(out_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[-max_lines:]
        text = ''.join(lines).lower()
        success_patterns = [
            "Sum of individual times",
            "List of suggested additional citations",
            "List of optional additional citations",
            "Timings for individual modules:",
            "****ORCA TERMINATED NORMALLY****",
            "TOTAL RUN TIME"
        ]
        error_patterns = [
            "error termination",
            "abnormal termination",
            "killed",
            "segfault",
            "segmentation fault",
            "mpirun detected",
            "out of memory",
            "the job aborted",
            "execution failed",
            "aborting the run",
            "the mdci module",
            "not enough slots",
            "there are not enough slots available",
            "illegal state",
            "signal: aborted",
            "received signal",
            "non-zero exit code",
            "primary job",
            "End of error message",
            "aborted",
            "Invalid argument"
        ]
        has_success = any(p in text for p in success_patterns)
        has_error   = any(p in text for p in error_patterns)
        if has_error:
            return False
        if has_success:
            return True
        # No definite success or failure -> conservatively assume not successful
        return False
    except:
        return False

def is_completed(mol_id, method, checkpoint):
    """Check if task is done (out file exists or checkpoint says so)"""
    job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
    out_file = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
    
    if os.path.exists(out_file):
        return is_orca_out_successful(out_file)
    return checkpoint.get(str(mol_id), {}).get(method, False)

def parse_inp_filename(filename):
    """
    Parse input filename to extract mol_id and method.
    Expected format: dsgdb9nsd_041160_mp2_631gs.inp
    Returns (mol_id, method) or None if parsing fails.
    """
    base = os.path.basename(filename)
    if not base.startswith("dsgdb9nsd_") or not base.endswith(".inp"):
        return None
    # Remove extension and split
    parts = base[:-4].split('_')
    if len(parts) != 4:
        return None
    try:
        mol_id = int(parts[1])
        method = parts[2]
        # Optionally verify basis matches global BASIS
        # if parts[3] != BASIS: return None
        return mol_id, method
    except ValueError:
        return None

def cleanup_completed_inputs(checkpoint):
    """
    Scan ORCA_FILES_DIR for .inp files. For each, if the corresponding task is completed,
    delete the .inp file and all associated intermediate files (e.g., .gbw, .prop, etc.).
    """
    print("[Cleanup] Scanning for completed tasks to remove input files...")
    removed = 0
    for f in os.listdir(ORCA_FILES_DIR):
        if not f.endswith('.inp'):
            continue
        parsed = parse_inp_filename(f)
        if parsed is None:
            continue
        mol_id, method = parsed
        if is_completed(mol_id, method, checkpoint):
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            # Delete the .inp file
            inp_path = os.path.join(ORCA_FILES_DIR, f)
            try:
                os.remove(inp_path)
                removed += 1
                # print(f"[Cleanup] Removed {f} (task completed)")
            except OSError as e:
                print(f"[Cleanup] Error removing {f}: {e}")
            
            # Optionally delete other files for this job (e.g., .gbw, .prop, etc.)
            for other in glob.glob(os.path.join(ORCA_FILES_DIR, f"{job_base}.*")):
                if other.endswith('.inp'):
                    continue
                try:
                    os.remove(other)
                    print(f"[Cleanup] Removed intermediate {os.path.basename(other)}")
                except OSError:
                    pass
    print(f"[Cleanup] Removed {removed} completed input files.")

def copy_inputs_sequentially(start_id, end_id, checkpoint):
    """
    Copy input files from source to ORCA_FILES_DIR, but only for tasks that are NOT completed
    and within the specified molecule range. Also skip if destination already exists.
    Returns number of files copied.
    """
    copied = 0
    skipped_completed = 0
    missing_source = 0
    total_potential = 0

    for mol_id in range(start_id, end_id + 1):
        for method in METHODS:
            total_potential += 1
            inp_filename = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
            dst = os.path.join(ORCA_FILES_DIR, inp_filename)
            
            # Skip if task already completed
            if is_completed(mol_id, method, checkpoint):
                skipped_completed += 1
                continue

            # Source path
            src_dir = os.path.join(SOURCE_ROOT, method, f"{BASIS}_{method}")
            src = os.path.join(src_dir, inp_filename)

            if os.path.exists(src):
                shutil.copy2(src, dst)
                copied += 1
                # print(f"[Copy] Copied {inp_filename}")
            else:
                missing_source += 1
                print(f"[Copy] WARNING: Source missing for {inp_filename}")

    print(f"[Copy] Total potential tasks in range: {total_potential}")
    print(f"[Copy] Skipped (completed): {skipped_completed}")
    print(f"[Copy] Missing source files: {missing_source}")
    print(f"[Copy] Actually copied: {copied}")
    return copied > 0

# ===============================================================================
# CPU Binding Functions
# Create_rankfile: set CORES_PER_SOCKET based on partition (Intel/AMD/Hugemem) for optimal binding
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
    """
    Optimized rankfile creation. CORES_PER_SOCKET is read from SLURM script environment variable.
    """
    # Read from environment variable, default to 16 (Intel) if not set
    cores_per_socket_str = os.environ.get("CORES_PER_SOCKET", "16")
    try:
        CORES_PER_SOCKET = int(cores_per_socket_str)
    except ValueError:
        CORES_PER_SOCKET = 16
        print(f"Warning: CORES_PER_SOCKET environment variable invalid ('{cores_per_socket_str}'), using default value 16")

    hostname = socket.gethostname()
    start = int(cpu_range.split('-')[0])
    
    # Calculate socket_id and starting core within socket
    socket_id = start // CORES_PER_SOCKET
    local_start = start % CORES_PER_SOCKET

    rankfile_path = os.path.join(ORCA_FILES_DIR, f"rankfile_slot{slot_idx}.txt")
    
    print(f"[Rankfile] Creating {rankfile_path} | socket={socket_id}, local_start={local_start}, nprocs={nprocs}, CORES_PER_SOCKET={CORES_PER_SOCKET}")
    
    with open(rankfile_path, 'w') as f:
        for r in range(nprocs):
            core_on_socket = local_start + r
            # Standard OpenMPI rankfile format
            f.write(f"rank {r}={hostname} slot={socket_id}:{core_on_socket}\n")
    
    return rankfile_path

# ==============================================================================
# Main Task Function
# ==============================================================================
def run_task(task_info, checkpoint, lock):
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    inp_file = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    work_inp = os.path.join(ORCA_FILES_DIR, inp_file)
    job_base = inp_file.replace(".inp", "")
    work_out = os.path.join(ORCA_FILES_DIR, f"{job_base}.out")

    prefix = f"[Slot {slot_id} | {job_base}]" # Slot means the worker process handling this task
    
    # Cleanup any residue from previous failed runs for THIS specific job
    for f in glob.glob(os.path.join(ORCA_FILES_DIR, f"{job_base}*")):
         if os.path.abspath(f) == os.path.abspath(work_inp): continue
         try:
             if os.path.isfile(f): os.remove(f)
         except OSError: pass
    
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

    # Run ORCA with OpenMPI binding + timeout
    env = os.environ.copy()
    env["PATH"] = f"{ORCA_HOME}/bin:{env.get('PATH','')}"
    env["LD_LIBRARY_PATH"] = f"{ORCA_HOME}/lib:{env.get('LD_LIBRARY_PATH','')}"
    
    # Allow OpenMPI to oversubscribe slots (needed when running multiple mpirun instances in one SLURM allocation)
    env["OMPI_MCA_rmaps_base_oversubscribe"] = "true"
    # recommended binding settings for ORCA + OpenMPI (socket-aware, but allow oversubscription within socket if needed)
    env["OMPI_MCA_hwloc_base_binding_policy"] = "core"
    env["OMPI_MCA_rmaps_base_mapping_policy"] = "slot"

    # Check if we should skip binding (set by submit script)
    skip_binding = os.environ.get("ORCA_SKIP_CPU_BIND", "0") == "1"

    if skip_binding:
        cmd = ["timeout", "4h", ORCA_BIN, work_inp]
        print(f"{prefix} START on simple mode (no explicit binding)")
        rankfile = None
    else:
        # Create rankfile (NUMA-optimized)
        rankfile = create_rankfile(slot_id, cpu_range, nprocs)
        cmd = ["taskset", "-c", cpu_range, "timeout", "4h", ORCA_BIN, work_inp]
        print(f"{prefix} START on cores {cpu_range} (socket-aware), run command {' '.join(cmd)}")

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

    # ======================================================================
    # Check output for success and only then mark as completed in checkpoint.
    ## ======================================================================
    mkl_file = os.path.join(ORCA_FILES_DIR, f"{job_base}.mkl")
    mkl_final = os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl")
    out_final  = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")

    # only consider successful if .out indicates success AND .mkl exists with reasonable size (to avoid false positives from empty/failed runs)
    success = False
    if os.path.exists(out_final) and is_orca_out_successful(out_final):
        if os.path.exists(mkl_file) and os.path.getsize(mkl_file) > 2000:  # 2KB
            shutil.copy2(mkl_file, mkl_final)
            success = True

    if os.path.exists(work_out):
        shutil.copy2(work_out, out_final)

    # Cleanup all files for this job in ORCA_FILES_DIR to save space (including .inp, .gbw, .prop, etc.)
    for f in glob.glob(os.path.join(ORCA_FILES_DIR, f"{job_base}*")):
         if f.endswith('.inp'): continue
         try:
             if os.path.isfile(f): os.remove(f)
         except OSError: pass

    if rankfile and os.path.exists(rankfile):
        try:
            os.remove(rankfile)
        except OSError:
            pass
    # Strict check: only mark as completed if .out indicates success AND .mkl exists with reasonable size
    if success:
        with lock:
            current_cp = load_checkpoint()
            current_cp.setdefault(str(mol_id), {})[method] = True
            save_checkpoint(current_cp) 
        print(f"{prefix} DONE (valid .out + .mkl, checkpoint updated)")
    else:
        print(f"{prefix} WARNING: not marked as completed (.out invalid or .mkl missing/empty)")

def print_progress(start_id, end_id, checkpoint):
    total = (end_id - start_id + 1) * len(METHODS)
    done = sum(1 for mid in range(start_id, end_id + 1) for m in METHODS if is_completed(mid, m, checkpoint))
    pct = (done / total) * 100
    print(f"Progress: {done}/{total} tasks done ({pct:.1f}%)")

def main():
    global METHODS, BASIS
    if len(sys.argv) < 6:
        print("Usage: python run_batch_manager.py <START> <END> <CONCURRENCY> <BASIS> <METHODS>")
        print("  <METHODS> should be a comma-separated list, e.g., 'mp2,ccsd,ccsdt'")
        sys.exit(1)

    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    concurrency = int(sys.argv[3])
    BASIS = sys.argv[4]
    METHODS = sys.argv[5].split(',')

    print(f"Configured for methods: {METHODS}, basis: {BASIS}")
    print(f"Range: {start_id}-{end_id}, Concurrency: {concurrency}")

    ensure_dirs()
    init_checkpoint_file()  # Set checkpoint filename based on BASIS and METHODS
    checkpoint = load_checkpoint()

    # Step 0: Pre-scan existing .out and .mkl files to initialize checkpoint and correct any discrepancies
    print("Pre-scanning existing output files to initialize checkpoint...")
    updated = False
    out_dir = FINAL_OUT_DIR
    mkl_dir = FINAL_MKL_DIR
    for mol_id in range(start_id, end_id + 1):
        mol_key = str(mol_id)
        if mol_key not in checkpoint:
            checkpoint[mol_key] = {}
        for method in METHODS:
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            out_file = os.path.join(out_dir, f"{job_base}.out")
            mkl_file = os.path.join(mkl_dir, f"{job_base}.mkl")

            # check both .out and .mkl for a more robust completion check (you can adjust this logic as needed)
            is_done = (os.path.exists(out_file) and is_orca_out_successful(out_file) and
                       os.path.exists(mkl_file) and os.path.getsize(mkl_file) > 2000)
            
            current_status = checkpoint[mol_key].get(method, False)
            if is_done and not current_status:
                checkpoint[mol_key][method] = True
                updated = True
                print(f"[Pre-scan] Found completed: {job_base}")
            elif not is_done and current_status:
                # If checkpoint says completed but files are missing → mark as incomplete
                checkpoint[mol_key][method] = False
                updated = True
                print(f"[Pre-scan] Correction: {job_base} files missing, marked as incomplete")
    
    if updated:
        with multiprocessing.Lock():
            save_checkpoint(checkpoint)

    print_progress(start_id, end_id, checkpoint)
    # Step 1: Clean up input files for already completed tasks
    cleanup_completed_inputs(checkpoint)
    # Step 2: Copy missing input files for incomplete tasks (if source exists)
    copy_inputs_sequentially(start_id, end_id, checkpoint)
    # Step 3: Build task list based on existing .inp files within the range
    tasks = []
    for f in os.listdir(ORCA_FILES_DIR):
        if not f.endswith('.inp'):
            continue
        parsed = parse_inp_filename(f)
        if parsed is None:
            continue
        mol_id, method = parsed
        if start_id <= mol_id <= end_id:
            tasks.append((mol_id, method))

    if not tasks:
        print("No pending tasks found in the specified range. Exiting.")
        sys.exit(0)

    print(f"Found {len(tasks)} pending tasks based on existing input files.")

    total_cores = multiprocessing.cpu_count()
    cpu_ranges, nprocs = get_cpu_ranges(total_cores, concurrency)
    print(f"CONFIG: {total_cores} cores → {concurrency} slots * {nprocs} cores")
    print(f"Binding: {cpu_ranges}")

    # Queue tasks
    q = multiprocessing.Queue()
    for task in tasks:
        q.put(task)
    for _ in range(concurrency):
        q.put(None)  # sentinel

    def worker(sid, crange, np, lock):
        while True:
            item = q.get()
            if item is None:
                break
            run_task((item[0], item[1], crange, np, sid), checkpoint, lock)

    procs = []
    lock = multiprocessing.Lock()
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], nprocs, lock))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

    # Final progress
    print_progress(start_id, end_id, checkpoint)
    print("=== CHUNK DONE (checkpoint saved) ===")

if __name__ == "__main__":
    main()