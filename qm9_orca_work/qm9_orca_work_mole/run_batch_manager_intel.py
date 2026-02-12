#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
import glob
import multiprocessing
import time
from concurrent.futures import ThreadPoolExecutor

#################################################################################
# Setup Environment
#################################################################################
# Root directory where data comes from
SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/source_files"

# Working directory
WORK_ROOT = "/scr/u/u3651388/orcarun/qm9_orca_work_mole"
ORCA_FILES_DIR = os.path.join(WORK_ROOT, "orca_files")

# Output aggregation directories
FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_out")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_mkl")

# Scripts to run after calculation
SCRIPT_SUM_ERROUT = os.path.join(WORK_ROOT, "03_sum_errout.sh")
SCRIPT_ORCA2MKL = os.path.join(WORK_ROOT, "orca2mkl.sh")

# ORCA Environment
ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN = os.path.join(ORCA_HOME, "bin", "orca")

# Task settings
METHODS = ["mp2", "ccsd", "ccsdt"]
BASIS = "631gs"

# ================= Configuration End =================

def ensure_dirs():
    for d in [ORCA_FILES_DIR, FINAL_OUT_DIR, FINAL_MKL_DIR]:
        if not os.path.exists(d):
            os.makedirs(d, exist_ok=True)

def get_cpu_ranges(total_cores, num_slots):
    """
    Divides total cores into ranges for binding.
    Returns a list of strings like ["0-7", "8-15", ...].
    """
    cores_per_slot = total_cores // num_slots
    ranges = []
    for i in range(num_slots):
        start = i * cores_per_slot
        if i == num_slots - 1:
            end = total_cores - 1 # Give remainder to last slot
        else:
            end = (i + 1) * cores_per_slot - 1
        ranges.append(f"{start}-{end}")
    return ranges, cores_per_slot

def run_task(task_info):
    """
    Worker function to process a single calculation.
    """
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    
    mol_dir_name = f"dsgdb9nsd_{mol_id:06d}"
    source_dir = os.path.join(SOURCE_ROOT, mol_dir_name)
    
    # Construct filename e.g. dsgdb9nsd_000053_ccsd_631gs.inp
    basis_clean = BASIS # assuming filename uses '631gs' directly
    inp_filename = f"dsgdb9nsd_{mol_id:06d}_{method}_{basis_clean}.inp"
    source_inp_path = os.path.join(source_dir, inp_filename)
    
    # Target paths
    work_inp_path = os.path.join(ORCA_FILES_DIR, inp_filename)
    job_basename = inp_filename.replace(".inp", "")
    out_filename = f"{job_basename}.out"
    work_out_path = os.path.join(ORCA_FILES_DIR, out_filename)
    
    prefix = f"[Slot {slot_id} | {job_basename}]"

    if not os.path.exists(source_inp_path):
        print(f"{prefix} FAIL: Source file not found: {source_inp_path}")
        return

    # 1. Copy Input
    print(f"{prefix} Copying input...")
    shutil.copy2(source_inp_path, work_inp_path)
    
    # 2. Modify %pal nprocs
    # We strip existing %pal and append our own valid for the slot
    with open(work_inp_path, 'r') as f:
        lines = f.readlines()
    
    with open(work_inp_path, 'w') as f:
        f.write(f"%pal nprocs {nprocs} end\n") # Prepend the correct PAL
        for line in lines:
            if "%pal" not in line.lower():
                f.write(line)

    # 3. Run ORCA with core binding
    # Command: taskset -c <range> orca <input> > <output>
    # Note: We need to set ORCA PATHs or use full path
    env = os.environ.copy()
    env["PATH"] = f"{os.path.join(ORCA_HOME, 'bin')}:{env.get('PATH', '')}"
    env["LD_LIBRARY_PATH"] = f"{os.path.join(ORCA_HOME, 'lib')}:{env.get('LD_LIBRARY_PATH', '')}"
    
    cmd = ["taskset", "-c", cpu_range, ORCA_BIN, work_inp_path]
    
    print(f"{prefix} Starting calculation on cores {cpu_range}...")
    start_time = time.time()
    try:
        with open(work_out_path, "w") as outfile:
            subprocess.run(cmd, stdout=outfile, stderr=subprocess.STDOUT, env=env, check=False)
    except Exception as e:
        print(f"{prefix} Execution Error: {e}")
        return

    duration = time.time() - start_time
    print(f"{prefix} Finished in {duration:.1f}s")
    
    # 4. Post-processing & Error Check
    # Integrated error checking to replace 03_sum_errout.sh for this specific job
    # This ensures we check exactly the file we just generated
    error_patterns = [
        "aborting the run", "Error termination", "The MDCI module", "mdci_state.cpp",
        "orca_mdci_mpi", "not enough slots", "illegal state", "Segmentation fault",
        "Signal: Aborted"
    ]
    
    has_error = False
    err_content = []
    
    if os.path.exists(work_out_path):
        with open(work_out_path, 'r', errors='ignore') as f:
            content = f.read()
            for pat in error_patterns:
                if pat.lower() in content.lower():
                    has_error = True
                    err_content.append(f"Matched Error: {pat}")
                    # Grab last 25 lines
                    lines = content.splitlines()
                    err_content.append("\nLast 25 lines:")
                    err_content.extend(lines[-25:])
                    break
    
    if has_error:
        print(f"{prefix} ERROR DETECTED. Creating .err file.")
        err_filename = f"{job_basename}.err"
        work_err_path = os.path.join(ORCA_FILES_DIR, err_filename)
        with open(work_err_path, 'w') as f:
            f.write(f"Job: {job_basename}\n")
            f.write(f"Date: {time.ctime()}\n")
            f.write("\n".join(err_content))
    
    # Run orca2mkl if successful (or even if failed, sometimes GBW exists)
    gbw_file = os.path.join(ORCA_FILES_DIR, f"{job_basename}.gbw")
    if os.path.exists(gbw_file):
        subprocess.run(["orca_2mkl", job_basename, "-mkl"], cwd=ORCA_FILES_DIR, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # 5. Move Files
    # Preserve .out and .mkl
    mkl_file = os.path.join(ORCA_FILES_DIR, f"{job_basename}.mkl")
    
    if os.path.exists(work_out_path):
        shutil.copy2(work_out_path, os.path.join(FINAL_OUT_DIR, out_filename))
    
    if os.path.exists(mkl_file):
        shutil.copy2(mkl_file, os.path.join(FINAL_MKL_DIR, f"{job_basename}.mkl"))
    
    # Move everything back to source (excluding the ones we just backed up? User said "turn output to original folder... except keep out,mkl in repo")
    # Interpretation: Move ALL generated files to source folder, AND keep copies of out/mkl in repo.
    generated_files = glob.glob(os.path.join(ORCA_FILES_DIR, f"{job_basename}*"))
    for fpath in generated_files:
        fname = os.path.basename(fpath)
        shutil.move(fpath, os.path.join(source_dir, fname))

    print(f"{prefix} Completed.")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 run_batch_manager.py <START_ID> <END_ID> [CONCURRENCY]")
        sys.exit(1)
        
    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    
    # Determine concurrency
    total_cores = multiprocessing.cpu_count()
    if len(sys.argv) >= 4:
        concurrency = int(sys.argv[3])
    else:
        # Heuristic: 
        # MP2/CCSD on small molecules (QM9) isn't very parallel efficient on >8 cores.
        # On 32 core node, 4 jobs x 8 cores is reasonable.
        concurrency = 4 
    
    ensure_dirs()
    
    # Generate CPU Binding Ranges
    cpu_ranges, cores_per_slot = get_cpu_ranges(total_cores, concurrency)
    print(f"Configuration: {total_cores} Cores, {concurrency} Concurrent Jobs")
    print(f"Cores per Job: {cores_per_slot}")
    print(f"Binding Maps: {cpu_ranges}")
    print("="*60)
    
    # Generate Task Queue
    # We want to prioritize completing one molecule (all 3 methods) before moving to next?
    # Or just fill the queue.
    tasks = []
    slot_cycle = 0
    for mol_id in range(start_id, end_id + 1):
        for method in METHODS:
            # Assign a slot implementation detail: ThreadPoolExecutor doesn't strictly allow pinning a thread to a slot easily without queue management.
            # But we can just pass the "cpu_range" as an argument.
            # To ensure we don't oversubscribe specific cores, we need to carefully manage the pool.
            # Simple approach: The pool has N workers. We pass N distinct cpu_ranges.
            # But ThreadPoolExecutor threads pick tasks arbitrarily.
            # Better: Use a Queue and Workers manually, or just pass the range index % concurrency.
            # If we submit sequentially, task i gets range i % N. 
            # This is rough if task times vary wildy (one core range becomes idle while others busy).
            # Robust approach: Use a Queue where workers (pinned to cores) pull tasks.
            pass

    # Robust Worker-Queue Implementation
    task_queue = multiprocessing.Queue()
    
    # Fill Queue
    count = 0
    for mol_id in range(start_id, end_id + 1):
        for method in METHODS:
            task_queue.put((mol_id, method))
            count += 1
            
    # Add termination signals
    for _ in range(concurrency):
        task_queue.put(None)
        
    print(f"Queued {count} calculations.")

    # Define Worker Process
    def worker(slot_idx, cpu_range, nprocs):
        while True:
            item = task_queue.get()
            if item is None:
                break
            mol_id, method = item
            run_task((mol_id, method, cpu_range, nprocs, slot_idx))

    # Start Processes
    processes = []
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], cores_per_slot))
        p.start()
        processes.append(p)
        
    for p in processes:
        p.join()
        
    print("All tasks finished.")

if __name__ == "__main__":
    main()
