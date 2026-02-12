#!/usr/bin/env python3
import os
import sys
import shutil
import subprocess
import glob
import multiprocessing
import time

# ============================ CONFIG ============================
SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/source_files"
WORK_ROOT = "/scr/u/u3651388/orcarun/qm9_orca_work_mole"
ORCA_FILES_DIR = os.path.join(WORK_ROOT, "orca_files")
FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_out")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", "orca_mkl")

ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN = os.path.join(ORCA_HOME, "bin", "orca")

METHODS = ["mp2", "ccsd", "ccsdt"]
BASIS = "631gs"
# ============================================================

def ensure_dirs():
    for d in [ORCA_FILES_DIR, FINAL_OUT_DIR, FINAL_MKL_DIR]:
        os.makedirs(d, exist_ok=True)

def get_node_config():
    """self-tuning slurm node CPU count and set concurrency accordingly"""
    total_cores = int(os.environ.get('SLURM_CPUS_ON_NODE', multiprocessing.cpu_count()))
    if total_cores >= 96:
        concurrency = 10          # AMD 128 cores recommended 10~12
        cores_per_slot = total_cores // concurrency
    elif total_cores >= 64:
        concurrency = 8
        cores_per_slot = total_cores // concurrency
    else:
        concurrency = 4           # Intel 32 cores
        cores_per_slot = total_cores // concurrency
    return total_cores, concurrency, cores_per_slot

def create_rankfile(slot_idx, cpu_range, nprocs):
    """Optimized rankfile: bind each rank to a single core (highest efficiency)"""
    rankfile_path = os.path.join(ORCA_FILES_DIR, f"rankfile_slot{slot_idx}.txt")
    hostname = os.uname()[1] or "localhost"
    
    with open(rankfile_path, 'w') as f:
        start = int(cpu_range.split('-')[0])
        for rank in range(nprocs):
            core = start + rank
            f.write(f"rank {rank}={hostname} slot=0:{core}\n")
    return rankfile_path

def run_task(task_info):
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    mol_dir_name = f"dsgdb9nsd_{mol_id:06d}"
    source_dir = os.path.join(SOURCE_ROOT, mol_dir_name)
    prefix = f"[Slot {slot_id} | {mol_id:06d}_{method}]"

    inp_filename = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    work_inp_path = os.path.join(ORCA_FILES_DIR, inp_filename)
    job_basename = inp_filename.replace(".inp", "")
    work_out_path = os.path.join(ORCA_FILES_DIR, f"{job_basename}.out")

    if not os.path.exists(work_inp_path):
        print(f"{prefix} SKIP: Input not found")
        return

    # 1. set nprocs in input file (important for AMD scaling)
    with open(work_inp_path, 'r') as f:
        lines = f.readlines()
    with open(work_inp_path, 'w') as f:
        f.write(f"%pal nprocs {nprocs} end\n")
        for line in lines:
            if "%pal" not in line.lower():
                f.write(line)

    # 2. Create precise rankfile
    rankfile = create_rankfile(slot_id, cpu_range, nprocs)

    # 3. Run ORCA (strongest binding method)
    env = os.environ.copy()
    cmd = [
        ORCA_BIN, work_inp_path,
        "--bind-to", "core",
        "--map-by", "core",
        "-rf", rankfile,
        "--report-bindings"
    ]

    print(f"{prefix} START → Cores: {cpu_range}  (nprocs={nprocs})")
    start = time.time()
    with open(work_out_path, "w") as f:
        subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, env=env)

    print(f"{prefix} DONE in {time.time()-start:.1f}s")

    # 4. Post-process: orca_2mkl + Backup + Move
    gbw = os.path.join(ORCA_FILES_DIR, f"{job_basename}.gbw")
    if os.path.exists(gbw):
        subprocess.run(["orca_2mkl", job_basename, "-mkl"], cwd=ORCA_FILES_DIR, env=env)

    # Copy out and mkl to aggregation directories (important!)
    mkl_file = os.path.join(ORCA_FILES_DIR, f"{job_basename}.mkl")
    if os.path.exists(work_out_path):
        shutil.copy2(work_out_path, os.path.join(FINAL_OUT_DIR, f"{job_basename}.out"))
    if os.path.exists(mkl_file):
        shutil.copy2(mkl_file, os.path.join(FINAL_MKL_DIR, f"{job_basename}.mkl"))

    # 5. Move all files back to the original molecule folder
    for fpath in glob.glob(os.path.join(ORCA_FILES_DIR, f"{job_basename}*")):
        shutil.move(fpath, os.path.join(source_dir, os.path.basename(fpath)))

    os.remove(rankfile)  # Clean up temporary files
    print(f"{prefix} Completed & Moved\n")

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 run_batch_manager.py <START_ID> <END_ID> [CONCURRENCY]")
        sys.exit(1)

    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    total_cores, concurrency, cores_per_slot = get_node_config()

    print(f"Node Detected: {total_cores} Cores → Concurrency = {concurrency} jobs")
    print(f"Cores per Job = {cores_per_slot}")

    ensure_dirs()

    # Sequentially generate task queue (by molecule ID)
    task_queue = multiprocessing.Queue()
    count = 0
    for mol_id in range(start_id, end_id + 1):
        for method in ["mp2", "ccsd", "ccsdt"]:
            task_queue.put((mol_id, method))
            count += 1
    for _ in range(concurrency):
        task_queue.put(None)

    # Start Worker
    def worker(i, cpu_range, nprocs):
        while True:
            item = task_queue.get()
            if item is None: break
            run_task((*item, cpu_range, nprocs, i))

    processes = []
    cpu_ranges = [f"{i*cores_per_slot}-{(i+1)*cores_per_slot-1}" for i in range(concurrency)]
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], cores_per_slot))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("All calculations completed successfully!")

if __name__ == "__main__":
    main()