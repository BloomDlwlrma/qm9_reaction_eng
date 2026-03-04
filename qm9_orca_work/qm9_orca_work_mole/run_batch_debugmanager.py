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
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        # 可選：logging.FileHandler('orca_batch.log')
    ]
)

parser = argparse.ArgumentParser(description="ORCA batch runner with per-job isolation")
parser.add_argument("start_id", type=int)
parser.add_argument("end_id", type=int)
parser.add_argument("concurrency", type=int)
parser.add_argument("basis", type=str)
parser.add_argument("methods", type=str)
parser.add_argument("--work-subdir", type=str, default=None,
                    help="Unique working subdirectory for this job (recommended: contains SLURM_JOB_ID)")

args = parser.parse_args()

START_ID    = args.start_id
END_ID      = args.end_id
CONCURRENCY = args.concurrency
BASIS       = args.basis
METHODS     = args.methods.split(',')
# =============================================================================
# Setup Environment
# =============================================================================
WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
methods_str = '_'.join(sorted(METHODS))
if args.work_subdir:
    ORCA_FILES_BASE = os.path.join(args.work_subdir, "orca_files_debug")
    print(f"[Isolation] Using job-specific ORCA_FILES_BASE: {ORCA_FILES_BASE}")
else:
    # fallback（不推薦在多作業時使用）
    ORCA_FILES_BASE = os.path.join(WORK_ROOT, "orca_files", "orca_files_debug")
    print("Warning: No --work-subdir provided, using shared directory (risk of conflict)")
skip = int(os.environ.get("ORCA_SKIP_CPU_BIND", "0") == "1")
cpsstr = os.environ.get("CORES_PER_SOCKET", "16")
FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output_debug", f"orca_out_{methods_str}_{BASIS}_{skip}_{cpsstr}") # cannot use when method=mp2+ccsd+ccsdt
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output_debug", f"orca_mkl_{methods_str}_{BASIS}_{skip}_{cpsstr}")

CHECKPOINT_FILE = None

ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN  = os.path.join(ORCA_HOME, "bin", "orca")

SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources"
# =============================================================================
# Per-slot directory helper
# =============================================================================
def get_slot_dir(slot_id):
    return os.path.join(ORCA_FILES_BASE, f"slot_{slot_id}")

# =============================================================================
# ensure_dirs
# =============================================================================
def ensure_dirs(concurrency):
    os.makedirs(ORCA_FILES_BASE, exist_ok=True)
    os.makedirs(FINAL_OUT_DIR,   exist_ok=True)
    os.makedirs(FINAL_MKL_DIR,   exist_ok=True)
    os.makedirs(os.path.join(WORK_ROOT, "checkpoints_debug"), exist_ok=True)
    
    for i in range(concurrency):
        os.makedirs(get_slot_dir(i), exist_ok=True)

# =============================================================================
# Checkpoint functions
# =============================================================================
def init_checkpoint_file():
    global CHECKPOINT_FILE
    methods_str = "_".join(METHODS)
    CHECKPOINT_FILE = os.path.join(WORK_ROOT, "checkpoints_debug", f"checkpoint_{BASIS}_{methods_str}.json")
    print(f"[INFO] Checkpoint file: {CHECKPOINT_FILE}")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_checkpoint(checkpoint):
    tmp_file = CHECKPOINT_FILE + ".tmp"
    with open(tmp_file, 'w') as f:
        json.dump(checkpoint, f, indent=2)
    os.replace(tmp_file, CHECKPOINT_FILE)

def is_completed(mol_id, method, checkpoint):
    job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
    out_file = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
    if os.path.exists(out_file):
        return True
    return checkpoint.get(str(mol_id), {}).get(method, False)

def parse_inp_filename(filename):
    base = os.path.basename(filename)
    if not base.startswith("dsgdb9nsd_") or not base.endswith(".inp"):
        return None
    parts = base[:-4].split('_')
    if len(parts) != 4:
        return None
    try:
        mol_id = int(parts[1])
        method = parts[2]
        return mol_id, method
    except ValueError:
        return None

# =============================================================================
# copy_inputs – round-robin to slots
# =============================================================================
def copy_inputs_sequentially(start_id, end_id, checkpoint, concurrency):
    copied = 0
    skipped_completed = 0
    missing_source = 0
    total_potential = 0
    slot_counter = 0

    for mol_id in range(start_id, end_id + 1):
        for method in METHODS:
            total_potential += 1
            inp_filename = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"

            if is_completed(mol_id, method, checkpoint):
                skipped_completed += 1
                continue

            src_dir = os.path.join(SOURCE_ROOT, method, f"{BASIS}_{method}")
            src = os.path.join(src_dir, inp_filename)

            if not os.path.exists(src):
                missing_source += 1
                continue

            slot_id = slot_counter % concurrency
            slot_counter += 1

            dst_dir = get_slot_dir(slot_id)
            dst = os.path.join(dst_dir, inp_filename)

            shutil.copy2(src, dst)
            copied += 1

    print(f"[Copy] Total potential tasks: {total_potential}")
    print(f"[Copy] Skipped (completed): {skipped_completed}")
    print(f"[Copy] Missing source files: {missing_source}")
    print(f"[Copy] Actually copied: {copied} (distributed across {concurrency} slots)")
    return copied

# =============================================================================
# cleanup_completed_inputs – per slot
# =============================================================================
def cleanup_completed_inputs(checkpoint, concurrency):
    print("[Cleanup] Scanning for completed tasks to remove input files...")
    removed = 0

    for slot_id in range(concurrency):
        slot_dir = get_slot_dir(slot_id)
        if not os.path.exists(slot_dir):
            continue
        for f in os.listdir(slot_dir):
            if not f.endswith('.inp'):
                continue
            parsed = parse_inp_filename(f)
            if parsed is None:
                continue
            mol_id, method = parsed
            if is_completed(mol_id, method, checkpoint):
                job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
                inp_path = os.path.join(slot_dir, f)
                try:
                    os.remove(inp_path)
                    removed += 1
                except OSError as e:
                    print(f"[Cleanup] Error removing {f} in slot {slot_id}: {e}")

                for other in glob.glob(os.path.join(slot_dir, f"{job_base}.*")):
                    if other.endswith('.inp'):
                        continue
                    try:
                        os.remove(other)
                    except OSError:
                        pass

    print(f"[Cleanup] Removed {removed} completed input files.")

# =============================================================================
# get_cpu_ranges (unchanged)
# =============================================================================
def get_cpu_ranges(total_cores, num_slots):
    cores_per_slot = total_cores // num_slots
    ranges = []
    for i in range(num_slots):
        start = i * cores_per_slot
        end = total_cores - 1 if i == num_slots - 1 else (i + 1) * cores_per_slot - 1
        ranges.append(f"{start}-{end}")
    return ranges, cores_per_slot

def create_rankfile(slot_idx, cpu_range, nprocs):
    cores_per_socket_str = os.environ.get("CORES_PER_SOCKET", "16")
    try:
        CORES_PER_SOCKET = int(cores_per_socket_str)
    except ValueError:
        CORES_PER_SOCKET = 16
        print(f"Warning: CORES_PER_SOCKET invalid, using default 16")

    hostname = socket.gethostname()
    start = int(cpu_range.split('-')[0])
    socket_id = start // CORES_PER_SOCKET
    local_start = start % CORES_PER_SOCKET

    rankfile_path = os.path.join(ORCA_FILES_BASE, f"rankfile_slot{slot_idx}.txt")
    
    print(f"[Rankfile] Creating {rankfile_path} | socket={socket_id}, local_start={local_start}, nprocs={nprocs}")
    
    with open(rankfile_path, 'w') as f:
        for r in range(nprocs):
            core_on_socket = local_start + r
            f.write(f"rank {r}={hostname} slot={socket_id}:{core_on_socket}\n")
    
    return rankfile_path

# =============================================================================
# run_task – 強化版 checkpoint 更新，並在成功後刪除輸入文件
# =============================================================================
def run_task(task_info, slot_dir, checkpoint, lock):
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    inp_file = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    work_inp = os.path.join(slot_dir, inp_file)
    job_base = inp_file.replace(".inp", "")
    work_out = os.path.join(slot_dir, f"{job_base}.out")

    prefix = f"[Slot {slot_id} | {job_base}]"

    # Cleanup previous residues in this slot only
    for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
        if os.path.abspath(f) == os.path.abspath(work_inp):
            continue
        try:
            if os.path.isfile(f):
                os.remove(f)
        except OSError:
            pass

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

    env = os.environ.copy()
    env["PATH"] = f"{ORCA_HOME}/bin:{env.get('PATH','')}"
    env["LD_LIBRARY_PATH"] = f"{ORCA_HOME}/lib:{env.get('LD_LIBRARY_PATH','')}"
    env["OMPI_MCA_rmaps_base_oversubscribe"] = "true"
    env["OMPI_MCA_hwloc_base_binding_policy"] = "core"
    env["OMPI_MCA_rmaps_base_mapping_policy"] = "slot"

    skip_binding = os.environ.get("ORCA_SKIP_CPU_BIND", "0") == "1"

    if skip_binding:
        cmd = ["timeout", "4h", ORCA_BIN, work_inp]
        print(f"{prefix} START (no explicit binding)")
        rankfile = None
    else:
        rankfile = create_rankfile(slot_id, cpu_range, nprocs)
        cmd = ["taskset", "-c", cpu_range, "timeout", "4h", ORCA_BIN, work_inp]
        print(f"{prefix} START on cores {cpu_range}, cmd: {' '.join(cmd)}")

    start_t = time.time()
    try:
        with open(work_out, "w") as outf:
            subprocess.run(cmd, stdout=outf, stderr=subprocess.STDOUT, env=env, check=False)
    except Exception as e:
        print(f"{prefix} EXEC ERROR: {e}")
        return

    print(f"{prefix} FINISHED in {time.time()-start_t:.1f}s")

    # orca_2mkl + move results
    gbw = os.path.join(slot_dir, f"{job_base}.gbw")
    if os.path.exists(gbw):
        try:
            subprocess.run(["orca_2mkl", job_base, "-mkl"], cwd=slot_dir, env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            mkl = os.path.join(slot_dir, f"{job_base}.mkl")
            if os.path.exists(mkl):
                shutil.copy2(mkl, os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl"))
        except Exception as e:
            print(f"{prefix} MKL ERROR: {e}")

    final_out = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
    copied_success = False

    if os.path.exists(work_out):
        try:
            shutil.copy2(work_out, final_out)
            copied_success = True
        except Exception as e:
            print(f"{prefix} COPY ERROR: Failed to copy .out to final dir: {e}")

    # 強化檢查：只有最終 .out 存在，才更新 checkpoint 並刪除輸入文件
    if copied_success and os.path.exists(final_out):
        with lock:
            current_cp = load_checkpoint()
            mol_key = str(mol_id)
            if mol_key not in current_cp:
                current_cp[mol_key] = {}
            current_cp[mol_key][method] = True
            save_checkpoint(current_cp)
        print(f"{prefix} DONE (checkpoint updated)")
        # 成功後刪除輸入文件，避免重複處理
        try:
            os.remove(work_inp)
        except OSError as e:
            print(f"{prefix} Failed to remove input file: {e}")
    else:
        print(f"{prefix} WARNING: Final .out not found or copy failed → checkpoint NOT updated")

    # Cleanup slot dir (keep .inp if you want to debug failed jobs)
    for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
        if f.endswith('.inp'):
            continue
        try:
            if os.path.isfile(f):
                os.remove(f)
        except OSError:
            pass

    if rankfile and os.path.exists(rankfile):
        try:
            os.remove(rankfile)
        except OSError:
            pass

# =============================================================================
# print_progress (unchanged)
# =============================================================================
def print_progress(start_id, end_id, checkpoint):
    total = (end_id - start_id + 1) * len(METHODS)
    done = sum(1 for mid in range(start_id, end_id + 1) for m in METHODS if is_completed(mid, m, checkpoint))
    pct = (done / total) * 100 if total > 0 else 0
    print(f"Progress: {done}/{total} tasks done ({pct:.1f}%)")

# =============================================================================
# main (修改部分)
# =============================================================================
def main():
    global METHODS, BASIS
    if len(sys.argv) < 6:
        print("Usage: python this_script.py <START> <END> <CONCURRENCY> <BASIS> <METHODS>")
        sys.exit(1)

    start_id    = int(sys.argv[1])
    end_id      = int(sys.argv[2])
    concurrency = int(sys.argv[3])
    BASIS       = sys.argv[4]
    METHODS     = sys.argv[5].split(',')

    print(f"Configured for methods: {METHODS}, basis: {BASIS}")
    print(f"Range: {start_id}-{end_id}, Concurrency: {concurrency}")

    ensure_dirs(concurrency)
    init_checkpoint_file()
    checkpoint = load_checkpoint()

    # Pre-scan
    print("Pre-scanning existing output files...")
    updated = False
    for mol_id in range(start_id, end_id + 1):
        mol_key = str(mol_id)
        if mol_key not in checkpoint:
            checkpoint[mol_key] = {}
        for method in METHODS:
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            out_file = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
            mkl_file = os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl")
            is_done = os.path.exists(out_file) and os.path.exists(mkl_file)
            current = checkpoint[mol_key].get(method, False)
            if is_done and not current:
                checkpoint[mol_key][method] = True
                updated = True
                print(f"[Pre-scan] Found completed: {job_base}")
            elif not is_done and current:
                checkpoint[mol_key][method] = False
                updated = True
                print(f"[Pre-scan] Correction: {job_base} marked incomplete")

    if not updated:
        print("[Pre-scan] No update needed")

    print_progress(start_id, end_id, checkpoint)

    cleanup_completed_inputs(checkpoint, concurrency)
    copy_inputs_sequentially(start_id, end_id, checkpoint, concurrency)

    # ==================== 修改開始 ====================
    # 不再收集 tasks 列表，直接啟動每個 slot 的 worker 處理自己目錄下的所有 .inp 文件
    total_cores = multiprocessing.cpu_count()
    cpu_ranges, nprocs = get_cpu_ranges(total_cores, concurrency)
    print(f"CONFIG: {total_cores} cores → {concurrency} slots * {nprocs} cores")

    lock = multiprocessing.Lock()

    def worker(slot_id, cpu_range, nprocs, lock):
        slot_dir = get_slot_dir(slot_id)
        # 獲取該 slot 目錄下所有 .inp 文件（按名稱排序，使順序可預測）
        inp_files = sorted([f for f in os.listdir(slot_dir) if f.endswith('.inp')])
        print(f"[Slot {slot_id}] Found {len(inp_files)} pending tasks.")
        for inp_file in inp_files:
            parsed = parse_inp_filename(inp_file)
            if parsed is None:
                print(f"[Slot {slot_id}] Skipping invalid filename: {inp_file}")
                continue
            mol_id, method = parsed
            # 可選：再次檢查任務是否在範圍內（但複製時已保證）
            if not (start_id <= mol_id <= end_id):
                print(f"[Slot {slot_id}] Warning: {inp_file} out of range, skipping.")
                continue
            # 執行任務，注意傳入的 checkpoint 參數未使用，但保留接口
            run_task((mol_id, method, cpu_range, nprocs, slot_id), slot_dir, checkpoint, lock)

    processes = []
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], nprocs, lock))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
    # ==================== 修改結束 ====================

    # 全部完成後重新載入 checkpoint 打印最終進度
    final_checkpoint = load_checkpoint()
    print_progress(start_id, end_id, final_checkpoint)
    print("=== CHUNK DONE ===")

if __name__ == "__main__":
    main()