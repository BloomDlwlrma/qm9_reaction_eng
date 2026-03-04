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
import sqlite3   # ← 新增：SQLite 支持

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
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
    ORCA_FILES_BASE = os.path.join(args.work_subdir, "orca_files")
    print(f"[Isolation] Using job-specific ORCA_FILES_BASE: {ORCA_FILES_BASE}")
else:
    ORCA_FILES_BASE = os.path.join(WORK_ROOT, "orca_files", "orca_files_debug")
    print("Warning: No --work-subdir provided, using shared directory (risk of conflict)")

FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_out_{methods_str}_{BASIS}")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_mkl_{methods_str}_{BASIS}")

# ================ 新增：SQLite 資料庫設定 ================
DB_FILE = os.path.join(WORK_ROOT, "checkpoints", f"checkpoint_{BASIS}_{methods_str}.db")
# =======================================================

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
    os.makedirs(os.path.join(WORK_ROOT, "checkpoints"), exist_ok=True)
    
    for i in range(concurrency):
        os.makedirs(get_slot_dir(i), exist_ok=True)

# =============================================================================
# 新增：SQLite 初始化與核心函數（取代原本的 JSON checkpoint）
# =============================================================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS completed (
            mol_id TEXT,
            method TEXT,
            done INTEGER DEFAULT 1,
            PRIMARY KEY (mol_id, method)
        )
    ''')
    conn.commit()
    conn.close()
    print(f"[DB] SQLite 資料庫初始化完成：{DB_FILE}")

def is_completed(mol_id, method):
    """查詢是否已完成（純 SQLite 查詢）"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM completed WHERE mol_id=? AND method=?", (str(mol_id), method))
    exists = c.fetchone() is not None
    conn.close()
    return exists

def mark_completed(mol_id, method):
    """標記為已完成（原子性寫入）"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO completed (mol_id, method, done) VALUES (?, ?, 1)",
        (str(mol_id), method)
    )
    conn.commit()
    conn.close()

def print_progress(start_id, end_id):
    """使用 SQLite 計算進度（高效）"""
    total = (end_id - start_id + 1) * len(METHODS)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    placeholders = ','.join(['?'] * len(METHODS))
    query = f"""
        SELECT COUNT(*) 
        FROM completed 
        WHERE mol_id >= ? 
          AND mol_id <= ? 
          AND method IN ({placeholders})
    """
    params = [str(start_id), str(end_id)] + METHODS
    c.execute(query, params)
    done = c.fetchone()[0]
    conn.close()
    
    pct = (done / total) * 100 if total > 0 else 0
    print(f"Progress: {done}/{total} tasks done ({pct:.1f}%)")

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
# copy_inputs – round-robin to slots（移除 checkpoint 參數）
# =============================================================================
def copy_inputs_sequentially(start_id, end_id, concurrency):
    copied = 0
    skipped_completed = 0
    missing_source = 0
    total_potential = 0
    slot_counter = 0

    for mol_id in range(start_id, end_id + 1):
        for method in METHODS:
            total_potential += 1
            inp_filename = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"

            if is_completed(mol_id, method):   # ← 使用新 DB 函數
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
# cleanup_completed_inputs – per slot（移除 checkpoint 參數）
# =============================================================================
def cleanup_completed_inputs(concurrency):
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
            if is_completed(mol_id, method):   # ← 使用新 DB 函數
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
# get_cpu_ranges & create_rankfile（完全不變）
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
# run_task – 改用 mark_completed（最關鍵修改）
# =============================================================================
def run_task(task_info, slot_dir, lock):
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    inp_file = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    work_inp = os.path.join(slot_dir, inp_file)
    job_base = inp_file.replace(".inp", "")
    work_out = os.path.join(slot_dir, f"{job_base}.out")

    prefix = f"[Slot {slot_id} | {job_base}]"

    # Cleanup previous residues
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
        print(f"{prefix} START on cores {cpu_range}")

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
            print(f"{prefix} COPY ERROR: {e}")

    # 只有成功複製 .out 才標記完成（同時檢查 .mkl 存在）
    if copied_success and os.path.exists(final_out):
        mkl_final = os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl")
        if os.path.exists(mkl_final):
            with lock:                     # 保持 lock 確保多進程安全
                mark_completed(mol_id, method)
            print(f"{prefix} DONE (DB 已更新)")
            # 成功後刪除輸入文件
            try:
                os.remove(work_inp)
            except OSError as e:
                print(f"{prefix} Failed to remove input file: {e}")
        else:
            print(f"{prefix} WARNING: .mkl missing → not marked complete")
    else:
        print(f"{prefix} WARNING: Final .out not found → not marked complete")

    # Cleanup slot dir
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
# main（重點修改區）
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
    init_db()                                      # ← 新增：初始化 SQLite

    # ====================== Pre-scan 同步現有結果到 DB ======================
    print("Pre-scanning existing output files and syncing to DB...")
    synced = 0
    for mol_id in range(start_id, end_id + 1):
        for method in METHODS:
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            out_file = os.path.join(FINAL_OUT_DIR, f"{job_base}.out")
            mkl_file = os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl")
            if os.path.exists(out_file) and os.path.exists(mkl_file):
                mark_completed(mol_id, method)     # 單進程寫入，無需 lock
                synced += 1
    print(f"[Pre-scan] Synced {synced} completed jobs to SQLite")

    print_progress(start_id, end_id)

    cleanup_completed_inputs(concurrency)           # ← 移除 checkpoint 參數
    copy_inputs_sequentially(start_id, end_id, concurrency)  # ← 移除 checkpoint 參數

    # ────────────────────────────────────────────────
    #          核心數來源與分配邏輯（不變）
    # ────────────────────────────────────────────────
    slurm_cpus_str = os.environ.get('SLURM_CPUS_PER_TASK')
    if slurm_cpus_str is not None:
        total_cores = int(slurm_cpus_str)
        source = f"SLURM_CPUS_PER_TASK = {total_cores}"
    else:
        total_cores = multiprocessing.cpu_count()
        source = f"cpu_count() = {total_cores} (no SLURM env)"

    print(f"[Core detection] Using {source}")

    if concurrency > total_cores:
        print(f"Warning: concurrency ({concurrency}) > available cores ({total_cores}) → will oversubscribe")
        cores_per_slot = 1
    else:
        cores_per_slot = total_cores // concurrency

    nprocs = cores_per_slot
    print(f"[Allocation] {total_cores} cores ÷ {concurrency} slots = {nprocs} cores per ORCA task")

    cpu_ranges, _ = get_cpu_ranges(total_cores, concurrency)

    print(f"CONFIG: {total_cores} effective cores → {concurrency} slots × {nprocs} cores each")
    # ────────────────────────────────────────────────

    lock = multiprocessing.Lock()

    def worker(slot_id, cpu_range, nprocs, lock):
        slot_dir = get_slot_dir(slot_id)
        inp_files = sorted([f for f in os.listdir(slot_dir) if f.endswith('.inp')])
        print(f"[Slot {slot_id}] Found {len(inp_files)} pending tasks.")
        for inp_file in inp_files:
            parsed = parse_inp_filename(inp_file)
            if parsed is None:
                print(f"[Slot {slot_id}] Skipping invalid filename: {inp_file}")
                continue
            mol_id, method = parsed
            if not (start_id <= mol_id <= end_id):
                print(f"[Slot {slot_id}] Warning: {inp_file} out of range, skipping.")
                continue
            run_task((mol_id, method, cpu_range, nprocs, slot_id), slot_dir, lock)

    processes = []
    for i in range(concurrency):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], nprocs, lock))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print_progress(start_id, end_id)
    print("=== CHUNK DONE ===")
    print(f"[DB] 所有完成紀錄已安全儲存在：{DB_FILE}")

if __name__ == "__main__":
    main()
    # sqlite3 /scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/checkpoints/checkpoint_631gss_ccsdt.db "SELECT COUNT(*) FROM completed""
    '''
DB="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/checkpoints/checkpoint_631gss_ccsdt.db"
PREFIX="631gss-ccsdt"
TIMESTAMP=$(date +%Y%m%d%H%M)
COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM completed;")
echo "${PREFIX}-${TIMESTAMP}:${COUNT}" >> complete_num_631gss_ccsdt.txt
    '''

    '''
sqlite3 /scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole/checkpoints/checkpoint_631gss_ccsdt.db <<EOF > missing.txt
WITH all_mols AS (
  SELECT mol_id FROM (
    SELECT printf('%06d', value) AS mol_id 
    FROM generate_series(88001, 92000)
  )
),
all_combinations AS (
  SELECT mol_id, method 
  FROM all_mols 
  CROSS JOIN (SELECT 'b3lyp' AS method UNION SELECT 'pbe0' UNION SELECT 'ωb97x-d')
)
SELECT a.mol_id, a.method
FROM all_combinations a
LEFT JOIN completed c ON a.mol_id = c.mol_id AND a.method = c.method
WHERE c.mol_id IS NULL
ORDER BY a.mol_id, a.method;
EOF
    '''