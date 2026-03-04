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
import socket

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
                    help="Fixed path for this chunk to allow resume")

args = parser.parse_args()

START_ID    = args.start_id
END_ID      = args.end_id
CONCURRENCY = args.concurrency
BASIS       = args.basis
METHODS     = args.methods.split(',')
WORK_SUBDIR = args.work_subdir

# =============================================================================
# Setup Environment
# =============================================================================
WORK_ROOT = "/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole"
ORCA_FILES_BASE = os.path.join(WORK_SUBDIR, "orca_files")
methods_str = '_'.join(sorted(METHODS))

FINAL_OUT_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_out_{methods_str}_{BASIS}")
FINAL_MKL_DIR = os.path.join(WORK_ROOT, "orca_output", f"orca_mkl_{methods_str}_{BASIS}")
DB_FILE = os.path.join(WORK_ROOT, "checkpoints", f"checkpoint_{BASIS}_{methods_str}.db")

ORCA_HOME = "/lustre1/g/chem_yangjun/orca6.1.0/orca-6.1.0-f.0_linux_x86-64"
ORCA_BIN  = os.path.join(ORCA_HOME, "bin", "orca")
SOURCE_ROOT = "/lustre1/g/chem_yangjun/u3651388/osv_mp2_ml_gen/orca2pyscf/sources"

# ==============================================================================
# Database & Helper Functions
# ==============================================================================
def init_db():
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS completed (
            mol_id TEXT, method TEXT, done INTEGER DEFAULT 1,
            PRIMARY KEY (mol_id, method))''')
    conn.commit()
    conn.close()

def is_completed(mol_id, method):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM completed WHERE mol_id=? AND method=?", (str(mol_id), method))
    res = c.fetchone()
    conn.close()
    return res is not None

def mark_completed(mol_id, method):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO completed (mol_id, method, done) VALUES (?, ?, 1)", (str(mol_id), method))
    conn.commit()
    conn.close()

def parse_inp_filename(filename):
    base = os.path.basename(filename)
    if not base.startswith("dsgdb9nsd_") or not base.endswith(".inp"): return None
    parts = base[:-4].split('_')
    if len(parts) != 4: return None
    try: return int(parts[1]), parts[2]
    except: return None

def get_slot_dir(slot_id):
    return os.path.join(ORCA_FILES_BASE, f"slot_{slot_id}")

def ensure_dirs():
    os.makedirs(ORCA_FILES_BASE, exist_ok=True)
    os.makedirs(FINAL_OUT_DIR, exist_ok=True)
    os.makedirs(FINAL_MKL_DIR, exist_ok=True)
    for i in range(CONCURRENCY):
        os.makedirs(get_slot_dir(i), exist_ok=True)

# ==============================================================================
# SPE Restart Logic (不改坐标，只读波函数)
# ==============================================================================
def prepare_spe_restart_input(original_inp, restart_gbw_name):
    """保留 xyzfile，只插入 MOREAD 和 %moinp"""
    with open(original_inp, 'r') as f:
        lines = f.readlines()
    
    new_lines = []
    header_processed = False
    
    for line in lines:
        stripped = line.strip()
        # 处理 ! 行
        if stripped.startswith('!') and not header_processed:
            clean_line = re.sub(r'\s+MOREAD', '', line, flags=re.IGNORECASE).strip()
            new_lines.append(f"{clean_line} MOREAD\n")
            new_lines.append(f'%moinp "{restart_gbw_name}"\n')
            header_processed = True
        else:
            new_lines.append(line)
            
    with open(original_inp, 'w') as f:
        f.writelines(new_lines)

# ==============================================================================
# Task Execution
# ==============================================================================
def copy_inputs():
    slot_counter = 0
    copied = 0
    print(f"[Setup] Scanning tasks {START_ID}-{END_ID}...")
    for mol_id in range(START_ID, END_ID + 1):
        for method in METHODS:
            if is_completed(mol_id, method): continue
            
            inp_file = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
            src_path = os.path.join(SOURCE_ROOT, method, f"{BASIS}_{method}", inp_file)
            
            if not os.path.exists(src_path): continue
            
            slot_id = slot_counter % CONCURRENCY
            slot_counter += 1
            dst_path = os.path.join(get_slot_dir(slot_id), inp_file)
            
            if not os.path.exists(dst_path) or os.path.getsize(dst_path) == 0:
                shutil.copy2(src_path, dst_path)
                copied += 1
    print(f"[Setup] Distributed {copied} input files to {CONCURRENCY} slots.")

def run_task(task_info, slot_dir, lock):
    mol_id, method, cpu_range, nprocs, slot_id = task_info
    inp_name = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}.inp"
    work_inp = os.path.join(slot_dir, inp_name)
    job_base = inp_name.replace(".inp", "")
    
    # 关键文件路径
    current_out = os.path.join(slot_dir, f"{job_base}.out")
    current_gbw = os.path.join(slot_dir, f"{job_base}.gbw")
    restart_gbw = os.path.join(slot_dir, "temp_restart.gbw")
    
    prefix = f"[Slot {slot_id}|{mol_id}]"

    # --- 1. 检查断点 (Resume) ---
    is_restart = False
    if os.path.exists(current_gbw):
        print(f"{prefix} Found previous .gbw. Configuring SPE restart.")
        try:
            shutil.move(current_gbw, restart_gbw)
            prepare_spe_restart_input(work_inp, "temp_restart.gbw")
            is_restart = True
        except Exception as e:
            print(f"{prefix} Restart prep failed: {e}. Starting fresh.")
            if os.path.exists(restart_gbw): os.remove(restart_gbw)

    # --- 2. 清理临时文件 (保留 inp 和 restart.gbw) ---
    for f in glob.glob(os.path.join(slot_dir, f"{job_base}*")):
        abs_f = os.path.abspath(f)
        if abs_f == os.path.abspath(work_inp): continue
        if is_restart and os.path.basename(f) == "temp_restart.gbw": continue
        try: os.remove(f)
        except: pass

    if not os.path.exists(work_inp): return

    # --- 3. 设置 %pal ---
    with open(work_inp, 'r') as f: lines = f.readlines()
    with open(work_inp, 'w') as f:
        f.write(f"%pal nprocs {nprocs} end\n")
        for line in lines:
            if "%pal" not in line.lower(): f.write(line)

    # --- 4. 运行 ORCA ---
    env = os.environ.copy()
    env["PATH"] = f"{ORCA_HOME}/bin:{env.get('PATH','')}"
    env["LD_LIBRARY_PATH"] = f"{ORCA_HOME}/lib:{env.get('LD_LIBRARY_PATH','')}"
    env["OMPI_MCA_rmaps_base_oversubscribe"] = "true"

    # 使用 timeout 防止卡死，留 1 小时给 SLURM 清理
    
    skip_binding = os.environ.get("ORCA_SKIP_CPU_BIND", "0") == "1"
    if skip_binding:
        # 不使用 rankfile，直接依靠 OS 或 taskset
        cmd = ["taskset", "-c", cpu_range, "timeout", "23h", ORCA_BIN, work_inp]

    try:
        with open(current_out, "w") as outf:
            subprocess.run(cmd, stdout=outf, stderr=subprocess.STDOUT, env=env)
    except Exception as e:
        print(f"{prefix} Exec Error: {e}")
        return

    # --- 5. 结果处理 ---
    # 生成 mkl
    gbw_path = os.path.join(slot_dir, f"{job_base}.gbw")
    if os.path.exists(gbw_path):
        subprocess.run(["orca_2mkl", job_base, "-mkl"], cwd=slot_dir, env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    mkl_path = os.path.join(slot_dir, f"{job_base}.mkl")
    
    # 验证是否成功 (检查输出文件尾部)
    success = False
    if os.path.exists(current_out) and os.path.exists(mkl_path):
        try:
            with open(current_out, 'rb') as f:
                f.seek(-2048, 2)
                tail = f.read().decode(errors='ignore')
            if "ORCA TERMINATED NORMALLY" in tail:
                success = True
        except: pass

    if success:
        shutil.copy2(current_out, os.path.join(FINAL_OUT_DIR, f"{job_base}.out"))
        shutil.copy2(mkl_path, os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl"))
        with lock: mark_completed(mol_id, method)
        print(f"{prefix} DONE.")
        # 清理所有相关文件
        for f in [work_inp, restart_gbw, current_out, mkl_path, gbw_path]:
            if os.path.exists(f): os.remove(f)
    else:
        print(f"{prefix} INCOMPLETE. Files kept for resume.")

def worker(slot_id, cpu_range, cores_per_slot, lock):
    slot_dir = get_slot_dir(slot_id)
    # 持续循环到没有任务或时间到
    # 这里简单处理：扫描一次文件夹。如果需要一直跑，外层 bash 会控制重启。
    inps = sorted([f for f in os.listdir(slot_dir) if f.endswith('.inp')])
    for inp in inps:
        parsed = parse_inp_filename(inp)
        if not parsed: continue
        mol_id, method = parsed
        if is_completed(mol_id, method): continue
        run_task((mol_id, method, cpu_range, cores_per_slot, slot_id), slot_dir, lock)

# ==============================================================================
# Main
# ==============================================================================
def main():
    ensure_dirs()
    init_db()

    # Pre-scan (DB Sync)
    synced = 0
    for mol_id in range(START_ID, END_ID + 1):
        for method in METHODS:
            job_base = f"dsgdb9nsd_{mol_id:06d}_{method}_{BASIS}"
            if os.path.exists(os.path.join(FINAL_OUT_DIR, f"{job_base}.out")) and \
               os.path.exists(os.path.join(FINAL_MKL_DIR, f"{job_base}.mkl")):
                mark_completed(mol_id, method)
                synced += 1
    print(f"[DB] Synced {synced} completed jobs.")

    copy_inputs()

    # CPU Setup
    slurm_cpus = os.environ.get('SLURM_CPUS_PER_TASK')
    total_cores = int(slurm_cpus) if slurm_cpus else multiprocessing.cpu_count()
    
    # 这里的逻辑：如果 Intel 32核，Concurrency=2，则每个Slot 16核
    # 如果 AMD 128核，Concurrency=8，则每个Slot 16核
    cores_per_slot = total_cores // CONCURRENCY
    
    cpu_ranges = []
    for i in range(CONCURRENCY):
        s = i * cores_per_slot
        e = (i + 1) * cores_per_slot - 1
        cpu_ranges.append(f"{s}-{e}")

    print(f"CONFIG: {total_cores} cores | {CONCURRENCY} slots | {cores_per_slot} cores/slot")
    
    lock = multiprocessing.Lock()
    procs = []
    for i in range(CONCURRENCY):
        p = multiprocessing.Process(target=worker, args=(i, cpu_ranges[i], cores_per_slot, lock))
        p.start()
        procs.append(p)
    
    for p in procs: p.join()
    print("=== CHUNK DONE ===")

if __name__ == "__main__":
    main()