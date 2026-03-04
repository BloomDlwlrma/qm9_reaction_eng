#!/usr/bin/env python3
"""
整理舊的 ORCA 輸出檔案，按 方法 + 基組 分類移動到新的目錄結構
用法範例：
    python migrate_old_outputs.py --source /path/to/old/orca_out_0 --dry-run
    python migrate_old_outputs.py --source /path/to/old1 --source /path/to/old2
"""

import os
import shutil
import argparse
from collections import defaultdict

def parse_filename(filename):
    """
    從檔名解析 mol_id, method, basis
    預期格式: dsgdb9nsd_XXXXXX_{method}_{basis}.out / .mkl
    返回 (mol_id, method, basis) 或 None
    """
    base = os.path.splitext(filename)[0]
    if not base.startswith("dsgdb9nsd_"):
        return None
    parts = base.split('_')
    if len(parts) != 4:
        return None
    try:
        mol_id = int(parts[1])
        method = parts[2]
        basis = parts[3]
        return mol_id, method, basis
    except (ValueError, IndexError):
        return None


def migrate_files(source_dirs, root_output_dir, dry_run=True):
    moved_count = defaultdict(int)
    skipped_count = 0
    error_count = 0

    for src_dir in source_dirs:
        if not os.path.isdir(src_dir):
            print(f"[SKIP] 來源目錄不存在: {src_dir}")
            continue

        print(f"\n處理來源目錄: {src_dir}")

        for filename in os.listdir(src_dir):
            full_path = os.path.join(src_dir, filename)
            if not os.path.isfile(full_path):
                continue

            parsed = parse_filename(filename)
            if parsed is None:
                skipped_count += 1
                continue

            mol_id, method, basis = parsed
            methods_str = method   # 如果單一方法；若有多方法需調整

            if filename.endswith('.out'):
                target_dir = os.path.join(
                    root_output_dir,
                    "orca_output",
                    f"orca_out_{methods_str}_{basis}"
                )
                file_type = "out"
            elif filename.endswith('.mkl'):
                target_dir = os.path.join(
                    root_output_dir,
                    "orca_output",
                    f"orca_mkl_{methods_str}_{basis}"
                )
                file_type = "mkl"
            else:
                skipped_count += 1
                continue

            os.makedirs(target_dir, exist_ok=True)
            target_path = os.path.join(target_dir, filename)

            if os.path.exists(target_path):
                print(f"[SKIP 已存在] {filename} → {target_path}")
                skipped_count += 1
                continue

            action = "會移動" if not dry_run else "預覽移動"
            print(f"[{action}] {filename} → {target_path}")

            if not dry_run:
                try:
                    shutil.move(full_path, target_path)
                    moved_count[file_type] += 1
                except Exception as e:
                    print(f"[錯誤] 移動失敗 {filename}: {e}")
                    error_count += 1

    print("\n=== 總結 ===")
    print(f"移動 .out 檔案 : {moved_count['out']}")
    print(f"移動 .mkl 檔案 : {moved_count['mkl']}")
    print(f"略過（已存在/格式不符）: {skipped_count}")
    print(f"發生錯誤 : {error_count}")


def main():
    parser = argparse.ArgumentParser(description="遷移舊 ORCA 輸出到新結構")
    parser.add_argument("--source", action="append", required=True,
                        help="來源目錄，可多次指定，例如 --source dir1 --source dir2")
    parser.add_argument("--root", default="/scr/u/u3651388/qm9_reaction_eng/qm9_orca_work/qm9_orca_work_mole",
                        help="WORK_ROOT 根目錄")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="只預覽，不實際移動 (預設開啟)")

    args = parser.parse_args()

    print("=== ORCA 舊檔案遷移工具 ===")
    print(f"根目錄     : {args.root}")
    print(f"來源目錄   : {args.source}")
    print(f"模式       : {'Dry-run (預覽)' if args.dry_run else '實際移動'}")
    print("-" * 50)

    if input("\n確認要繼續？ (y/n): ").strip().lower() != 'y':
        print("已取消。")
        return

    migrate_files(args.source, args.root, dry_run=args.dry_run)


if __name__ == "__main__":
    main()