#!/usr/bin/env python3
"""Extract ORCA pert. triples progress lines into a CSV.

This script targets output blocks like:
  10% done  Triple   4   3   2 : current eijk=    -2.48 ET(i,j,k)= -0.0005 ET= -0.0012

It writes one CSV row per "Triple i j k" line.

Typical usage:
  python3 scripts/04_extract_triples_correction.py \
    qm9_orca_work/qm9_orca_work_molecu/water.out \
    --output qm9_out.csv --mode write

For batch processing (append mode):
  python3 scripts/04_extract_triples_correction.py qm9_orca_work/qm9_orca_work_molecu/*.out

For downstream analysis (reduced output; sum over k):
    python3 scripts/_extract_triples_correction.py qm9_orca_work/qm9_orca_work_molecu/*.out \
        --pair-sum --output qm9_pair_sum.csv --mode write

Optionally, restrict to the latest batch per file:
    python3 scripts/_extract_triples_correction.py qm9_orca_work/qm9_orca_work_molecu/*.out \
        --pair-sum --latest-batch-only --output qm9_pair_sum_latest.csv --mode write

The reduced CSV contains columns: qm9_index,i,j,k,et_ijk (k is -1 meaning "summed over all k").
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional


_TRIPLE_LINE_RE = re.compile(
    r"^\s*(?:(?P<pct>\d+)%\s+done\s+)?"
    r"Triple\s+(?P<i>\d+)\s+(?P<j>\d+)\s+(?P<k>\d+)\s*:"
    r"\s*current eijk=\s*(?P<eijk>[-+0-9.Ee]+)"
    r"\s+ET\(i,j,k\)=\s*(?P<et_ijk>[-+0-9.Ee]+)"
    r"\s+ET=\s*(?P<et_cum>[-+0-9.Ee]+)\s*$"
)

_IBATCH_RE = re.compile(r"^\s*Ibatch:\s*(?P<batch>\d+)\s*$")


@dataclass(frozen=True)
class TripleRecord:
    source_file: str
    batch: Optional[int]
    i: int
    j: int
    k: int
    eijk: float
    et_ijk: float
    et_cum: float


def _iter_triples_from_file(path: Path) -> Iterator[TripleRecord]:
    batch: Optional[int] = None

    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                batch_match = _IBATCH_RE.match(line)
                if batch_match:
                    try:
                        batch = int(batch_match.group("batch"))
                    except Exception:
                        batch = None
                    continue

                m = _TRIPLE_LINE_RE.match(line)
                if not m:
                    continue

                i = int(m.group("i"))
                j = int(m.group("j"))
                k = int(m.group("k"))
                if i > j:
                    i, j = j, i

                yield TripleRecord(
                    source_file=str(path),
                    batch=batch,
                    i=i,
                    j=j,
                    k=k,
                    eijk=float(m.group("eijk")),
                    et_ijk=float(m.group("et_ijk")),
                    et_cum=float(m.group("et_cum")),
                )
                
    except FileNotFoundError:
        return


def _expand_inputs(inputs: list[str]) -> list[Path]:
    paths: list[Path] = []
    for raw in inputs:
        # argparse with glob expanded by shell for most shells, but be robust.
        if any(ch in raw for ch in "*?["):
            paths.extend(sorted(Path(p) for p in Path().glob(raw)))
        else:
            paths.append(Path(raw))
    # de-duplicate while preserving order
    seen: set[Path] = set()
    uniq: list[Path] = []
    for p in paths:
        rp = p
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(rp)
    return uniq


def _write_csv(records: Iterable[TripleRecord], output_path: Path, mode: str) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    write_header = True
    file_mode = "w" if mode == "write" else "a"
    if mode == "append" and output_path.exists() and output_path.stat().st_size > 0:
        write_header = False

    count = 0
    with output_path.open(file_mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source_file",
                "batch",
                "i",
                "j",
                "eijk",
                "et_ijk",
                "et_cum",
            ],
        )
        if write_header:
            writer.writeheader()

        for r in records:
            writer.writerow(
                {
                    "source_file": r.source_file,
                    "batch": r.batch,
                    "i": r.i,
                    "j": r.j,
                    "eijk": f"{r.eijk:.15g}",
                    "et_ijk": f"{r.et_ijk:.15g}",
                    "et_cum": f"{r.et_cum:.15g}",
                }
            )
            count += 1

    return count


def _write_df(records: Iterable[TripleRecord]):
    """Convert records to a pandas DataFrame sorted by i then j.

    This is meant for downstream analysis workflows.
    """

    try:
        import pandas as pd  # type: ignore
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("pandas is required for _write_df(). Install pandas and retry.") from e

    rows = [
        {
            "source_file": r.source_file,
            "batch": r.batch,
            "i": r.i,
            "j": r.j,
            "k": r.k,
            "eijk": r.eijk,
            "et_ijk": r.et_ijk,
            "et_cum": r.et_cum,
        }
        for r in records
    ]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["i", "j"], kind="mergesort").reset_index(drop=True)
    return df


# def _latest_batch_only(records: Iterable[TripleRecord]) -> list[TripleRecord]:
    """Filter records to keep only the latest batch per source file.

    If a file has no batch markers (batch is always None), all records are kept.
    """

    items = list(records)
    latest: dict[str, Optional[int]] = {}
    for r in items:
        cur = latest.get(r.source_file)
        if r.batch is None:
            if r.source_file not in latest:
                latest[r.source_file] = None
            continue
        if cur is None or r.batch > cur:
            latest[r.source_file] = r.batch

    filtered: list[TripleRecord] = []
    for r in items:
        target = latest.get(r.source_file)
        if target is None:
            filtered.append(r)
        elif r.batch == target:
            filtered.append(r)
    return filtered


def _pair_sum_rows(records: Iterable[TripleRecord]) -> list[dict[str, object]]:
    """Build rows aggregated over k for each (qm9_index, i, j).

    - If latest_batch_only=True, keeps only the latest batch per file.
    - Uses qm9_index derived from file stem (e.g. water.out -> water).
    - Sums et_ijk over all k values for the same (qm9_index, i, j).

    Output rows contain only: qm9_index, i, j, k, et_ijk.
    Here k is set to -1 to indicate "summed over all k".
    """

    input_records = list(records)
    acc: dict[tuple[str, int, int], float] = {}
    for r in input_records:
        qm9_index = Path(r.source_file).stem
        key = (qm9_index, r.i, r.j)
        acc[key] = acc.get(key, 0.0) + float(r.et_ijk)

    rows: list[dict[str, object]] = [
        {"qm9_index": qm9_index, "i": i, "j": j, "et_ijk": et_sum}
        for (qm9_index, i, j), et_sum in acc.items()
    ]
    rows.sort(key=lambda d: (str(d["qm9_index"]), int(d["i"]), int(d["j"])))
    return rows


def _write_pair_sum_csv(rows: Iterable[dict[str, object]], output_path: Path, mode: str) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    write_header = True
    file_mode = "w" if mode == "write" else "a"
    if mode == "append" and output_path.exists() and output_path.stat().st_size > 0:
        write_header = False

    count = 0
    with output_path.open(file_mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["qm9_index", "i", "j", "et_ijk"],
        )
        if write_header:
            writer.writeheader()

        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def _write_pair_sum_df(records: Iterable[TripleRecord]):
    """Return the pair-summed latest-batch view as a pandas DataFrame."""

    try:
        import pandas as pd  # type: ignore
    except ImportError as e:  # pragma: no cover
        raise RuntimeError(
            "pandas is required for _write_pair_sum_df(). Install pandas and retry."
        ) from e

    # Kept for backwards-compatibility of the function name; default to all batches.
    rows = _pair_sum_rows(records, latest_batch_only=False)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["qm9_index", "i", "j"], kind="mergesort").reset_index(drop=True)
    return df


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Extract ORCA triples correction progress lines (Triple i j k ...) to CSV."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more ORCA .out files (globs supported depending on shell).",
    )
    parser.add_argument(
        "-O","--output",
        default="qm9_out.csv",
        help="Output CSV path (default: qm9_out.csv in current working directory).",
    )
    parser.add_argument(
        "--mode",
        choices=["write", "append"],
        default="append",
        help="Write mode: overwrite (write) or append (append). Default: append.",
    )
    parser.add_argument(
        "--pair-sum",
        action="store_true",
        help=(
            "Write a reduced CSV for downstream use: include all batches by default, "
            "use qm9_index as file stem, and sum et_ijk over all k for each (qm9_index,i,j)."
        ),
    )

    args = parser.parse_args(argv)

    input_paths = _expand_inputs(args.inputs)
    if not input_paths:
        print("No input files found.", file=sys.stderr)
        return 2

    all_records: list[TripleRecord] = []
    missing_files: list[str] = []
    for path in input_paths:
        if not path.exists():
            missing_files.append(str(path))
            continue
        all_records.extend(list(_iter_triples_from_file(path)))

    if missing_files:
        print(f"Warning: missing {len(missing_files)} file(s)", file=sys.stderr)

    if not all_records:
        print("No Triple lines matched. Output not written.", file=sys.stderr)
        return 3

    out_path = Path(args.output)

    if args.pair_sum:
        rows = _pair_sum_rows(all_records)
        if not rows:
            print("No rows after latest-batch filtering. Output not written.", file=sys.stderr)
            return 3
        written = _write_pair_sum_csv(rows, out_path, args.mode)
        print(f"Wrote {written} rows to {out_path}")
        return 0

    all_records.sort(key=lambda r: (r.i, r.j))
    written = _write_csv(all_records, out_path, args.mode)
    print(f"Wrote {written} rows to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
