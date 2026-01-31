#!/usr/bin/env python3
"""Extract ORCA pair correlation energies (EP(Final)) into a CSV.

This script targets ORCA output blocks like:

------------------------------
PAIR CORRELATION ENERGIES (Eh)
------------------------------

	i     j         EP(Guess)         EP(Final)       Ratio       EP(aa)            EP(ab)     #PNOs
	3     3  :     -0.027493143      -0.031230917     0.88     -0.000000000      -0.031230917          38

It writes a reduced CSV with columns:
  qm9_index,i,j,EP_final

Where qm9_index is derived from the file name stem (e.g. water.out -> water).
The (i,j) indices are normalized so i <= j.

Typical usage:
  python3 scripts/_extract_pair_corr_eng.py qm9_orca_work/qm9_orca_work_molecu/*.out \
	--output qm9_pair_corr.csv --mode write
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


_PAIR_SECTION_TITLE_RE = re.compile(r"^\s*PAIR\s+CORRELATION\s+ENERGIES\s*\(Eh\)\s*$")

# ORCA outputs can be wrapped or even place multiple rows on one physical line.
# We only need i, j and EP(Final), so match those inline and ignore the rest.
_PAIR_INLINE_RE = re.compile(
	r"(?<!\d)(?P<i>\d+)\s+(?P<j>\d+)\s*:\s*"
	r"(?P<ep_guess>[-+0-9.Ee]+)\s+"
	r"(?P<ep_final>[-+0-9.Ee]+)"
)


@dataclass(frozen=True)
class PairCorrRecord:
	source_file: str
	i: int
	j: int
	ep_final: float


def _iter_pair_corr_from_file(path: Path) -> Iterator[PairCorrRecord]:
	try:
		with path.open("r", encoding="utf-8", errors="replace") as f:
			in_section = False
			for line in f:
				if not in_section:
					if _PAIR_SECTION_TITLE_RE.match(line):
						in_section = True
					continue

				# After the title line, scan for 0..N rows in each physical line.
				any_found = False
				for m in _PAIR_INLINE_RE.finditer(line):
					any_found = True

					i = int(m.group("i"))
					j = int(m.group("j"))
					if i > j:
						i, j = j, i

					yield PairCorrRecord(
						source_file=str(path),
						i=i,
						j=j,
						ep_final=float(m.group("ep_final")),
					)

				if not any_found:
					continue
	except FileNotFoundError:
		return


def _expand_inputs(inputs: list[str]) -> list[Path]:
	paths: list[Path] = []
	for raw in inputs:
		if any(ch in raw for ch in "*?["):
			paths.extend(sorted(Path(p) for p in Path().glob(raw)))
		else:
			paths.append(Path(raw))

	seen: set[Path] = set()
	uniq: list[Path] = []
	for p in paths:
		if p in seen:
			continue
		seen.add(p)
		uniq.append(p)
	return uniq


def _write_pair_corr_csv(records: Iterable[PairCorrRecord], output_path: Path, mode: str) -> int:
	output_path.parent.mkdir(parents=True, exist_ok=True)

	write_header = True
	file_mode = "w" if mode == "write" else "a"
	if mode == "append" and output_path.exists() and output_path.stat().st_size > 0:
		write_header = False

	count = 0
	with output_path.open(file_mode, newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=["qm9_index", "i", "j", "EP_final"])
		if write_header:
			writer.writeheader()

		for r in records:
			qm9_index = Path(r.source_file).stem
			writer.writerow(
				{
					"qm9_index": qm9_index,
					"i": r.i,
					"j": r.j,
					"EP_final": f"{r.ep_final:.15g}",
				}
			)
			count += 1
	return count


def _write_pair_corr_df(records: Iterable[PairCorrRecord]):
	"""Return a reduced pandas DataFrame for downstream analysis."""

	try:
		import pandas as pd  # type: ignore
	except ImportError as e:  # pragma: no cover
		raise RuntimeError(
			"pandas is required for _write_pair_corr_df(). Install pandas and retry."
		) from e

	rows = [
		{
			"qm9_index": Path(r.source_file).stem,
			"i": r.i,
			"j": r.j,
			"EP_final": r.ep_final,
		}
		for r in records
	]
	df = pd.DataFrame(rows)
	if not df.empty:
		df = df.sort_values(["qm9_index", "i", "j"], kind="mergesort").reset_index(drop=True)
	return df


def main(argv: list[str]) -> int:
	parser = argparse.ArgumentParser(
		description="Extract ORCA pair correlation energies (EP(Final)) to a reduced CSV."
	)
	parser.add_argument(
		"inputs",
		nargs="+",
		help="One or more ORCA .out files (globs supported depending on shell).",
	)
	parser.add_argument(
		"--output",
		default="qm9_pair_corr.csv",
		help="Output CSV path (default: qm9_pair_corr.csv in current working directory).",
	)
	parser.add_argument(
		"--mode",
		choices=["write", "append"],
		default="append",
		help="Write mode: overwrite (write) or append (append). Default: append.",
	)

	args = parser.parse_args(argv)

	input_paths = _expand_inputs(args.inputs)
	if not input_paths:
		print("No input files found.", file=sys.stderr)
		return 2

	all_records: list[PairCorrRecord] = []
	missing_files: list[str] = []
	for path in input_paths:
		if not path.exists():
			missing_files.append(str(path))
			continue
		all_records.extend(list(_iter_pair_corr_from_file(path)))

	if missing_files:
		print(f"Warning: missing {len(missing_files)} file(s)", file=sys.stderr)

	if not all_records:
		print("No pair correlation lines matched. Output not written.", file=sys.stderr)
		return 3

	all_records.sort(key=lambda r: (Path(r.source_file).stem, r.i, r.j))
	out_path = Path(args.output)
	written = _write_pair_corr_csv(all_records, out_path, args.mode)
	print(f"Wrote {written} rows to {out_path}")
	return 0


if __name__ == "__main__":
	raise SystemExit(main(sys.argv[1:]))
