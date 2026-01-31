#!/usr/bin/env python3
"""Convert extracted pair-energy CSVs into HDF5 for downstream workflows.

This script supports the two CSV formats produced in this repo:

- Pair correlation energies: columns qm9_index,i,j,EP_final
- Triples pair-sum energies: columns qm9_index,i,j,et_ijk

It writes HDF5 with the structure:

  /<molname>/pair_ene      (float64, shape (N,))
  /<molname>/pairlist      (int32,   shape (N,2))  columns are (i,j)

Where <molname> is taken from qm9_index.

Typical usage:
  python3 scripts/_create_pair_hdf5.py \
	--pair-csv csv/testing_pair.csv --pair-h5 pair.h5 \
	--triples-csv csv/testing_triples.csv --triples-h5 triples.h5
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional


def _write_h5_from_csv(csv_path: Path, h5_path: Path, *, energy_col: Optional[str] = None) -> int:
	try:
		import pandas as pd  # type: ignore
	except ImportError as e:  # pragma: no cover
		raise RuntimeError("pandas is required. Install pandas and retry.") from e

	try:
		import h5py  # type: ignore
	except ImportError as e:  # pragma: no cover
		raise RuntimeError("h5py is required. Install h5py and retry.") from e

	df = pd.read_csv(csv_path)

	required_base = {"qm9_index", "i", "j"}
	missing = required_base.difference(df.columns)
	if missing:
		raise ValueError(f"{csv_path}: missing required columns: {sorted(missing)}")

	if energy_col is None:
		if "EP_final" in df.columns:
			energy_col = "EP_final"
		elif "et_ijk" in df.columns:
			energy_col = "et_ijk"
		else:
			raise ValueError(
				f"{csv_path}: cannot infer energy column. Provide --energy-col; got columns: {list(df.columns)}"
			)

	if energy_col not in df.columns:
		raise ValueError(f"{csv_path}: energy column '{energy_col}' not found")

	df = df[["qm9_index", "i", "j", energy_col]].copy()
	df["i"] = df["i"].astype(int)
	df["j"] = df["j"].astype(int)
	df[energy_col] = df[energy_col].astype(float)

	# Normalize (i,j) so i <= j.
	swap_mask = df["i"] > df["j"]
	if swap_mask.any():
		tmp = df.loc[swap_mask, "i"].to_numpy()
		df.loc[swap_mask, "i"] = df.loc[swap_mask, "j"].to_numpy()
		df.loc[swap_mask, "j"] = tmp

	# Stable sort to keep deterministic ordering.
	df = df.sort_values(["qm9_index", "i", "j"], kind="mergesort")

	h5_path.parent.mkdir(parents=True, exist_ok=True)
	written_groups = 0
	with h5py.File(h5_path, "w") as h5:
		h5.attrs["source_csv"] = str(csv_path)
		h5.attrs["energy_col"] = str(energy_col)

		for molname, g in df.groupby("qm9_index", sort=False):
			grp = h5.create_group(str(molname))

			pairlist = g[["i", "j"]].to_numpy(dtype="int32", copy=True)
			pair_ene = g[energy_col].to_numpy(dtype="float64", copy=True)

			grp.create_dataset("pairlist", data=pairlist)
			grp.create_dataset("pair_ene", data=pair_ene)
			written_groups += 1

	return written_groups


def main(argv: list[str]) -> int:
	p = argparse.ArgumentParser(
		description="Read pair-energy CSVs and write HDF5 grouped by qm9_index (molname)."
	)
	p.add_argument("--pair-csv", type=str, help="CSV with qm9_index,i,j,EP_final")
	p.add_argument("--pair-h5", type=str, help="Output HDF5 path for pair energies")
	p.add_argument("--triples-csv", type=str, help="CSV with qm9_index,i,j,et_ijk")
	p.add_argument("--triples-h5", type=str, help="Output HDF5 path for triples pair-sum energies")
	p.add_argument(
		"--energy-col",
		type=str,
		default=None,
		help="Override the energy column name (auto-detects EP_final or et_ijk).",
	)

	args = p.parse_args(argv)

	did_any = False

	if args.pair_csv or args.pair_h5:
		if not (args.pair_csv and args.pair_h5):
			print("Error: --pair-csv and --pair-h5 must be provided together", file=sys.stderr)
			return 2
		groups = _write_h5_from_csv(Path(args.pair_csv), Path(args.pair_h5), energy_col=args.energy_col)
		print(f"Wrote {groups} molecule group(s) to {args.pair_h5}")
		did_any = True

	if args.triples_csv or args.triples_h5:
		if not (args.triples_csv and args.triples_h5):
			print("Error: --triples-csv and --triples-h5 must be provided together", file=sys.stderr)
			return 2
		groups = _write_h5_from_csv(
			Path(args.triples_csv), Path(args.triples_h5), energy_col=args.energy_col
		)
		print(f"Wrote {groups} molecule group(s) to {args.triples_h5}")
		did_any = True

	if not did_any:
		print("Error: provide either --pair-csv/--pair-h5 and/or --triples-csv/--triples-h5", file=sys.stderr)
		return 2

	return 0


if __name__ == "__main__":
	raise SystemExit(main(sys.argv[1:]))

