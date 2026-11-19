"""Entry point: `hippo` reads the three sources and writes every output."""

from __future__ import annotations

import argparse
import json
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from hippo import load, transform

log = logging.getLogger("hippo")


def _write(path: Path, payload) -> None:
    """Write JSON atomically so a crash never leaves a half-written output."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=4, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def run(pharmacies: list[str], claims: list[str], reverts: list[str], output: str) -> dict:
    """Run the whole pipeline and return the run summary."""
    with ProcessPoolExecutor() as pool:
        loaded = {
            name: load.load(name, dirs, pool)
            for name, dirs in (("pharmacies", pharmacies), ("claims", claims), ("reverts", reverts))
        }

    in_scope, out_of_scope = transform.scope(loaded["claims"].rows, loaded["pharmacies"].rows, loaded["reverts"].rows)

    rejected = [r for source in loaded.values() for r in source.rejected]
    rejected += [
        {
            "source": "claims",
            "source_file": r.source_file,
            "source_row": int(r.source_row),
            "reason": "npi not in pharmacy dataset",
        }
        for r in out_of_scope.itertuples()
    ]
    rejected.sort(key=lambda r: (r["source"], r["source_file"], r["source_row"] if r["source_row"] is not None else -1))

    summary = {
        "pharmacies": len(loaded["pharmacies"].rows),
        "claims_valid": len(loaded["claims"].rows),
        "claims_in_scope": len(in_scope),
        "claims_reverted": int(in_scope["reverted"].sum()) if len(in_scope) else 0,
        "reverts_valid": len(loaded["reverts"].rows),
        "rejected": len(rejected),
    }

    out = Path(output)
    out.mkdir(parents=True, exist_ok=True)
    _write(out / "metrics.json", transform.metrics(in_scope))
    _write(out / "top_chains.json", transform.top_chains(in_scope, loaded["pharmacies"].rows))
    _write(out / "most_prescribed_quantities.json", transform.common_quantities(in_scope))
    _write(out / "rejected.json", rejected)
    _write(out / "run_summary.json", summary)
    return summary


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="hippo", description=__doc__)
    parser.add_argument("--pharmacies", nargs="+", default=["data/input/pharmacies"], help="pharmacy CSV directories")
    parser.add_argument("--claims", nargs="+", default=["data/input/claims"], help="claim event JSON directories")
    parser.add_argument("--reverts", nargs="+", default=["data/input/reverts"], help="revert event JSON directories")
    parser.add_argument("--output", default="output", help="where the JSON outputs are written")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    summary = run(args.pharmacies, args.claims, args.reverts, args.output)
    for key, value in summary.items():
        log.info("%-16s %s", key, value)
    log.info("outputs written to %s/", args.output)


if __name__ == "__main__":
    main()
