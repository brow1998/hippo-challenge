"""Read and validate the three event sources.

Every record keeps a receipt (`source_file`, `source_row`) so any rejected or
accepted row can be traced back to the exact line it came from.
"""

from __future__ import annotations

import csv
import json
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass
class Loaded:
    """Valid rows as a DataFrame, plus every rejected row with its reason."""

    rows: pd.DataFrame
    rejected: list[dict] = field(default_factory=list)


def _read_json(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise TypeError("expected a JSON array of events")
    return data


def _read_csv(path: str) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _read_file(path: str) -> tuple[str, list[dict] | None, str | None]:
    reader = _read_csv if path.endswith(".csv") else _read_json
    try:
        return path, reader(path), None
    except (OSError, ValueError, TypeError) as exc:  # JSONDecodeError is a ValueError
        return path, None, f"unreadable file: {exc}"


def _files(dirs: list[str], suffix: str) -> list[str]:
    return sorted(str(p) for d in dirs for p in Path(d).glob(f"*{suffix}"))


def _text(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _number(value) -> float | None:
    if isinstance(value, bool) or value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _timestamp(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _validate_pharmacy(raw: dict) -> tuple[dict | None, str | None]:
    npi, chain = _text(raw.get("npi")), _text(raw.get("chain"))
    if npi is None or chain is None:
        return None, "missing npi or chain"
    return {"npi": npi, "chain": chain}, None


def _validate_claim(raw: dict) -> tuple[dict | None, str | None]:
    row = {
        "id": _text(raw.get("id")),
        "npi": _text(raw.get("npi")),
        "ndc": _text(raw.get("ndc")),
        "price": _number(raw.get("price")),
        "quantity": _number(raw.get("quantity")),
        "timestamp": _timestamp(raw.get("timestamp")),
    }
    missing = [k for k, v in row.items() if v is None]
    if missing:
        return None, f"missing or invalid: {', '.join(missing)}"
    if row["quantity"] <= 0:
        return None, "quantity must be positive"
    if row["price"] < 0:
        return None, "price must not be negative"
    return row, None


def _validate_revert(raw: dict) -> tuple[dict | None, str | None]:
    row = {
        "id": _text(raw.get("id")),
        "claim_id": _text(raw.get("claim_id")),
        "timestamp": _timestamp(raw.get("timestamp")),
    }
    missing = [k for k, v in row.items() if v is None]
    if missing:
        return None, f"missing or invalid: {', '.join(missing)}"
    return row, None


SOURCES = {
    "pharmacies": (".csv", _validate_pharmacy, "npi", ["npi", "chain"]),
    "claims": (".json", _validate_claim, "id", ["id", "npi", "ndc", "price", "quantity", "timestamp"]),
    "reverts": (".json", _validate_revert, "id", ["id", "claim_id", "timestamp"]),
}


def load(source: str, dirs: list[str], pool: ProcessPoolExecutor | None = None) -> Loaded:
    """Load one source from one or more directories.

    Rows that break the schema are rejected with a reason. Rows that repeat an
    already-seen key are rejected as duplicates, so replaying the same event
    stream never changes the result.
    """
    suffix, validate, key, columns = SOURCES[source]
    files = _files(dirs, suffix)
    results = pool.map(_read_file, files) if pool else map(_read_file, files)

    rows: list[dict] = []
    rejected: list[dict] = []
    seen: set[str] = set()
    for path, records, error in results:
        if error:
            rejected.append({"source": source, "source_file": path, "source_row": None, "reason": error})
            continue
        for index, raw in enumerate(records):
            receipt = {"source": source, "source_file": path, "source_row": index}
            row, reason = validate(raw) if isinstance(raw, dict) else (None, "not an object")
            if row is not None and row[key] in seen:
                row, reason = None, f"duplicate {key}"
            if row is None:
                rejected.append({**receipt, "reason": reason})
                continue
            seen.add(row[key])
            rows.append({**row, "source_file": path, "source_row": index})

    return Loaded(pd.DataFrame(rows, columns=[*columns, "source_file", "source_row"]), rejected)
