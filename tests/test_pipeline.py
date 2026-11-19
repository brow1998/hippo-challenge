import json
from pathlib import Path

import pytest

from hippo.cli import run

PHARMACIES = "chain,npi\nhealth,111\nsaint,222\ndoctor,333\n"

CLAIMS = [
    {"id": "c1", "npi": "111", "ndc": "A", "price": 10.0, "quantity": 2, "timestamp": "2024-01-01T10:00:00"},
    {"id": "c2", "npi": "111", "ndc": "A", "price": 30.0, "quantity": 2, "timestamp": "2024-01-01T11:00:00"},
    {"id": "c3", "npi": "222", "ndc": "A", "price": 4.0, "quantity": 2, "timestamp": "2024-01-01T12:00:00"},
    {"id": "c4", "npi": "333", "ndc": "A", "price": 8.0, "quantity": 2, "timestamp": "2024-01-01T12:00:00"},
    {"id": "c5", "npi": "999", "ndc": "A", "price": 1.0, "quantity": 1, "timestamp": "2024-01-01T13:00:00"},
    {"id": "c6", "npi": "111", "ndc": "A", "price": 5.0, "timestamp": "2024-01-01T14:00:00"},
    {"id": "c7", "npi": "111", "ndc": "A", "price": 5.0, "quantity": 0, "timestamp": "2024-01-01T14:00:00"},
]

REVERTS = [
    {"id": "r1", "claim_id": "c2", "timestamp": "2024-01-02T10:00:00"},
    {"id": "r2", "claim_id": "c2", "timestamp": "2024-01-02T11:00:00"},
]


@pytest.fixture
def dirs(tmp_path: Path) -> dict:
    paths = {name: tmp_path / name for name in ("pharmacies", "claims", "reverts")}
    for p in paths.values():
        p.mkdir()
    (paths["pharmacies"] / "p.csv").write_text(PHARMACIES)
    (paths["claims"] / "c.json").write_text(json.dumps(CLAIMS))
    (paths["reverts"] / "r.json").write_text(json.dumps(REVERTS))
    return {name: [str(p)] for name, p in paths.items()} | {"output": str(tmp_path / "out")}


def outputs(out: str) -> dict:
    return {p.name: p.read_bytes() for p in sorted(Path(out).glob("*.json"))}


def read(out: str, name: str):
    return json.loads((Path(out) / name).read_text())


def test_reverted_claims_count_as_fills_but_never_as_price(dirs):
    run(**dirs)
    row = next(r for r in read(dirs["output"], "metrics.json") if r["npi"] == "111")
    assert row == {"npi": "111", "ndc": "A", "fills": 2, "reverted": 1, "avg_price": 5.0, "total_price": 10.0}


def test_a_claim_reverted_twice_counts_once(dirs):
    summary = run(**dirs)
    assert summary["claims_reverted"] == 1


def test_top_chains_rank_by_lowest_average_unit_price(dirs):
    run(**dirs)
    assert read(dirs["output"], "top_chains.json") == [
        {"ndc": "A", "chain": [{"name": "saint", "avg_price": 2.0}, {"name": "doctor", "avg_price": 4.0}]}
    ]


def test_quantities_are_ordered_by_frequency(dirs, tmp_path):
    extra = tmp_path / "more_claims"
    extra.mkdir()
    (extra / "c.json").write_text(
        json.dumps(
            [
                {"id": "x1", "npi": "111", "ndc": "A", "price": 1.0, "quantity": 5, "timestamp": "2024-01-03T00:00:00"},
            ]
        )
    )
    run(**{**dirs, "claims": dirs["claims"] + [str(extra)]})
    assert read(dirs["output"], "most_prescribed_quantities.json") == [
        {"ndc": "A", "most_prescribed_quantity": [2.0, 5.0]}
    ]


def test_every_rejected_row_has_a_receipt(dirs):
    run(**dirs)
    rejected = {
        (r["source_row"], r["reason"]) for r in read(dirs["output"], "rejected.json") if r["source"] == "claims"
    }
    assert rejected == {
        (4, "npi not in pharmacy dataset"),
        (5, "missing or invalid: quantity"),
        (6, "quantity must be positive"),
    }
    assert all(r["source_file"].endswith(".json") for r in read(dirs["output"], "rejected.json"))


def test_running_twice_produces_identical_outputs(dirs):
    run(**dirs)
    first = outputs(dirs["output"])
    run(**dirs)
    assert outputs(dirs["output"]) == first


def test_replaying_the_same_events_changes_nothing(dirs, tmp_path):
    run(**dirs)
    expected = {k: v for k, v in outputs(dirs["output"]).items() if k not in {"rejected.json", "run_summary.json"}}

    replay = tmp_path / "replay"
    replay.mkdir()
    (replay / "c.json").write_text(json.dumps(CLAIMS))
    run(**{**dirs, "claims": dirs["claims"] + [str(replay)]})
    actual = {k: v for k, v in outputs(dirs["output"]).items() if k not in {"rejected.json", "run_summary.json"}}
    assert actual == expected


def test_an_unreadable_file_is_rejected_not_fatal(dirs):
    Path(dirs["claims"][0], "broken.json").write_text("[{]")
    run(**dirs)
    assert any(r["reason"].startswith("unreadable file") for r in read(dirs["output"], "rejected.json"))
    assert read(dirs["output"], "metrics.json")
