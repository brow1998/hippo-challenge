# hippo

[![ci](https://github.com/brow1998/hippo-challenge/actions/workflows/ci.yml/badge.svg)](https://github.com/brow1998/hippo-challenge/actions/workflows/ci.yml)

A small pharmacy claims pipeline. It reads a pharmacy registry and two event streams (claims and
reverts), throws out what doesn't fit the schema, and answers three business questions:

1. **How does each pharmacy perform per drug?** Fills, reverts, average unit price, total price.
2. **Which two chains are cheapest for each drug?** Ranked by average unit price.
3. **What quantities are prescribed most for each drug?** Ordered by frequency.

The problem statement is in [`instructions/`](instructions/01-data-project.md).

## Quick start

Needs [uv](https://docs.astral.sh/uv/). One command, from a clean checkout:

```bash
uv run hippo
```

That reads the sample data in `data/input/` and writes five files to `output/`:

| File | What's in it |
|---|---|
| `metrics.json` | Fills, reverts, average unit price and total price per `npi` × `ndc` |
| `top_chains.json` | The two cheapest chains per drug |
| `most_prescribed_quantities.json` | Prescribed quantities per drug, most frequent first |
| `rejected.json` | Every row that didn't make it, with its file, row number and reason |
| `run_summary.json` | Row counts for the run |

Point it at other data with one or more directories per source:

```bash
uv run hippo --pharmacies a/ --claims b/ c/ --reverts d/ --output out/
```

On the sample data:

```text
INFO pharmacies       17
INFO claims_valid     27073
INFO claims_in_scope  22988
INFO claims_reverted  260
INFO reverts_valid    305
INFO rejected         4091
```

## How it behaves

**Running it twice gives the same answer.** Outputs are sorted with explicit tie-breakers and written
atomically, so a second run produces byte-identical files. CI runs the pipeline twice and diffs the results.

**Replayed events don't count twice.** Claims and reverts are streams, so the same event can arrive
more than once. Events are keyed by `id`; repeats are rejected as duplicates. A claim reverted twice
counts as one revert.

**Every rejected row has a receipt.** Each entry in `rejected.json` names the source file, the row
index inside it and the reason: a missing or invalid field, a non-positive quantity, a negative price, a
duplicate id, an unreadable file, or a pharmacy that isn't in the registry.

**A bad file doesn't stop the run.** It is logged as rejected and the rest of the data is processed.

## Business rules

- Only claims from pharmacies in the pharmacy dataset are in scope.
- A revert invalidates its claim. A reverted claim still counts as a fill and as a prescribed
  quantity, but never contributes to `avg_price`, `total_price` or the chain ranking. A pharmacy and
  drug pair whose claims were all reverted reports `avg_price: null`.
- Unit price is `price / quantity`; `avg_price` is the mean unit price across claims.
- File parsing runs in a process pool, one worker per core.

## Layout

```text
hippo/load.py        read, validate and deduplicate each source, keeping file and row receipts
hippo/transform.py   the three outputs as pure functions over validated frames
hippo/cli.py         wiring, atomic writes, run summary
tests/               behaviour tests on small inline fixtures, no network
```

## Tests

```bash
uv run pytest
```

## License

[MIT](LICENSE) covers the code in this repository. The problem statement and sample data in
`instructions/` and `data/input/` belong to their authors.
