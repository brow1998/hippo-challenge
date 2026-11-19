"""The three business outputs. Pure functions over validated frames.

Business rules, decided once and applied everywhere:
- Only claims from pharmacies in the pharmacy dataset are in scope.
- A revert invalidates its claim. Reverted claims still count as fills and as
  prescriptions, but they never contribute to prices.
- A claim reverted more than once counts as one revert.
- Ties are broken by name or value so the output is byte-for-byte stable.
"""

from __future__ import annotations

import pandas as pd


def scope(claims: pd.DataFrame, pharmacies: pd.DataFrame, reverts: pd.DataFrame):
    """Split claims into in-scope and out-of-scope, and flag reverted ones."""
    known = claims["npi"].isin(pharmacies["npi"])
    in_scope = claims[known].copy()
    in_scope["reverted"] = in_scope["id"].isin(reverts["claim_id"])
    in_scope["unit_price"] = in_scope["price"] / in_scope["quantity"]
    return in_scope, claims[~known]


def metrics(claims: pd.DataFrame) -> list[dict]:
    """Fills, reverts, average unit price and total price per npi and ndc."""
    if claims.empty:
        return []
    counts = claims.groupby(["npi", "ndc"]).agg(fills=("id", "size"), reverted=("reverted", "sum"))
    priced = (
        claims[~claims["reverted"]]
        .groupby(["npi", "ndc"])
        .agg(avg_price=("unit_price", "mean"), total_price=("price", "sum"))
    )
    out = counts.join(priced).reset_index().sort_values(["npi", "ndc"])
    return [
        {
            "npi": r.npi,
            "ndc": r.ndc,
            "fills": int(r.fills),
            "reverted": int(r.reverted),
            # A pair whose every claim was reverted has no valid price.
            "avg_price": None if pd.isna(r.avg_price) else round(float(r.avg_price), 2),
            "total_price": 0.0 if pd.isna(r.total_price) else round(float(r.total_price), 2),
        }
        for r in out.itertuples(index=False)
    ]


def top_chains(claims: pd.DataFrame, pharmacies: pd.DataFrame, n: int = 2) -> list[dict]:
    """The n chains with the lowest average unit price for each drug."""
    valid = claims[~claims["reverted"]]
    if valid.empty:
        return []
    by_chain = (
        valid.merge(pharmacies[["npi", "chain"]], on="npi")
        .groupby(["ndc", "chain"], as_index=False)["unit_price"]
        .mean()
        .sort_values(["ndc", "unit_price", "chain"])
    )
    return [
        {
            "ndc": ndc,
            "chain": [
                {"name": r.chain, "avg_price": round(float(r.unit_price), 2)} for r in group.head(n).itertuples()
            ],
        }
        for ndc, group in by_chain.groupby("ndc", sort=True)
    ]


def common_quantities(claims: pd.DataFrame) -> list[dict]:
    """Prescribed quantities per drug, most frequent first."""
    if claims.empty:
        return []
    freq = (
        claims.groupby(["ndc", "quantity"])
        .size()
        .reset_index(name="count")
        .sort_values(["ndc", "count", "quantity"], ascending=[True, False, True])
    )
    return [
        {"ndc": ndc, "most_prescribed_quantity": [float(q) for q in group["quantity"]]}
        for ndc, group in freq.groupby("ndc", sort=True)
    ]
