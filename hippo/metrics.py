import pandas as pd
import logging

def compute_metrics(claims_df: pd.DataFrame, reverts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes metrics based on claims and reverts data.

    Metrics include:
      - fills: count of claims
      - total_price: sum of prices (rounded)
      - avg_price: average unit price (rounded)
      - reverted: count of reverts

    Parameters:
        claims_df (pd.DataFrame): Validated claims data.
        reverts_df (pd.DataFrame): Validated reverts data.

    Returns:
        pd.DataFrame: DataFrame with computed metrics.
    """
    if claims_df.empty:
        logging.error("Claims DataFrame is empty.")
        return pd.DataFrame()

    claims_df["unit_price"] = claims_df["price"] / claims_df["quantity"]

    metrics_claims = claims_df.groupby(["npi", "ndc"]).agg(
        fills=("id", "count"),
        total_price=("price", "sum"),
        avg_price=("unit_price", "mean")
    ).reset_index()

    if not reverts_df.empty:
        claims_reverts = claims_df[["id", "npi", "ndc"]].merge(
            reverts_df,
            left_on="id",
            right_on="claim_id",
            how="left"
        )
        reverts_count = claims_reverts.groupby(["npi", "ndc"]).apply(
            lambda df: df["claim_id"].notna().sum()
        ).reset_index(name="reverted")
    else:
        logging.error("No reverts data found.")
        reverts_count = pd.DataFrame(columns=["npi", "ndc", "reverted"])

    metrics = metrics_claims.merge(
        reverts_count,
        on=["npi", "ndc"],
        how="left"
    )
    metrics["reverted"] = metrics["reverted"].fillna(0).astype(int)
    metrics["total_price"] = metrics["total_price"].round(2)
    metrics["avg_price"] = metrics["avg_price"].round(2)

    return metrics
