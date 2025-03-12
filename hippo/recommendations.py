import pandas as pd
import json
import logging

def compute_top_chains(claims_df: pd.DataFrame, pharmacies_df: pd.DataFrame) -> list:
    """
    For each drug (ndc), computes the top 2 chains (obtained via npi) with the lowest average unit price.

    Parameters:
        claims_df (pd.DataFrame): Validated claims data.
        pharmacies_df (pd.DataFrame): Validated pharmacies data.

    Returns:
        list: A list of dictionaries in the format:
            {
                "ndc": <ndc>,
                "chain": [
                    {"name": <chain_name>, "avg_price": <avg_price>},
                    ...
                ]
            }
    """
    if claims_df.empty or pharmacies_df.empty:
        logging.error("Insufficient data to compute Top 2 Chains per Drug.")
        return []

    claims_with_chain = claims_df.merge(pharmacies_df, on="npi", how="left")

    if "unit_price" not in claims_with_chain.columns:
        claims_with_chain["unit_price"] = claims_with_chain["price"] / claims_with_chain["quantity"]

    group_chain = claims_with_chain.groupby(["ndc", "chain"]).agg(
        avg_unit_price=("unit_price", "mean")
    ).reset_index()

    top_chains = []
    for ndc, group in group_chain.groupby("ndc"):
        group_sorted = group.sort_values("avg_unit_price")
        top_2 = group_sorted.head(2)
        chain_list = [
            {"name": row["chain"], "avg_price": round(row["avg_unit_price"], 2)}
            for _, row in top_2.iterrows()
        ]
        top_chains.append({"ndc": ndc, "chain": chain_list})

    return top_chains

def save_top_chains(top_chains: list, output_file: str = "output/top_chains.json"):
    """
    Saves the top chains data to a JSON file.

    Parameters:
        top_chains (list): List of top chains per drug.
        output_file (str): Path to the output file.
    """
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(top_chains, f, indent=4)
