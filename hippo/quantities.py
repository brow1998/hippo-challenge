import pandas as pd
import json
import logging

def compute_common_quantities(claims_df: pd.DataFrame) -> list:
    """
    For each drug (ndc), identifies prescription quantities ordered by frequency (from highest to lowest).

    Parameters:
        claims_df (pd.DataFrame): Validated claims data.

    Returns:
        list: A list of dictionaries in the format:
            {
                "ndc": <ndc>,
                "most_prescribed_quantity": [<quantity1>, <quantity2>, ...]
            }
    """
    if claims_df.empty:
        logging.error("Insufficient claims data to compute common prescription quantities.")
        return []

    freq = claims_df.groupby(["ndc", "quantity"]).size().reset_index(name="count")

    top_quantities = []
    for ndc, group in freq.groupby("ndc"):
        group_sorted = group.sort_values("count", ascending=False)
        quantities_list = group_sorted["quantity"].tolist()
        top_quantities.append({
            "ndc": ndc,
            "most_prescribed_quantity": quantities_list
        })

    return top_quantities

def save_common_quantities(top_quantities: list, output_file: str = "output/most_prescribed_quantities.json"):
    """
    Saves the common quantities data to a JSON file.

    Parameters:
        top_quantities (list): List of prescription quantities per drug.
        output_file (str): Path to the output file.
    """
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(top_quantities, f, indent=4)
