import json
import logging
import os

from hippo import data_loader, metrics, quantities, recommendations


def main():
    """
    Main entry point for the Hippo-Project. Loads data, computes metrics, recommendations,
    and common prescription quantities, then saves the results to JSON files.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Starting Hippo-Project")

    os.makedirs("output", exist_ok=True)

    data = data_loader.load_all_data()
    pharmacies_df = data.get("pharmacies")
    claims_df = data.get("claims")
    reverts_df = data.get("reverts")

    metrics_df = metrics.compute_metrics(claims_df, reverts_df)
    if not metrics_df.empty:
        logging.info("--- Computed Metrics ---")
        logging.info(metrics_df.head())
        metrics_df.to_json("data/output/metrics.json", orient="records", indent=4)
    else:
        logging.error("No claims data available for metrics computation.")

    top_chains = recommendations.compute_top_chains(claims_df, pharmacies_df)
    if top_chains:
        logging.info("--- Top 2 Chains per Drug ---")
        logging.info(json.dumps(top_chains[:5], indent=4))
        recommendations.save_top_chains(top_chains)
    else:
        logging.error("Insufficient data to compute Top 2 Chains per Drug.")

    common_quantities = quantities.compute_common_quantities(claims_df)
    if common_quantities:
        logging.info("--- Most Common Prescription Quantities per Drug ---")
        logging.info(json.dumps(common_quantities[:5], indent=4))
        quantities.save_common_quantities(common_quantities)
    else:
        logging.error(
            "Insufficient claims data to compute common prescription quantities."
        )


if __name__ == "__main__":
    main()
