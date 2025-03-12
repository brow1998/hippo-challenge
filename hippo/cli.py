import argparse
import logging
import os

from hippo import data_loader, metrics, quantities, recommendations


def validate_data() -> bool:
    """
    Loads all data and logs the number of valid rows for each dataset.

    Returns:
        bool: `True` if all datasets have valid data, `False` otherwise.
    """
    data = data_loader.load_all_data()
    valid = True
    for key, df in data.items():
        if df.empty:
            logging.error(f"No valid data found for {key}.")
            valid = False
        else:
            logging.info(f"{key.capitalize()} data: {len(df)} valid rows.")
    if valid:
        logging.info("All datasets validated successfully.")
    else:
        logging.error("Some datasets have issues.")
    return valid


def generate_metrics(output_dir: str):
    """
    Generates metrics from claims and reverts data and saves the result to a JSON file.

    Parameters:
        output_dir (str): Directory where the output file will be saved.
    """
    data = data_loader.load_all_data()
    claims_df = data.get("claims")
    reverts_df = data.get("reverts")
    if claims_df.empty:
        logging.error("No claims data available for metrics computation.")
        return
    metrics_df = metrics.compute_metrics(claims_df, reverts_df)
    if metrics_df.empty:
        logging.error("Metrics computation resulted in an empty dataset.")
    else:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "metrics.json")
        metrics_df.to_json(output_path, orient="records", indent=4)
        logging.info(f"Metrics saved to {output_path}")


def generate_recommendations(output_dir: str):
    """
    Generates top 2 chain recommendations per drug and saves the result to a JSON file.

    Parameters:
        output_dir (str): Directory where the output file will be saved.
    """
    data = data_loader.load_all_data()
    pharmacies_df = data.get("pharmacies")
    claims_df = data.get("claims")
    if claims_df.empty or pharmacies_df.empty:
        logging.error("Insufficient data to compute recommendations.")
        return
    top_chains = recommendations.compute_top_chains(claims_df, pharmacies_df)
    if not top_chains:
        logging.error("Recommendations computation resulted in an empty dataset.")
    else:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "top_chains.json")
        recommendations.save_top_chains(top_chains, output_path)
        logging.info(f"Recommendations saved to {output_path}")


def generate_common_quantities(output_dir: str):
    """
    Generates the most common prescription quantities per drug and saves the result to a JSON file.

    Parameters:
        output_dir (str): Directory where the output file will be saved.
    """
    data = data_loader.load_all_data()
    claims_df = data.get("claims")
    if claims_df.empty:
        logging.error("No claims data available for common quantities computation.")
        return
    common_quantities = quantities.compute_common_quantities(claims_df)
    if not common_quantities:
        logging.error("Common quantities computation resulted in an empty dataset.")
    else:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, "most_prescribed_quantities.json")
        quantities.save_common_quantities(common_quantities, output_path)
        logging.info(f"Common quantities saved to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Hippo - Data Processing CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_validate = subparsers.add_parser(
        "validate", help="Validate input data files."
    )

    parser_metrics = subparsers.add_parser(
        "metrics", help="Generate metrics from data."
    )
    parser_metrics.add_argument(
        "--output", type=str, default="data/output", help="Output directory for metrics."
    )

    parser_recommend = subparsers.add_parser(
        "recommend", help="Generate top 2 chain recommendations per drug."
    )
    parser_recommend.add_argument(
        "--output",
        type=str,
        default="data/output",
        help="Output directory for recommendations.",
    )

    parser_common = subparsers.add_parser(
        "common", help="Generate most common prescription quantities per drug."
    )
    parser_common.add_argument(
        "--output",
        type=str,
        default="data/output",
        help="Output directory for common quantities.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if args.command == "validate":
        valid = validate_data()
        if valid:
            logging.info("Data validation completed successfully.")
        else:
            logging.error("Data validation encountered issues.")
    elif args.command == "metrics":
        generate_metrics(args.output)
    elif args.command == "recommend":
        generate_recommendations(args.output)
    elif args.command == "common":
        generate_common_quantities(args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
