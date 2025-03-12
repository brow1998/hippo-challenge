import glob
import pandas as pd
import os
import logging

FILE_FORMATS = {
    "json": pd.read_json,
    "csv": pd.read_csv
}

SCHEMAS = {
    "pharmacies": {
        "chain": "string",
        "npi": "string"
    },
    "claims": {
        "id": "string",
        "npi": "string",
        "ndc": "string",
        "price": "float",
        "quantity": "int",
        "timestamp": "datetime"
    },
    "reverts": {
        "id": "string",
        "claim_id": "string",
        "timestamp": "datetime"
    }
}

def load_data(layout: str, file_format: str, data_path: str = "data/input/{layout}") -> pd.DataFrame:
    """
    Loads and validates data for the given layout ('pharmacies', 'claims', or 'reverts').

    Parameters:
        layout (str): Data type (e.g., 'pharmacies', 'claims', 'reverts').
        file_format (str): File format ('json' or 'csv').
        data_path (str): Base path to the data folder.

    Returns:
        pd.DataFrame: DataFrame with only the columns defined in the schema; invalid rows are removed.
    """
    path = os.path.join(data_path.format(layout=layout), f'*.{file_format}')
    dataframes = []

    for item in glob.glob(path):
        logging.info(f"Reading file: {item}")
        try:
            df = FILE_FORMATS[file_format](item)
        except Exception as e:
            logging.error(f"Error reading file {item}: {e}")
            continue

        for col in SCHEMAS[layout].keys():
            if col not in df.columns:
                logging.warning(f"Column '{col}' missing in file {item}. Creating column with NA values.")
                df[col] = pd.NA

        for col, col_type in SCHEMAS[layout].items():
            if col_type == "datetime":
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif col_type in ["float", "int"]:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            elif col_type == "string":
                df[col] = df[col].astype("string")

        df = df[list(SCHEMAS[layout].keys())]

        required_cols = list(SCHEMAS[layout].keys())
        invalid_mask = df[required_cols].isna().any(axis=1)
        invalid_count = invalid_mask.sum()
        if invalid_count > 0:
            logging.warning(f"{invalid_count} ROW(S) WITH ISSUES in file {item}")
            for idx, row in df[invalid_mask].iterrows():
                logging.warning("==========================================")
                logging.warning(f"PROBLEMATIC ROW - Index: {idx}")
                logging.warning(row.to_dict())
                logging.warning("==========================================")

        df_valid = df.dropna(subset=required_cols)
        logging.info(f"File {item}: {len(df_valid)} valid rows out of {len(df)}.")
        dataframes.append(df_valid)

    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        logging.info(f"Data for {layout} loaded and validated:")
        logging.info(final_df.head())
        return final_df
    else:
        logging.error(f"No valid data found for {layout}.")
        return pd.DataFrame()

def load_all_data() -> dict:
    """
    Loads data for 'pharmacies', 'claims', and 'reverts'.

    Returns:
        dict: A dictionary with keys 'pharmacies', 'claims', and 'reverts' each containing the corresponding DataFrame.
    """
    data = {}
    data["pharmacies"] = load_data("pharmacies", "csv")
    data["claims"] = load_data("claims", "json")
    data["reverts"] = load_data("reverts", "json")
    return data
