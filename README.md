# Hippo

Hippo processes data from pharmacies, claims, and reverts to compute metrics, generate recommendations for the top 2 chains per drug, and identify the most common prescription quantities.

## Project Overview

Hippo is designed to process and analyze healthcare data. The application performs the following tasks:

1. **Data Loading and Validation:**

   Reads and validates data from three sources:

   * Pharmacies (CSV)
   * Claims (JSON)
   * Reverts (JSON)
2. **Metrics Computation:**

   Computes various metrics such as:

   * Count of claims (fills)
   * Total price of claims (rounded)
   * Average unit price (rounded)
   * Count of reverts
3. **Recommendations:**

   For each drug (`ndc`), the project identifies the top 2 chains (from pharmacy data) with the lowest average unit price.

4. **Common Prescription Quantities:**

   Identifies the most common prescription quantities per drug.

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/brow1998/hippo-challenge.git
   cd hippo-challenge
   ```

2. **Create a virtual environment (optional but recommended):**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage
Hippo provides a command-line interface (CLI) with the following commands:

**validate**: Validate input data files.

**metrics**: Generate metrics from data.

**recommend**: Generate top 2 chain recommendations per drug.

**common**: Generate the most common prescription quantities per drug.

All commands accept a global argument --data to specify the base directory where the input data folders are located. By default, Hippo looks for data in the `data/input` directory.

## Running the Commands
### Validate Data

To validate the input data, run:

`python -m hippo.cli validate`

data/input/pharmacies
data/input/claims
data/input/reverts
and log the number of valid rows for each dataset.

### Generate Metrics

To generate metrics and save the results to a directory (default is output), run:

`python -m hippo.cli metrics`

This command computes the metrics from claims and reverts data and saves the result as metrics.json in the specified output directory.

### Generate Recommendations

To generate top 2 chain recommendations per drug, run:

`python -m hippo.cli recommend`

This command computes recommendations and saves the result as top_chains.json in the specified output directory.

### Generate Common Quantities

To generate the most common prescription quantities per drug, run:

`python -m hippo.cli common`

This command computes the common prescription quantities and saves the result as most_prescribed_quantities.json in the specified output directory.

## Output

After running the commands, the following output files will be generated in the specified (or default) output directory:

**metrics.json**: Computed metrics from claims and reverts.
**top_chains.json**: Top 2 chains per drug based on average unit price.
**most_prescribed_quantities.json**: Most common prescription quantities per drug.