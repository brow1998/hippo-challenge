# Hippo

Hippo-Project processes data from pharmacies, claims, and reverts to compute metrics, generate recommendations for the top 2 chains per drug, and identify the most common prescription quantities.

## Project Overview

Hippo-Project is designed to process and analyze healthcare data. The application performs the following tasks:

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