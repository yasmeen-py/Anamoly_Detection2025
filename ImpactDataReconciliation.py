import pandas as pd

# Step 1: Load the datasets
catalyst_path = "Catalyst_data.csv"  # Reference dataset
impact_path = "Impact_data_sample.csv"  # Target dataset
output_path = "impact_fixed.csv"  # Path to save the corrected dataset

catalyst = pd.read_csv(catalyst_path)
impact = pd.read_csv(impact_path)

# Step 2: Perform an outer join on Trade_ID to align both datasets
reconciliation = pd.merge(
    catalyst,
    impact,
    on="TRADEID",
    how="outer",
    suffixes=("_catalyst", "_impact"),
    indicator=True  # Adds a column to indicate source (e.g., 'both', 'left_only', etc.)
)
print(reconciliation.head())
# Step 3: Identify and fix mismatched or missing data
criteria_columns = ["PRICE", "QUANTITY", "INVENTORY", "CUSIP", "TRADE_DATE", "SETTLE_DATE", "BUY_SELL"]


# Create a flag for mismatched fields, and fix them based on the reference data
def fix_anomalies(row):
    for column in criteria_columns:
        col_catalyst = f"{column}_catalyst"
        col_impact = f"{column}_impact"
        # If values don't match, or missing in impact, update with catalyst data
        if pd.isna(row[col_impact]) or row[col_catalyst] != row[col_impact]:
            row[col_impact] = row[col_catalyst]
    return row


# Apply the fix_anomalies function row-wise
reconciliation = reconciliation.apply(fix_anomalies, axis=1)

# Step 4: Save the corrected impact dataset
# Keep only the fixed target data (columns ending with '_impact')
fixed_columns = ["TRADEID"] + [f"{column}_impact" for column in criteria_columns]
impact_fixed = reconciliation[fixed_columns]

# Rename columns to remove '_impact' suffix
impact_fixed.columns = ["TRADEID"] + criteria_columns

# Save the corrected dataset as a new CSV file
impact_fixed.to_csv(output_path, index=False)

# Display a preview of the corrected dataset
print("Corrected dataset saved to:", output_path)
print(impact_fixed.head())