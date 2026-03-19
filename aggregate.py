import argparse
import json
import logging

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def aggregate_contributions(file_path):
    """
    Parse an Excel file containing Michigan PAC contribution data and aggregate
    contributions by receiving committee.

    Args:
        file_path (str): Path to the Excel (.xlsx) file to parse.

    Returns:
        str: JSON string containing aggregated data sorted by total contribution
             amount in descending order. Each entry contains:
             - committee_name: The receiving committee name
             - total_amount: Sum of all contributions to that committee
             - count: Number of contributions to that committee

    Raises:
        FileNotFoundError: If the specified file does not exist.
        ValueError: If required columns are missing from the spreadsheet.
    """
    logger.info(f"Loading Excel file: {file_path}")
    df = pd.read_excel(file_path)

    if "Receiving Committee Name" not in df.columns:
        raise ValueError("Required column 'Receiving Committee Name' not found in the spreadsheet")

    if "Amount of Contribution" not in df.columns:
        raise ValueError("Required column 'Amount of Contribution' not found in the spreadsheet")

    initial_rows = len(df)
    rows_with_missing_committee = df["Receiving Committee Name"].isna().sum() + (
        df["Receiving Committee Name"] == ""
    ).sum()
    if rows_with_missing_committee > 0:
        logger.warning(f"Found {rows_with_missing_committee} rows with missing or empty 'Receiving Committee Name'")

    df = df.dropna(subset=["Receiving Committee Name"])
    df = df[df["Receiving Committee Name"] != ""]
    skipped_rows = initial_rows - len(df)
    if skipped_rows > 0:
        logger.warning(f"Skipped {skipped_rows} rows due to missing committee names")

    aggregated = (
        df.groupby("Receiving Committee Name")
        .agg(
            total_amount=("Amount of Contribution", "sum"),
            count=("Amount of Contribution", "count"),
        )
        .reset_index()
    )

    aggregated.columns = ["committee_name", "total_amount", "count"]

    aggregated = aggregated.sort_values("total_amount", ascending=False)

    result = aggregated.to_dict(orient="records")

    logger.info(f"Successfully processed {len(df)} rows into {len(result)} committees")

    return json.dumps(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate Michigan PAC contribution data by receiving committee."
    )
    parser.add_argument(
        "file_path",
        nargs="?",
        default="downloads/contributions.xlsx",
        help="Path to the Excel file containing contribution data. Default: 'downloads/contributions.xlsx'",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level).",
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    output = aggregate_contributions(args.file_path)
    print(output)
