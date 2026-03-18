import argparse
import csv
import json
import logging
from collections import defaultdict
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def aggregate_contributions(file_path):
    """
    Parse a CSV file containing Michigan PAC contribution data and aggregate
    contributions by receiving committee.

    Args:
        file_path (str): Path to the CSV file to parse.

    Returns:
        str: JSON string containing aggregated data sorted by total contribution
             amount in descending order. Each entry contains:
             - committee_name: The receiving committee name
             - total_amount: Sum of all contributions to that committee
             - count: Number of contributions to that committee
    """
    aggregated = defaultdict[Any, dict[str, float | int]](lambda: {"total_amount": 0.0, "count": 0})
    skipped_rows = 0
    row_number = 0

    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError("CSV file is empty or cannot be read")
        if "Receiving Committee Name" not in reader.fieldnames:
            raise ValueError("Required column 'Receiving Committee Name' not found in CSV")
        if "Amount of Contribution" not in reader.fieldnames:
            raise ValueError("Required column 'Amount of Contribution' not found in CSV")

        for row in reader:
            row_number += 1
            committee_name = row.get("Receiving Committee Name", "").strip()
            amount_str = row.get("Amount of Contribution", "").strip()

            if not committee_name:
                logger.warning(f"Row {row_number}: Skipped due to missing or empty 'Receiving Committee Name'")
                skipped_rows += 1
                continue

            amount = float(amount_str) if amount_str else 0.0

            aggregated[committee_name]["total_amount"] += amount
            aggregated[committee_name]["count"] += 1

    if skipped_rows > 0:
        logger.warning(f"Total rows skipped: {skipped_rows}")
    logger.info(f"Successfully processed {row_number - skipped_rows} rows")

    result = [
        {
            "committee_name": committee_name,
            "total_amount": data["total_amount"],
            "count": data["count"],
        }
        for committee_name, data in aggregated.items()
    ]

    result.sort(key=lambda x: x["total_amount"], reverse=True)

    return json.dumps(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate Michigan PAC contribution data by receiving committee."
    )
    parser.add_argument(
        "file_path",
        nargs="?",
        default="downloads/contribution.csv",
        help="Path to the CSV file containing contribution data. Default: 'downloads/contribution.csv'",
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
