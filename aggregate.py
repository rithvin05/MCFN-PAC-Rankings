import os
import json
from datetime import date

import pandas as pd

DOWNLOAD_PATH = os.path.join("downloads", "contributions.xlsx")
OUTPUT_PATH = os.path.join("downloads", "pac_rankings.json")


def load_data():
    """Load the downloaded Excel contribution data."""
    if not os.path.exists(DOWNLOAD_PATH):
        raise FileNotFoundError(
            f"File not found: {DOWNLOAD_PATH}. Run retrieval.py first."
        )

    return pd.read_excel(DOWNLOAD_PATH)


def filter_pac_recipients(df):
    """Keep only rows where the receiving committee type is Political."""
    col = df["Receiving Committee Type"].astype(str).str.strip().str.upper()
    return df[col == "POLITICAL"].copy()


def convert_amounts(df):
    """Convert contribution amounts to numeric safely."""
    df["Amount of Contribution"] = pd.to_numeric(
        df["Amount of Contribution"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    df = df.dropna(subset=["Amount of Contribution"])
    return df


def aggregate_totals(df):
    """Aggregate total $ and count of contributions per PAC."""
    grouped = df.groupby("Receiving Committee Name").agg(
        total_contributions=("Amount of Contribution", "sum"),
        num_contributions=("Amount of Contribution", "count"),
    )

    grouped = grouped.sort_values(by="total_contributions", ascending=False)

    return grouped


def build_rankings_json(grouped):
    """Convert aggregated data into ranked JSON structure."""
    rankings = []

    for i, (committee, row) in enumerate(grouped.iterrows(), start=1):
        rankings.append(
            {
                "rank": i,
                "committee_name": committee,
                "total_amount": round(float(row["total_contributions"]), 2),
                "num_contributions": int(row["num_contributions"]),
            }
        )

    return {
        "generated_at": str(date.today()),
        "rankings": rankings,
    }


def save_json(data):
    """Save rankings JSON for the frontend."""
    os.makedirs("downloads", exist_ok=True)

    with open(OUTPUT_PATH, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nSaved PAC rankings JSON to: {OUTPUT_PATH}")


def main():
    df = load_data()

    print("Rows before filter:", len(df))
    print("\nReceiving Committee Type counts:")
    print(df["Receiving Committee Type"].astype(str).str.strip().value_counts().head(10))

    df = filter_pac_recipients(df)
    print("\nRows after Political filter:", len(df))

    df = convert_amounts(df)

    totals = aggregate_totals(df)
    print("Number of ranked committees:", len(totals))

    rankings_json = build_rankings_json(totals)
    save_json(rankings_json)

    print("\nTop 10 PAC Rankings\n")
    for r in rankings_json["rankings"][:10]:
        print(
            f"{r['rank']}. {r['committee_name']} — "
            f"${r['total_amount']:,.2f} "
            f"({r['num_contributions']} contributions)"
        )


if __name__ == "__main__":
    main()