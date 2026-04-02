from flask import Flask, jsonify, request
from flask_cors import CORS

from data_retrieval import fetch_contributions_export
from aggregate import (
    load_data,
    filter_pac_recipients,
    convert_amounts,
    aggregate_totals,
    build_rankings_json,
)

app = Flask(__name__)
CORS(app)


@app.route("/api/rankings", methods=["GET"])
def get_rankings():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    if not start_date or not end_date:
        return jsonify({"error": "start_date and end_date are required"}), 400

    try:
        fetch_contributions_export(
            start_date=start_date,
            end_date=end_date,
            committee_type="16",
            schedule_type="181",
        )

        df = load_data()
        df = filter_pac_recipients(df)
        df = convert_amounts(df)
        totals = aggregate_totals(df)
        result = build_rankings_json(totals)

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=False)