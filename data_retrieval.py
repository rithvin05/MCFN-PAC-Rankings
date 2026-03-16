import os
from datetime import date, datetime
from typing import Optional

import requests

BASE_URL = "https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do"
SEARCH_PAGE = "page.miboeContributionPublicSearch"
EXPORT_URL = f"{BASE_URL}?page={SEARCH_PAGE}&action=export"
DATE_FMT = "%Y-%m-%d"
DOWNLOAD_DIR = "downloads"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://mi-boe.entellitrak.com",
    "Referer": f"https://mi-boe.entellitrak.com/etk-mi-boe-prod/page.request.do?page={SEARCH_PAGE}",
    "Connection": "keep-alive",
}


def _parse_date(value: str | date, label: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(value, DATE_FMT).date()
    except ValueError as exc:
        raise ValueError(
            f"{label} must be a date object or 'YYYY-MM-DD' string, got: {value!r}"
        ) from exc



def _normalize_amount(value: Optional[int | float | str]) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        if isinstance(value, float) and not value.is_integer():
            return str(value)
        return f"{int(value):,}"
    return str(value)



def _validate_inputs(
    start_date: str | date,
    end_date: str | date,
    min_amount: Optional[int | float | str],
    max_amount: Optional[int | float | str],
) -> tuple[date, date]:
    start = _parse_date(start_date, "start_date")
    end = _parse_date(end_date, "end_date")

    if start > end:
        raise ValueError(f"start_date ({start}) must be on or before end_date ({end}).")
    if end > date.today():
        raise ValueError(f"end_date ({end}) cannot be in the future.")

    if min_amount not in (None, "") and max_amount not in (None, ""):
        try:
            min_num = float(str(min_amount).replace(",", ""))
            max_num = float(str(max_amount).replace(",", ""))
        except ValueError as exc:
            raise ValueError("min_amount and max_amount must be numeric.") from exc
        if min_num > max_num:
            raise ValueError("min_amount cannot be greater than max_amount.")

    return start, end



def _build_payload(
    start: date,
    end: date,
    *,
    min_amount: Optional[int | float | str] = None,
    max_amount: Optional[int | float | str] = None,
    contribution_type: str = "",
    contributor_name: str = "",
    committee_name: str = "",
    schedule_type: str = "181",
) -> dict[str, str]:
    return {
        "form.committeeName": committee_name,
        "form.committeeId": "",
        "form.committeeType": "",
        "form.committeeCandidateLastName": "",
        "form.committeeOfficeTitle": "",
        "form.committeePoliticalParty": "",
        "form.campaignStatementYear": "",
        "form.campaignCoverageYearBegin": "",
        "form.campaignCoverageYearEnd": "",
        "form.campaignStatementType": "",
        "form.campaignStatementName": "",
        "form.contributionType": contribution_type,
        "form.contributionAmountGreaterThan": _normalize_amount(min_amount),
        "form.contributionAmountLessThan": _normalize_amount(max_amount),
        "form.contributionSchedule": schedule_type,
        "form.contributionDateBegin": start.strftime(DATE_FMT),
        "form.contributionDateEnd": end.strftime(DATE_FMT),
        "form.contributorLastNamePac": contributor_name,
        "form.contributorFirstName": "",
        "form.contributorAddress": "",
        "form.contributorCity": "",
        "form.contributorState": "",
        "form.contributorZip": "",
        "form.contributorEmployer": "",
        "form.contributorOccupation": "",
        "form.contactType": "",
    }



def _detect_server_error(resp: requests.Response) -> Optional[str]:
    content_type = resp.headers.get("content-type", "").lower()
    if any(text_type in content_type for text_type in ("text/", "json", "html")):
        body = resp.text.lower()

        if "25,000" in body or "25000" in body:
            return (
                "The portal says the query returned more than 25,000 records. "
                "Narrow the date range or add more filters."
            )
        if "no records" in body or "no results" in body:
            return "The query completed, but no records were returned."
        return "The server returned text/HTML instead of an Excel file."
    return None



def fetch_contributions_export(
    start_date: str | date,
    end_date: str | date,
    *,
    min_amount: Optional[int | float | str] = None,
    max_amount: Optional[int | float | str] = None,
    contribution_type: str = "",
    contributor_name: str = "",
    committee_name: str = "",
    schedule_type: str = "181",
    out_dir: str = DOWNLOAD_DIR,
    out_name: Optional[str] = None,
    timeout: int = 90,
) -> Optional[str]:
    """
    Fetch contribution data from the Michigan BOE portal and save it as an Excel file.

    Returns the saved file path on success, or None on failure.
    """
    try:
        start, end = _validate_inputs(start_date, end_date, min_amount, max_amount)
    except ValueError as exc:
        print(f"Input error: {exc}")
        return None

    payload = _build_payload(
        start,
        end,
        min_amount=min_amount,
        max_amount=max_amount,
        contribution_type=contribution_type,
        contributor_name=contributor_name,
        committee_name=committee_name,
        schedule_type=schedule_type,
    )

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "contributions.xlsx")

    try:
        with requests.Session() as session:
            seed_url = f"{BASE_URL}?page={SEARCH_PAGE}"
            seed_resp = session.get(seed_url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=30)
            seed_resp.raise_for_status()

            with session.post(EXPORT_URL, data=payload, headers=HEADERS, stream=True, timeout=timeout) as resp:
                print(f"POST status: {resp.status_code}")
                resp.raise_for_status()

                error_message = _detect_server_error(resp)
                if error_message:
                    print(f"Fetch failed: {error_message}")
                    preview = resp.text[:500].strip()
                    if preview:
                        print("Response preview:")
                        print(preview)
                    return None

                with open(out_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

        print(f"Saved: {out_path}")
        return out_path

    except requests.exceptions.ReadTimeout:
        print(
            "Fetch timed out. This often means the query is too large. "
            "Try a smaller date range or more filters."
        )
        return None
    except requests.RequestException as exc:
        print(f"Request failed: {exc}")
        return None


if __name__ == "__main__":
    fetch_contributions_export(
        start_date="2026-01-01",
        end_date="2026-01-31",
        min_amount=1000,
        contribution_type="individual",
        schedule_type="181",
    )
