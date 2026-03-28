#!/usr/bin/env python3
"""
MD Case Search Scraper
Downloads the daily public cases PDF from mdcourts.gov and parses it into CSV.

Usage:
    python3 scraper.py                   # today's report (yesterday's cases)
    python3 scraper.py 2026-03-28        # specific date
    python3 scraper.py --help
"""

import csv
import re
import subprocess
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PDF_URL_TEMPLATE = "https://www.mdcourts.gov/data/case/file{date}.pdf"
OUTPUT_DIR = Path("output")

# User-agent that works with the Cloudflare-protected site
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Case header: CASENUMBER   NAME   CASE TYPE   MM/DD/YYYY
# Case numbers may be short alphanumeric (traffic: 01P1BNK) or
# hyphenated court format (criminal: D-121-CR-26-000433)
CASE_NUMBER_RE = re.compile(r"^([A-Z0-9][A-Z0-9\-]{4,22})\s{2,}")
DATE_AT_END_RE = re.compile(r"(\d{2}/\d{2}/\d{4})\s*$")

# City, State ZIP  (e.g.  CUMBERLAND, MD 21502  or  BETHESDA, MD 20814-3212)
CITY_STATE_ZIP_RE = re.compile(
    r"^(.+?),\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$"
)

# Charge line:  1 - SOME CHARGE TEXT
CHARGE_RE = re.compile(r"^\d+\s+-\s+(.+)$")

# ---------------------------------------------------------------------------
# Court location → county mapping
# ---------------------------------------------------------------------------

COURT_TO_COUNTY = {
    "Allegany": "Allegany County",
    "Allegany Circuit Court": "Allegany County",
    "Annapolis": "Anne Arundel County",
    "Anne Arundel Circuit Court": "Anne Arundel County",
    "Appellate Court of Maryland": "Anne Arundel County",
    "Supreme Court of Maryland": "Anne Arundel County",
    "Baltimore City Circuit Court": "Baltimore City",
    "Eastside": "Baltimore City",
    "Fayette": "Baltimore City",
    "Hargrove": "Baltimore City",
    "Wabash": "Baltimore City",
    "Baltimore County Circuit Court": "Baltimore County",
    "Catonsville": "Baltimore County",
    "Essex": "Baltimore County",
    "Towson": "Baltimore County",
    "Calvert": "Calvert County",
    "Calvert Circuit Court": "Calvert County",
    "Caroline": "Caroline County",
    "Caroline Circuit Court": "Caroline County",
    "Carroll": "Carroll County",
    "Carroll Circuit Court": "Carroll County",
    "Cecil": "Cecil County",
    "Cecil Circuit Court": "Cecil County",
    "Charles": "Charles County",
    "Charles Circuit Court": "Charles County",
    "Dorchester": "Dorchester County",
    "Dorchester Circuit Court": "Dorchester County",
    "Frederick": "Frederick County",
    "Frederick Circuit Court": "Frederick County",
    "Garrett": "Garrett County",
    "Garrett Circuit Court": "Garrett County",
    "Glen Burnie": "Anne Arundel County",
    "Harford": "Harford County",
    "Harford Circuit Court": "Harford County",
    "Howard": "Howard County",
    "Howard Circuit Court": "Howard County",
    "Hyattsville": "Prince George's County",
    "Upper Marlboro": "Prince George's County",
    "Prince Georges Circuit Court": "Prince George's County",
    "Kent": "Kent County",
    "Kent Circuit Court": "Kent County",
    "Rockville": "Montgomery County",
    "Silver Spring": "Montgomery County",
    "Montgomery Circuit Court": "Montgomery County",
    "Ocean City": "Worcester County",
    "Snow Hill": "Worcester County",
    "Worcester Circuit Court": "Worcester County",
    "Queen Annes": "Queen Anne's County",
    "Queen Annes Circuit Court": "Queen Anne's County",
    "Saint Marys": "Saint Mary's County",
    "Saint Marys Circuit Court": "Saint Mary's County",
    "Somerset": "Somerset County",
    "Somerset Circuit Court": "Somerset County",
    "Talbot": "Talbot County",
    "Talbot Circuit Court": "Talbot County",
    "Washington": "Washington County",
    "Washington Circuit Court": "Washington County",
    "Wicomico": "Wicomico County",
    "Wicomico Circuit Court": "Wicomico County",
}

# Lines to always skip
SKIP_PATTERNS = [
    "AOC - Cases Filed Report",
    "Disclaimer:",
    "Reporting Period:",
    "Report Name:",
    "Run Date:",
    "Case Number",   # column headers
]

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def build_url(report_date: date) -> str:
    return PDF_URL_TEMPLATE.format(date=report_date.strftime("%Y-%m-%d"))


def download_pdf(report_date: date, dest: Path) -> Path:
    url = build_url(report_date)
    print(f"Downloading: {url}", file=sys.stderr)
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req) as resp:
        dest.write_bytes(resp.read())
    print(f"Saved to: {dest} ({dest.stat().st_size:,} bytes)", file=sys.stderr)
    return dest


# ---------------------------------------------------------------------------
# PDF → text
# ---------------------------------------------------------------------------


def pdf_to_text(pdf_path: Path) -> str:
    result = subprocess.run(
        ["pdftotext", "-layout", str(pdf_path), "-"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        raise RuntimeError(f"pdftotext failed: {result.stderr}")
    return result.stdout


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def should_skip(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return True
    for pat in SKIP_PATTERNS:
        if pat in line:
            return True
    # Page footer: "Page: N of M"
    if re.match(r"^\s*Page:\s+\d+", line):
        return True
    return False


def parse_case_header(line: str):
    """
    Parse a case header line.
    Returns (case_number, name, case_type, file_date) or None.

    The line looks like:
      01P1BNK    MORGAN, ELIJAH PAUL                     Citation - Traffic        03/27/2026
    Strategy: extract case number from left, date from right,
    then find the last large whitespace gap to split name from case_type.
    """
    stripped = line.strip()
    cn_match = CASE_NUMBER_RE.match(stripped)
    if not cn_match:
        return None
    date_match = DATE_AT_END_RE.search(stripped)
    if not date_match:
        return None

    case_number = cn_match.group(1)
    file_date = date_match.group(1)

    # Middle section: everything between the case number and the date
    middle = stripped[cn_match.end() : date_match.start()].rstrip()

    # Split middle on the last run of 3+ spaces to get name and case_type
    # e.g. "MORGAN, ELIJAH PAUL                     Citation - Traffic        "
    parts = re.split(r"\s{3,}", middle)
    parts = [p.strip() for p in parts if p.strip()]

    if len(parts) >= 2:
        name = parts[0]
        case_type = parts[-1]
    elif len(parts) == 1:
        name = parts[0]
        case_type = ""
    else:
        return None

    return case_number, name, case_type, file_date


def parse_name(raw: str):
    """
    Split 'LAST, FIRST MIDDLE' into (last, first_middle).
    Returns (last_name, first_name) where first_name may include middle.
    Handles suffixes like Jr., Sr., III appended after the first name.
    """
    raw = raw.strip()
    if "," in raw:
        last, rest = raw.split(",", 1)
        return last.strip().title(), rest.strip().title()
    return raw.title(), ""


def process_address_lines(lines: list[str]) -> dict:
    """
    Given the raw text lines collected from the indented address block
    (name already stripped by caller), parse into street / city / state / zip.

    Lines order: [name_line(s)..., street_line, city_state_zip]
    """
    result = {"address_street": "", "address_city": "", "address_state": "", "address_zip": ""}
    if not lines:
        return result

    # Find the city/state/zip line from the bottom
    csz_idx = None
    for i in range(len(lines) - 1, -1, -1):
        m = CITY_STATE_ZIP_RE.match(lines[i])
        if m:
            csz_idx = i
            result["address_city"] = m.group(1).strip().title()
            result["address_state"] = m.group(2)
            result["address_zip"] = m.group(3)
            break

    if csz_idx is None:
        # Can't parse — return whatever we have
        return result

    # Line immediately before city/state/zip is the street
    if csz_idx > 0:
        result["address_street"] = lines[csz_idx - 1].strip()

    return result


# ---------------------------------------------------------------------------
# Civil two-column extraction
# ---------------------------------------------------------------------------


def _extract_civil_def(line: str, civil_def_col: int) -> str:
    """
    Extract the defendant (right-column) text from a civil two-column address line.

    Strategy:
    1. If there is a run of 3+ spaces in the visible portion, split on the LAST
       such gap and return the rightmost chunk.  This handles the common case where
       plaintiff and defendant columns are clearly separated.
    2. If no 3+ gap exists (plaintiff text runs right up against defendant text),
       fall back to the known column position.  If that position falls mid-word,
       walk backwards to the nearest word boundary so we return the whole word.
    """
    visible = line.strip()
    if not visible:
        return ""

    # Strategy 1: gap-based split
    parts = re.split(r"\s{3,}", visible)
    if len(parts) >= 2:
        return parts[-1].strip()

    # Strategy 2: column slice with word-boundary walk-back
    if len(line) <= civil_def_col:
        return ""
    col = civil_def_col
    # If we landed mid-word, walk back to the preceding space
    while col > 0 and line[col - 1] not in (" ", "\t"):
        col -= 1
    result = line[col:].strip()
    # Sanity check: if the result looks like it's just the tail of plaintiff data
    # (i.e. nothing was in the defendant column), return empty.
    # A valid defendant chunk should be at least 3 chars.
    return result if len(result) >= 3 else ""


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


def parse_cases(text: str) -> list[dict]:
    """Parse the full pdftotext output into a list of case dicts."""
    records = []
    current_county = ""
    current_case = None
    address_lines = []
    civil_def_col = 0   # column where defendant data starts in two-column civil layout
    state = "SCAN"  # SCAN | IN_CASE | IN_ADDRESS | IN_CIVIL_ADDRESS | IN_CHARGES

    def finalize_case():
        nonlocal current_case, address_lines
        if current_case is None:
            return
        addr = process_address_lines(address_lines)
        current_case.update(addr)
        records.append(current_case)
        current_case = None
        address_lines = []

    for line in text.splitlines():
        if should_skip(line):
            continue

        stripped = line.strip()

        # ---- Address label(s) ----
        if "Defendant Address:" in line:
            if "Plaintiff Address:" in line:
                # Civil two-column layout: Plaintiff Address:   Defendant Address:
                # Record where the defendant column starts so we can slice each data line.
                civil_def_col = line.index("Defendant Address:")
                state = "IN_CIVIL_ADDRESS"
            else:
                # Criminal / Citation: single indented defendant address block
                state = "IN_ADDRESS"
            address_lines = []
            continue

        # ---- Charges label ----
        if stripped == "Charges:":
            state = "IN_CHARGES"
            continue

        # ---- Charge line ----
        charge_match = CHARGE_RE.match(stripped)
        if state == "IN_CHARGES" and charge_match:
            if current_case is not None:
                current_case["charges"].append(charge_match.group(1).strip())
            continue

        # ---- Single-column (criminal/citation) address lines ----
        if state == "IN_ADDRESS" and line.startswith(" ") and stripped:
            address_lines.append(stripped)
            continue

        # ---- Two-column (civil) address lines ----
        # Extract only the defendant (right-column) portion of each line.
        if state == "IN_CIVIL_ADDRESS" and line.startswith(" ") and stripped:
            def_part = _extract_civil_def(line, civil_def_col)
            if def_part:
                address_lines.append(def_part)
            continue

        # ---- Case header line (must start at column 0) ----
        parsed = parse_case_header(line) if not line.startswith(" ") else None
        if parsed:
            finalize_case()
            case_number, name, case_type, file_date = parsed
            last_name, first_name = parse_name(name)
            current_case = {
                "case_number": case_number,
                "defendant_name": name.title(),
                "last_name": last_name,
                "first_name": first_name,
                "case_type": case_type,
                "file_date": file_date,
                "court_location": current_county,
                "county": COURT_TO_COUNTY.get(current_county, ""),
                "address_street": "",
                "address_city": "",
                "address_state": "",
                "address_zip": "",
                "charges": [],
            }
            state = "IN_CASE"
            continue

        # ---- County / court location header ----
        # A non-indented, non-case-number line that isn't blank or a header
        if not line.startswith(" ") and stripped:
            # Reject lines that look like partial charge continuations
            if not CHARGE_RE.match(stripped) and not DATE_AT_END_RE.search(stripped):
                current_county = stripped
                state = "SCAN"
            continue

    finalize_case()
    return records


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "case_number",
    "file_date",
    "county",
    "court_location",
    "last_name",
    "first_name",
    "defendant_name",
    "case_type",
    "address_street",
    "address_city",
    "address_state",
    "address_zip",
    "charges",
]


def records_to_csv(records: list[dict], output_path: Path):
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            row = dict(rec)
            row["charges"] = " | ".join(rec.get("charges", []))
            writer.writerow(row)
    print(f"Wrote {len(records):,} records to {output_path}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    # Determine target date
    if len(sys.argv) > 1:
        try:
            report_date = date.fromisoformat(sys.argv[1])
        except ValueError:
            print(f"Error: invalid date '{sys.argv[1]}' — use YYYY-MM-DD", file=sys.stderr)
            sys.exit(1)
    else:
        # The PDF posted today contains yesterday's cases
        report_date = date.today()

    OUTPUT_DIR.mkdir(exist_ok=True)

    date_str = report_date.strftime("%Y-%m-%d")
    pdf_path = OUTPUT_DIR / f"cases-{date_str}.pdf"
    csv_path = OUTPUT_DIR / f"cases-{date_str}.csv"

    # Download (skip if already on disk)
    if not pdf_path.exists():
        try:
            download_pdf(report_date, pdf_path)
        except Exception as e:
            print(f"Download failed: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Using cached PDF: {pdf_path}", file=sys.stderr)

    # Parse
    print("Extracting text from PDF...", file=sys.stderr)
    text = pdf_to_text(pdf_path)

    print("Parsing cases...", file=sys.stderr)
    records = parse_cases(text)
    print(f"Parsed {len(records):,} cases", file=sys.stderr)

    # Export to CSV (archival backup)
    records_to_csv(records, csv_path)

    # Upsert into DuckDB
    import db as _db
    conn = _db.get_conn()
    _db.init_db(conn)
    _db.upsert_records(conn, records)
    conn.close()
    print(f"Upserted {len(records):,} records into {_db.DB_PATH}", file=sys.stderr)

    print(f"Done: {csv_path}")


if __name__ == "__main__":
    main()
