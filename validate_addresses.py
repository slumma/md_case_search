#!/usr/bin/env python3
"""
Address validation using the USPS Address Validation API (free).

Registration: https://www.usps.com/business/web-tools-apis/
Once registered, set USPS_USER_ID in .env

Validates addresses for cases that have a street address but haven't been
validated yet. Results are stored permanently — each address is only
checked once.

Usage:
    python3 validate_addresses.py              # validate all pending
    python3 validate_addresses.py --limit 100  # validate up to N records
    python3 validate_addresses.py --dry-run    # count pending, no API calls
"""

import argparse
import os
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from dotenv import load_dotenv

import db

load_dotenv()

USPS_API_URL = "https://secure.shippingapis.com/ShippingAPI.dll"


def validate_address(user_id: str, street: str, city: str, state: str, zipcode: str) -> dict:
    """
    Call USPS Address Validation API for one address.
    Returns a dict with verdict and corrected components.
    """
    xml_request = (
        f'<AddressValidateRequest USERID="{user_id}">'
        f'<Revision>1</Revision>'
        f'<Address ID="0">'
        f'<Address1></Address1>'
        f'<Address2>{street}</Address2>'
        f'<City>{city}</City>'
        f'<State>{state}</State>'
        f'<Zip5>{zipcode[:5]}</Zip5>'
        f'<Zip4></Zip4>'
        f'</Address>'
        f'</AddressValidateRequest>'
    )

    resp = requests.get(
        USPS_API_URL,
        params={"API": "Verify", "XML": xml_request},
        timeout=10,
    )
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    address = root.find("Address")

    if address is None:
        return {"verdict": "Error", "corrected_street": "", "corrected_city": "",
                "corrected_state": "", "corrected_zip": ""}

    # Check for error response
    error = address.find("Error")
    if error is not None:
        desc = error.findtext("Description", "Unknown error")
        return {"verdict": f"Invalid — {desc}", "corrected_street": "",
                "corrected_city": "", "corrected_state": "", "corrected_zip": ""}

    def get(tag):
        return (address.findtext(tag) or "").strip().title()

    corrected_street = get("Address2")
    apt = (address.findtext("Address1") or "").strip()
    if apt:
        corrected_street = f"{corrected_street} {apt.title()}"

    zip5 = (address.findtext("Zip5") or "").strip()
    zip4 = (address.findtext("Zip4") or "").strip()
    corrected_zip = f"{zip5}-{zip4}" if zip4 else zip5

    # DPVConfirmation: Y=confirmed, S/D=partial, N=no match
    dpv = (address.findtext("DPVConfirmation") or "").strip()
    if dpv == "Y":
        verdict = "Confirmed"
    elif dpv in ("S", "D"):
        verdict = "Partial match"
    elif dpv == "N":
        verdict = "Not found"
    else:
        verdict = "Validated"  # older API response without DPV still means address is real

    return {
        "verdict":          verdict,
        "corrected_street": corrected_street,
        "corrected_city":   get("City"),
        "corrected_state":  (address.findtext("State") or "").strip().upper(),
        "corrected_zip":    corrected_zip,
    }


def run(limit: int | None = None, dry_run: bool = False):
    user_id = os.environ.get("USPS_USER_ID", "")
    if not user_id and not dry_run:
        print(
            "Error: USPS_USER_ID not set.\n"
            "Register free at https://www.usps.com/business/web-tools-apis/\n"
            "Then add USPS_USER_ID=your_id to .env",
            file=sys.stderr,
        )
        sys.exit(1)

    conn = db.get_conn()
    db.init_db(conn)

    query = """
        SELECT case_number, strftime(file_date, '%Y-%m-%d') AS file_date,
               address_street, address_city, address_state, address_zip
        FROM cases
        WHERE address_street != ''
          AND (addr_validated IS NULL OR addr_validated = FALSE)
        ORDER BY file_date DESC
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    pending = conn.execute(query).fetchall()
    print(f"Pending validation: {len(pending):,} records")

    if dry_run or not pending:
        conn.close()
        return

    ok = 0
    failed = 0

    for i, (case_number, file_date, street, city, state, zipcode) in enumerate(pending, 1):
        try:
            result = validate_address(user_id, street, city, state, zipcode)
            conn.execute("""
                UPDATE cases SET
                    addr_validated        = TRUE,
                    addr_verdict          = ?,
                    addr_corrected_street = ?,
                    addr_corrected_city   = ?,
                    addr_corrected_state  = ?,
                    addr_corrected_zip    = ?,
                    addr_validated_at     = now()
                WHERE case_number = ?
                  AND strftime(file_date, '%Y-%m-%d') = ?
            """, [
                result["verdict"],
                result["corrected_street"],
                result["corrected_city"],
                result["corrected_state"],
                result["corrected_zip"],
                case_number,
                file_date,
            ])
            ok += 1

            if i % 100 == 0:
                print(f"  {i:,}/{len(pending):,} validated...", flush=True)

            # USPS allows ~5 req/s for the free tier
            time.sleep(0.2)

        except requests.HTTPError as e:
            print(f"  API error on {case_number}: {e}", file=sys.stderr)
            failed += 1
        except Exception as e:
            print(f"  Error on {case_number}: {e}", file=sys.stderr)
            failed += 1

    conn.close()
    print(f"\nDone. {ok:,} validated, {failed:,} failed.")


def main():
    parser = argparse.ArgumentParser(description="Validate addresses via USPS API (free)")
    parser.add_argument("--limit",   type=int, default=None, help="Max records to validate")
    parser.add_argument("--dry-run", action="store_true",    help="Count pending only, no API calls")
    args = parser.parse_args()
    run(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
