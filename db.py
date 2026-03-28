#!/usr/bin/env python3
"""
Database module for MD Case Scraper.
All DuckDB interactions go through this module.
"""

from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path("cases.duckdb")

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH), read_only=read_only)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cases (
            case_number     TEXT    NOT NULL,
            file_date       DATE    NOT NULL,
            county          TEXT    NOT NULL DEFAULT '',
            court_location  TEXT    NOT NULL DEFAULT '',
            last_name       TEXT    NOT NULL DEFAULT '',
            first_name      TEXT    NOT NULL DEFAULT '',
            defendant_name  TEXT    NOT NULL DEFAULT '',
            case_type       TEXT    NOT NULL DEFAULT '',
            address_street  TEXT    NOT NULL DEFAULT '',
            address_city    TEXT    NOT NULL DEFAULT '',
            address_state   TEXT    NOT NULL DEFAULT '',
            address_zip     TEXT    NOT NULL DEFAULT '',
            charges_flat    TEXT    NOT NULL DEFAULT '',
            scraped_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (case_number, file_date)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS case_charges (
            case_number  TEXT NOT NULL,
            file_date    DATE NOT NULL,
            charge_seq   INT  NOT NULL,
            charge_text  TEXT NOT NULL,
            PRIMARY KEY (case_number, file_date, charge_seq)
        )
    """)

    # Indexes for common lookup patterns
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_cases_last_name   ON cases(last_name)",
        "CREATE INDEX IF NOT EXISTS idx_cases_file_date   ON cases(file_date)",
        "CREATE INDEX IF NOT EXISTS idx_cases_county      ON cases(county)",
        "CREATE INDEX IF NOT EXISTS idx_cases_case_type   ON cases(case_type)",
        "CREATE INDEX IF NOT EXISTS idx_charges_text      ON case_charges(charge_text)",
    ]:
        conn.execute(ddl)


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def upsert_records(conn: duckdb.DuckDBPyConnection, records: list[dict]) -> int:
    """
    Upsert a list of case dicts into the database.
    Returns the number of rows processed.
    """
    if not records:
        return 0

    case_rows = []
    charge_rows = []

    for rec in records:
        charges_list = rec.get("charges", [])
        charges_flat = " | ".join(charges_list)

        # Normalize file_date from MM/DD/YYYY to YYYY-MM-DD
        raw_date = rec.get("file_date", "")
        try:
            from datetime import datetime
            file_date = datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            file_date = raw_date

        case_rows.append((
            rec.get("case_number", ""),
            file_date,
            rec.get("county", ""),
            rec.get("court_location", ""),
            rec.get("last_name", ""),
            rec.get("first_name", ""),
            rec.get("defendant_name", ""),
            rec.get("case_type", ""),
            rec.get("address_street", ""),
            rec.get("address_city", ""),
            rec.get("address_state", ""),
            rec.get("address_zip", ""),
            charges_flat,
        ))

        for seq, charge in enumerate(charges_list, start=1):
            charge_rows.append((
                rec.get("case_number", ""),
                file_date,
                seq,
                charge.strip(),
            ))

    conn.execute("BEGIN")
    try:
        # DuckDB implements ON CONFLICT DO UPDATE as delete+insert internally,
        # so we must clear case_charges for every case in the batch before
        # upserting cases — not just the ones that happen to have charges.
        all_pairs = {(r[0], r[1]) for r in case_rows}
        for case_number, file_date in all_pairs:
            conn.execute(
                "DELETE FROM case_charges WHERE case_number = ? AND file_date = ?",
                [case_number, file_date],
            )

        conn.executemany("""
            INSERT INTO cases (
                case_number, file_date, county, court_location,
                last_name, first_name, defendant_name, case_type,
                address_street, address_city, address_state, address_zip,
                charges_flat, scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (case_number, file_date) DO UPDATE SET
                county         = excluded.county,
                court_location = excluded.court_location,
                last_name      = excluded.last_name,
                first_name     = excluded.first_name,
                defendant_name = excluded.defendant_name,
                case_type      = excluded.case_type,
                address_street = excluded.address_street,
                address_city   = excluded.address_city,
                address_state  = excluded.address_state,
                address_zip    = excluded.address_zip,
                charges_flat   = excluded.charges_flat,
                scraped_at     = now()
        """, case_rows)

        if charge_rows:
            conn.executemany("INSERT INTO case_charges VALUES (?, ?, ?, ?)", charge_rows)

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return len(case_rows)


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def available_dates(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Return all filing dates present in the DB, newest first."""
    rows = conn.execute("""
        SELECT DISTINCT strftime(file_date, '%Y-%m-%d') AS d
        FROM cases
        ORDER BY d DESC
    """).fetchall()
    return [r[0] for r in rows]


def query_date(conn: duckdb.DuckDBPyConnection, file_date: str) -> pd.DataFrame:
    """Return all cases for a given YYYY-MM-DD date as a DataFrame."""
    df = conn.execute("""
        SELECT
            case_number,
            strftime(file_date, '%m/%d/%Y') AS file_date,
            county,
            court_location,
            last_name,
            first_name,
            defendant_name,
            case_type,
            address_street,
            address_city,
            address_state,
            address_zip,
            charges_flat AS charges
        FROM cases
        WHERE strftime(file_date, '%Y-%m-%d') = ?
        ORDER BY county, last_name, first_name
    """, [file_date]).df()
    return df.fillna("")


def query_trends(conn: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    """Return DataFrames used by the Trends tab."""

    volume = conn.execute("""
        SELECT
            strftime(file_date, '%Y-%m-%d') AS date,
            county,
            COUNT(*) AS cases
        FROM cases
        WHERE county != ''
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()

    type_mix = conn.execute("""
        SELECT
            strftime(file_date, '%Y-%m-%d') AS date,
            case_type,
            COUNT(*) AS cases
        FROM cases
        WHERE case_type != ''
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """).df()

    repeat_offenders = conn.execute("""
        SELECT
            defendant_name,
            last_name,
            first_name,
            COUNT(DISTINCT file_date) AS filing_days,
            COUNT(*)                  AS total_cases,
            MIN(strftime(file_date, '%Y-%m-%d')) AS first_seen,
            MAX(strftime(file_date, '%Y-%m-%d')) AS last_seen
        FROM cases
        WHERE defendant_name != ''
        GROUP BY 1, 2, 3
        HAVING COUNT(DISTINCT file_date) > 1
        ORDER BY filing_days DESC, total_cases DESC
        LIMIT 200
    """).df()

    top_charges = conn.execute("""
        SELECT
            charge_text,
            COUNT(*) AS occurrences,
            COUNT(DISTINCT strftime(file_date, '%Y-%m-%d')) AS days_seen
        FROM case_charges
        WHERE charge_text != ''
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 30
    """).df()

    return {
        "volume": volume,
        "type_mix": type_mix,
        "repeat_offenders": repeat_offenders,
        "top_charges": top_charges,
    }
