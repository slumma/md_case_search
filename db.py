#!/usr/bin/env python3
"""
Database module for MD Case Scraper.
All DuckDB interactions go through this module.
"""

import uuid
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
            case_number           TEXT    NOT NULL,
            file_date             DATE    NOT NULL,
            county                TEXT    NOT NULL DEFAULT '',
            court_location        TEXT    NOT NULL DEFAULT '',
            last_name             TEXT    NOT NULL DEFAULT '',
            first_name            TEXT    NOT NULL DEFAULT '',
            defendant_name        TEXT    NOT NULL DEFAULT '',
            case_type             TEXT    NOT NULL DEFAULT '',
            address_street        TEXT    NOT NULL DEFAULT '',
            address_city          TEXT    NOT NULL DEFAULT '',
            address_state         TEXT    NOT NULL DEFAULT '',
            address_zip           TEXT    NOT NULL DEFAULT '',
            charges_flat          TEXT    NOT NULL DEFAULT '',
            scraped_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
            -- Address validation columns (populated by validate_addresses.py)
            addr_validated        BOOLEAN DEFAULT FALSE,
            addr_verdict          TEXT    DEFAULT '',
            addr_corrected_street TEXT    DEFAULT '',
            addr_corrected_city   TEXT    DEFAULT '',
            addr_corrected_state  TEXT    DEFAULT '',
            addr_corrected_zip    TEXT    DEFAULT '',
            addr_validated_at     TIMESTAMPTZ DEFAULT NULL,
            PRIMARY KEY (case_number, file_date)
        )
    """)

    # Add columns to existing DBs that predate schema changes
    for col, typedef in [
        ("addr_validated",        "BOOLEAN DEFAULT FALSE"),
        ("addr_verdict",          "TEXT DEFAULT ''"),
        ("addr_corrected_street", "TEXT DEFAULT ''"),
        ("addr_corrected_city",   "TEXT DEFAULT ''"),
        ("addr_corrected_state",  "TEXT DEFAULT ''"),
        ("addr_corrected_zip",    "TEXT DEFAULT ''"),
        ("addr_validated_at",     "TIMESTAMPTZ DEFAULT NULL"),
        ("source_state",          "TEXT DEFAULT 'MD'"),
    ]:
        try:
            conn.execute(f"ALTER TABLE cases ADD COLUMN {col} {typedef}")
        except Exception:
            pass  # column already exists

    conn.execute("""
        CREATE TABLE IF NOT EXISTS case_charges (
            case_number  TEXT NOT NULL,
            file_date    DATE NOT NULL,
            charge_seq   INT  NOT NULL,
            charge_text  TEXT NOT NULL,
            PRIMARY KEY (case_number, file_date, charge_seq)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS lead_exports (
            id            INTEGER PRIMARY KEY,
            exported_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            client_label  TEXT NOT NULL DEFAULT '',
            case_number   TEXT NOT NULL,
            file_date     DATE NOT NULL,
            source_state  TEXT NOT NULL DEFAULT 'MD',
            export_batch  TEXT NOT NULL DEFAULT ''
        )
    """)

    # Indexes for common lookup patterns
    for ddl in [
        "CREATE INDEX IF NOT EXISTS idx_cases_last_name    ON cases(last_name)",
        "CREATE INDEX IF NOT EXISTS idx_cases_file_date    ON cases(file_date)",
        "CREATE INDEX IF NOT EXISTS idx_cases_county       ON cases(county)",
        "CREATE INDEX IF NOT EXISTS idx_cases_case_type    ON cases(case_type)",
        "CREATE INDEX IF NOT EXISTS idx_charges_text       ON case_charges(charge_text)",
        "CREATE INDEX IF NOT EXISTS idx_exports_batch      ON lead_exports(export_batch)",
        "CREATE INDEX IF NOT EXISTS idx_exports_case       ON lead_exports(case_number, file_date)",
    ]:
        conn.execute(ddl)

    # source_state index — only possible after the migration column has been added
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cases_source_state ON cases(source_state)")
    except Exception:
        pass


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
            rec.get("source_state", "MD"),
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
                charges_flat, scraped_at, source_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
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
                source_state   = excluded.source_state,
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
            charges_flat AS charges,
            COALESCE(addr_verdict, '')          AS addr_verdict,
            COALESCE(addr_corrected_street, '') AS addr_corrected_street,
            COALESCE(addr_corrected_city, '')   AS addr_corrected_city,
            COALESCE(addr_corrected_state, '')  AS addr_corrected_state,
            COALESCE(addr_corrected_zip, '')    AS addr_corrected_zip
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


# ---------------------------------------------------------------------------
# Leads
# ---------------------------------------------------------------------------

def query_leads(
    conn: duckdb.DuckDBPyConnection,
    state: str = "MD",
    county: str = "",
    case_type: str = "",
    charge_category: str = "",
    date_from: str = "",
    date_to: str = "",
    has_address: bool = False,
    addr_validated: bool = False,
    not_exported: bool = False,
    limit: int = 500,
) -> pd.DataFrame:
    """
    Return filtered leads from the cases table.
    charge_category is a keyword matched against charges_flat (case-insensitive).
    """
    conditions = ["c.source_state = ?"]
    params: list = [state]

    if county:
        conditions.append("c.county = ?")
        params.append(county)
    if case_type:
        conditions.append("c.case_type = ?")
        params.append(case_type)
    if charge_category:
        conditions.append("c.charges_flat ILIKE ?")
        params.append(f"%{charge_category}%")
    if date_from:
        conditions.append("c.file_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("c.file_date <= ?")
        params.append(date_to)
    if has_address:
        conditions.append("c.address_street != ''")
    if addr_validated:
        conditions.append("c.addr_validated = TRUE")
    if not_exported:
        conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM lead_exports le
                WHERE le.case_number = c.case_number AND le.file_date = c.file_date
            )
        """)

    where = " AND ".join(conditions)
    params.append(limit)

    df = conn.execute(f"""
        SELECT
            c.case_number,
            strftime(c.file_date, '%Y-%m-%d')  AS file_date,
            c.source_state,
            c.county,
            c.case_type,
            c.defendant_name,
            c.address_street,
            c.address_city,
            c.address_state,
            c.address_zip,
            c.charges_flat,
            c.addr_validated,
            c.addr_verdict,
            c.addr_corrected_street,
            c.addr_corrected_city,
            c.addr_corrected_state,
            c.addr_corrected_zip,
            COUNT(le.id) AS times_exported
        FROM cases c
        LEFT JOIN lead_exports le
               ON le.case_number = c.case_number AND le.file_date = c.file_date
        WHERE {where}
        GROUP BY
            c.case_number, c.file_date, c.source_state, c.county, c.case_type,
            c.defendant_name, c.address_street, c.address_city, c.address_state,
            c.address_zip, c.charges_flat, c.addr_validated, c.addr_verdict,
            c.addr_corrected_street, c.addr_corrected_city,
            c.addr_corrected_state, c.addr_corrected_zip
        ORDER BY c.file_date DESC, c.county, c.defendant_name
        LIMIT ?
    """, params).df()

    return df.fillna("")


def record_export(
    conn: duckdb.DuckDBPyConnection,
    client_label: str,
    records: list[dict],
    state: str = "MD",
) -> str:
    """
    Record a batch export to lead_exports. Returns the batch UUID.
    """
    batch_id = str(uuid.uuid4())
    rows = [
        (client_label, r["case_number"], r["file_date"], state, batch_id)
        for r in records
        if r.get("case_number") and r.get("file_date")
    ]
    if rows:
        conn.executemany(
            "INSERT INTO lead_exports (client_label, case_number, file_date, source_state, export_batch) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
    return batch_id


def export_history(conn: duckdb.DuckDBPyConnection, limit: int = 100) -> pd.DataFrame:
    """Return recent export batches grouped by batch ID."""
    return conn.execute("""
        SELECT
            export_batch,
            MAX(strftime(exported_at, '%Y-%m-%d %H:%M')) AS exported_at,
            MAX(client_label)  AS client_label,
            source_state,
            COUNT(*)           AS lead_count
        FROM lead_exports
        GROUP BY export_batch, source_state
        ORDER BY MAX(exported_at) DESC
        LIMIT ?
    """, [limit]).df().fillna("")
