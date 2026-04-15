import json
import sqlite3
from pathlib import Path

from ingest_stock_intel import extract_tabular_signals, upsert_signal
from models.schemas import LEAD_COLUMNS_SQL


REFERENCE_CSV = Path(__file__).resolve().parents[1] / "reference" / "Bligh Park marketing report.csv"


def _create_leads_table(conn: sqlite3.Connection) -> None:
    columns = {**LEAD_COLUMNS_SQL, "contacts": "TEXT DEFAULT '[]'"}
    column_sql = ", ".join(f"{name} {definition}" for name, definition in columns.items())
    conn.execute(f"CREATE TABLE leads ({column_sql})")


def test_extract_tabular_signals_supports_marketing_report_csv() -> None:
    signals = extract_tabular_signals(REFERENCE_CSV)

    three_acres = [signal for signal in signals if signal.canonical_address == "3 Acres Pl"]

    assert len(three_acres) == 3

    by_name = {
        signal.contacts[0]["name"]: signal.contacts[0]
        for signal in three_acres
        if signal.contacts
    }

    assert by_name["Kres Matti"]["phone"] == "0475869773"
    assert by_name["Mark Ramm"]["gender"] == "male"
    assert by_name["Carol Williams"]["gender"] == "unknown"


def test_upsert_signal_merges_contacts_for_same_address() -> None:
    conn = sqlite3.connect(":memory:")
    _create_leads_table(conn)

    signals = [
        signal
        for signal in extract_tabular_signals(REFERENCE_CSV)
        if signal.canonical_address == "3 Acres Pl"
    ]

    for signal in signals:
        upsert_signal(conn, signal)

    upsert_signal(conn, signals[1])

    row = conn.execute(
        "SELECT owner_name, contact_phones, contacts FROM leads WHERE address = ?",
        ("3 Acres Pl",),
    ).fetchone()

    assert row is not None

    owner_name, contact_phones_json, contacts_json = row
    contacts = json.loads(contacts_json)
    contact_pairs = {(contact["name"], contact["phone"]) for contact in contacts}

    assert len(contacts) == 3
    assert contact_pairs == {
        ("Kres Matti", "0475869773"),
        ("Mark Ramm", "0411342742"),
        ("Carol Williams", ""),
    }
    assert json.loads(contact_phones_json) == ["0475869773", "0411342742"]
    assert set(owner_name.split(" & ")) == {"Kres Matti", "Mark Ramm", "Carol Williams"}
