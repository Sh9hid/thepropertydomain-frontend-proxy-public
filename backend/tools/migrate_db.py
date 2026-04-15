from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import JSON, MetaData, Table, UniqueConstraint, create_engine, inspect, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import SQLModel

BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from models.sql_models import (  # noqa: E402,F401
    Agent,
    Appointment,
    CommunicationAccount,
    Lead,
    LeadNote,
    SoldEvent,
    Task,
)


DEFAULT_SQLITE_PATH = REPO_ROOT / "leads.db"
TABLE_ORDER = [
    "leads",
    "tasks",
    "appointments",
    "sold_events",
    "communication_accounts",
    "agents",
    "notes",
]

JSON_COLUMNS = {
    "leads": {
        "potential_contacts",
        "contact_emails",
        "contact_phones",
        "key_details",
        "property_images",
        "features",
        "summary_points",
        "risk_flags",
        "source_tags",
        "next_actions",
        "source_evidence",
        "linked_files",
        "stage_note_history",
        "activity_log",
    },
    "sold_events": {"matched_lead_ids"},
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate SQLite data into PostgreSQL.")
    parser.add_argument(
        "--sqlite-path",
        default=str(DEFAULT_SQLITE_PATH),
        help="Path to the SQLite source database. Defaults to the repo-level leads.db.",
    )
    parser.add_argument(
        "--database-url",
        required=True,
        help="Target PostgreSQL SQLAlchemy URL. Use postgresql+psycopg2://...",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows to upsert per batch.",
    )
    return parser.parse_args()


def normalize_database_url(url: str) -> str:
    normalized = url.strip()
    if normalized.startswith("postgres://"):
        normalized = normalized.replace("postgres://", "postgresql+psycopg2://", 1)
    elif normalized.startswith("postgresql://"):
        normalized = normalized.replace("postgresql://", "postgresql+psycopg2://", 1)
    else:
        normalized = normalized.replace("postgresql+asyncpg://", "postgresql+psycopg2://", 1)
    return normalized


def chunked(items: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def sqlite_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0] for row in rows}


def sqlite_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def resolve_table_order(source_tables: set[str], target_tables: set[str]) -> list[str]:
    common = (source_tables & target_tables) - {"sqlite_sequence"}
    ordered = [table_name for table_name in TABLE_ORDER if table_name in common]
    remaining = sorted(common - set(ordered))
    return ordered + remaining


def conflict_targets_for_table(table: Table) -> list[list[str]]:
    targets: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()

    pk_columns = [column.name for column in table.primary_key.columns]
    if pk_columns:
        pk_tuple = tuple(pk_columns)
        seen.add(pk_tuple)
        targets.append(pk_columns)

    for column in table.columns:
        if column.unique:
            unique_target = (column.name,)
            if unique_target not in seen:
                seen.add(unique_target)
                targets.append([column.name])

    for constraint in table.constraints:
        if isinstance(constraint, UniqueConstraint):
            cols = tuple(column.name for column in constraint.columns)
            if cols and cols not in seen:
                seen.add(cols)
                targets.append(list(cols))

    for index in table.indexes:
        if not index.unique:
            continue
        cols = tuple(column.name for column in index.columns)
        if cols and cols not in seen:
            seen.add(cols)
            targets.append(list(cols))

    return targets


def _target_column_is_json(target_column: Any | None) -> bool:
    if target_column is None:
        return False
    column_type = getattr(target_column, "type", None)
    return isinstance(column_type, (JSON, JSONB))


def maybe_parse_json(table_name: str, column_name: str, value: Any, target_column: Any | None = None) -> Any:
    if value is None:
        return None
    should_parse = _target_column_is_json(target_column) or column_name in JSON_COLUMNS.get(table_name, set())
    if not should_parse:
        return value
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return [] if column_name.endswith("s") else None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def ensure_target_schema(engine) -> None:
    SQLModel.metadata.create_all(engine)


def reflect_table(engine, table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def fetch_rows(conn: sqlite3.Connection, table_name: str, columns: list[str]) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    output: list[dict[str, Any]] = []
    for row in rows:
        item = {}
        for column in columns:
            item[column] = maybe_parse_json(table_name, column, row[column])
        output.append(item)
    return output


def upsert_rows(engine, table: Table, rows: list[dict[str, Any]], batch_size: int) -> int:
    pk_columns = [column.name for column in table.primary_key.columns]
    written = 0
    with engine.begin() as conn:
        for batch in chunked(rows, batch_size):
            if not batch:
                continue
            stmt = pg_insert(table).values(batch)
            if pk_columns:
                update_columns = {
                    column.name: stmt.excluded[column.name]
                    for column in table.columns
                    if column.name not in pk_columns
                }
                stmt = stmt.on_conflict_do_update(index_elements=pk_columns, set_=update_columns)
            conn.execute(stmt)
            written += len(batch)
    return written


def main() -> int:
    args = parse_args()
    sqlite_path = Path(args.sqlite_path).resolve()
    if not sqlite_path.exists():
        print(f"[migrate_db] SQLite source not found: {sqlite_path}")
        return 1

    database_url = normalize_database_url(args.database_url)
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row

    target_engine = create_engine(database_url, future=True)
    ensure_target_schema(target_engine)

    existing_tables = sqlite_tables(sqlite_conn)
    target_inspector = inspect(target_engine)

    migrated_counts: dict[str, int] = {}
    for table_name in TABLE_ORDER:
        if table_name not in existing_tables:
            continue
        if not target_inspector.has_table(table_name):
            continue

        source_columns = sqlite_columns(sqlite_conn, table_name)
        target_table = reflect_table(target_engine, table_name)
        common_columns = [column.name for column in target_table.columns if column.name in source_columns]
        if not common_columns:
            continue

        rows = fetch_rows(sqlite_conn, table_name, common_columns)
        migrated_counts[table_name] = upsert_rows(target_engine, target_table, rows, args.batch_size)

    sqlite_conn.close()

    with target_engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS intelligence"))
        conn.commit()

    print("[migrate_db] Migration complete")
    for table_name in TABLE_ORDER:
        if table_name in migrated_counts:
            print(f"  - {table_name}: {migrated_counts[table_name]} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
