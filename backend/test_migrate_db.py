from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.dialects.postgresql import JSONB

from tools.migrate_db import conflict_targets_for_table, maybe_parse_json, resolve_table_order


def test_resolve_table_order_migrates_all_shared_tables_with_priority_first() -> None:
    source_tables = {"leads", "tasks", "notes", "call_log", "sqlite_sequence", "custom_table"}
    target_tables = {"leads", "tasks", "notes", "call_log", "custom_table", "other_only"}

    ordered = resolve_table_order(source_tables, target_tables)

    assert ordered[:3] == ["leads", "tasks", "notes"]
    assert ordered[3:] == ["call_log", "custom_table"]
    assert "sqlite_sequence" not in ordered


def test_maybe_parse_json_uses_target_column_type_even_without_manual_mapping() -> None:
    payload = '[{"message":"hello"}]'
    target_column = Column("messages", JSONB)

    parsed = maybe_parse_json(
        "some_table",
        "messages",
        payload,
        target_column=target_column,
    )

    assert parsed == [{"message": "hello"}]


def test_conflict_targets_include_primary_key_and_unique_indexes() -> None:
    metadata = MetaData()
    table = Table(
        "distress_sources",
        metadata,
        Column("id", String, primary_key=True),
        Column("source_key", String, unique=True),
        Column("label", String),
        Column("rank", Integer),
    )

    targets = conflict_targets_for_table(table)

    assert targets[0] == ["id"]
    assert ["source_key"] in targets
