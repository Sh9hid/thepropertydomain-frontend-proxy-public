import csv
import sys
import uuid
from pathlib import Path


SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from id4me_worker import (  # noqa: E402
    build_manual_enrich_payload,
    clean_text,
    extract_emails,
    extract_phones,
    merge_contacts,
    normalize_phone,
    parse_csv_file,
)


def test_normalize_phone_handles_au_formats():
    assert normalize_phone("+61 438 584 629") == "0438584629"
    assert normalize_phone("0426 206 506") == "0426206506"


def test_extract_contact_fields_from_text():
    text = "Aaron MacKey 0438 584 629 amack04@hotmail.com"
    assert extract_phones(text) == ["0438584629"]
    assert extract_emails(text) == ["amack04@hotmail.com"]


def test_merge_contacts_combines_shared_email_and_phone():
    raw = [
        {
            "name": "Aaron MacKey",
            "date_of_birth": None,
            "phones": ["0438584629"],
            "emails": ["amack04@hotmail.com"],
        },
        {
            "name": "Aaron Mackey",
            "date_of_birth": "1980-05-10",
            "phones": ["0438584629", "0411222333"],
            "emails": [],
        },
        {
            "name": "Tanaya Luscombe",
            "date_of_birth": "1995-01-24",
            "phones": [],
            "emails": ["t.luscombe@hotmail.com"],
        },
    ]

    merged = merge_contacts(raw)

    assert len(merged) == 2
    aaron = next(item for item in merged if "0438584629" in item["phones"])
    assert aaron["emails"] == ["amack04@hotmail.com"]
    assert aaron["phones"] == ["0411222333", "0438584629"]
    assert aaron["date_of_birth"] == "1980-05-10"


def test_parse_csv_file_filters_contactless_rows():
    csv_path = Path(__file__).resolve().parent / f"test_export_{uuid.uuid4().hex}.csv"
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["Name", "Date of Birth", "Mobile", "Email", "Landline"],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "Name": "Aimee Newby",
                    "Date of Birth": "",
                    "Mobile": "0426 206 506",
                    "Email": "spoiltrottendesigns@gmail.com",
                    "Landline": "",
                }
            )
            writer.writerow(
                {
                    "Name": "Regina Lee Wyborn",
                    "Date of Birth": "",
                    "Mobile": "",
                    "Email": "",
                    "Landline": "",
                }
            )
            writer.writerow(
                {
                    "Name": "Tanaya Luscombe",
                    "Date of Birth": "1995-01-24",
                    "Mobile": "",
                    "Email": "t.luscombe@hotmail.com",
                    "Landline": "",
                }
            )

        parsed = parse_csv_file(csv_path)

        assert len(parsed) == 2
        names = sorted(item["name"] for item in parsed)
        assert names == ["Aimee Newby", "Tanaya Luscombe"]
    finally:
        csv_path.unlink(missing_ok=True)


def test_build_manual_enrich_payload_uses_single_contact_name_only():
    payload = build_manual_enrich_payload(
        [
            {
                "name": "Tanaya Luscombe",
                "date_of_birth": "1995-01-24",
                "phones": [],
                "emails": ["t.luscombe@hotmail.com"],
            }
        ],
        last_seen="24-Jan-2025",
    )

    assert payload == {
        "owner_name": "Tanaya Luscombe",
        "phones": [],
        "emails": ["t.luscombe@hotmail.com"],
        "date_of_birth": "1995-01-24",
        "last_seen": "24-Jan-2025",
    }


def test_build_manual_enrich_payload_does_not_overwrite_owner_for_multiple_contacts():
    payload = build_manual_enrich_payload(
        [
            {
                "name": "Aimee Newby",
                "date_of_birth": None,
                "phones": ["0426206506"],
                "emails": ["spoiltrottendesigns@gmail.com"],
            },
            {
                "name": "Aaron MacKey",
                "date_of_birth": "1980-05-10",
                "phones": ["0438584629"],
                "emails": ["amack04@hotmail.com"],
            },
        ],
        last_seen=None,
    )

    assert payload == {
        "owner_name": None,
        "phones": ["0426206506", "0438584629"],
        "emails": ["amack04@hotmail.com", "spoiltrottendesigns@gmail.com"],
        "date_of_birth": None,
        "last_seen": None,
    }


def test_clean_text_collapses_nbsp_and_whitespace():
    assert clean_text(" Aaron\xa0MacKey \n  ") == "Aaron MacKey"
