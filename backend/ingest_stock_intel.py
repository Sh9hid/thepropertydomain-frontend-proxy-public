import argparse
import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

import pandas as pd

try:
    from pypdf import PdfReader  # type: ignore
except Exception:
    PdfReader = None

DB_PATH = Path(__file__).parent.parent / "leads.db"
if not DB_PATH.exists():
    DB_PATH = Path(__file__).with_name("leads.db")
from core import config

STOCK_ROOT = Path(config.STOCK_ROOT)
SYDNEY_TZ = ZoneInfo("Australia/Sydney")
HIGH_SIGNAL = ("contract", "sales advice", "rental appraisal", "masterplan", "lot plan", "suburb report")
SENSITIVE = ("passport", "licence", "license", "trust receipt", "kyc", "identity", "bank", "2fa", "recovery")
ADDRESS_RE = re.compile(r"\b\d{1,4}[A-Za-z]?\s+[A-Za-z0-9 .'-]+\b(?:street|st|road|rd|avenue|ave|drive|dr|place|pl|close|cl|way|lane|ln|court|ct|crescent|cres|boulevard|blvd)\b", re.I)
LOT_RE = re.compile(r"\bLot\s+\d+[A-Za-z]?\b", re.I)
DATE_RE = re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b")

@dataclass
class ExtractedSignal:
    canonical_address: str
    owner_name: str
    trigger_type: str
    lifecycle_stage: str
    scenario: str
    source_path: str
    date_found: str
    evidence_score: int
    call_today_score: int
    suburb: str = "Box Hill"
    postcode: str = ""
    lat: float = 0
    lng: float = 0
    contact_emails: list[str] | None = None
    contact_phones: list[str] | None = None
    main_image: str = ""
    gender: str = "unknown"
    contacts: list[dict[str, Any]] | None = None

    def __post_init__(self):
        if self.contacts is None:
            contact = _build_contact(
                name=self.owner_name if self.owner_name != "Owner record pending" else "",
                phone=self.contact_phones[0] if self.contact_phones else "",
                email=self.contact_emails[0] if self.contact_emails else "",
                gender=self.gender
            )
            self.contacts = [contact] if contact else []


def now_iso() -> str:
    return datetime.now(SYDNEY_TZ).replace(microsecond=0).isoformat()


def file_hash(path: Path) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()


def canonicalize_address(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "").strip())
    cleaned = cleaned.replace(" Rd", " Road").replace(" St", " Street").replace(" Ave", " Avenue")
    
    # Handle suburb duplication at the end (e.g., "123 Main St, Bligh Park, Bligh Park")
    # or "123 Main St, Bligh Park NSW, Bligh Park"
    parts = [p.strip() for p in cleaned.split(",")]
    if len(parts) >= 2:
        last = parts[-1].lower()
        prev = parts[-2].lower()
        if last in prev or prev in last:
            cleaned = ", ".join(parts[:-1])
            
    return cleaned.title()


def _clean_cell(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text in {"-", "--", "N/A", "n/a", "nan"} else text


def _normalize_marketing_phone(value: Any) -> str:
    text = _clean_cell(value)
    if not text or text.lower() == "do not call":
        return ""
    digits = re.sub(r"\D+", "", text)
    if not digits:
        return ""
    if len(digits) == 9 and digits[0] in {"2", "3", "4", "7", "8"}:
        digits = f"0{digits}"
    elif len(digits) == 11 and digits.startswith("61"):
        digits = f"0{digits[2:]}"
    return digits


def _normalize_gender(value: Any) -> str:
    lowered = _clean_cell(value).lower()
    if lowered.startswith("m"):
        return "male"
    if lowered.startswith("f"):
        return "female"
    return "unknown"


def _build_contact(*, name: str = "", phone: str = "", email: str = "", gender: str = "unknown") -> dict[str, str] | None:
    cleaned_name = re.sub(r"\s+", " ", _clean_cell(name))
    cleaned_phone = _normalize_marketing_phone(phone)
    cleaned_email = _clean_cell(email)
    cleaned_gender = _normalize_gender(gender)
    if not any([cleaned_name, cleaned_phone, cleaned_email]):
        return None
    return {
        "name": cleaned_name,
        "phone": cleaned_phone,
        "email": cleaned_email,
        "gender": cleaned_gender,
    }


def _parse_contacts(value: Any) -> list[dict[str, str]]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    contacts: list[dict[str, str]] = []
    for raw in value:
        if not isinstance(raw, dict):
            continue
        contact = _build_contact(
            name=raw.get("name", ""),
            phone=raw.get("phone", ""),
            email=raw.get("email", ""),
            gender=raw.get("gender", "unknown"),
        )
        if contact:
            contacts.append(contact)
    return contacts


def _contact_key(contact: dict[str, str]) -> tuple[str, str]:
    return (
        re.sub(r"\s+", " ", contact.get("name", "")).strip().lower(),
        _normalize_marketing_phone(contact.get("phone", "")),
    )


def _dedupe_owner_names(values: list[str]) -> list[str]:
    seen: set[str] = set()
    names: list[str] = []
    for value in values:
        for part in str(value or "").split("&"):
            cleaned = re.sub(r"\s+", " ", _clean_cell(part)).strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            names.append(cleaned)
    return names


def _dedupe_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    cleaned_values: list[str] = []
    for value in values:
        cleaned = _clean_cell(value)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned_values.append(cleaned)
    return cleaned_values


def _dedupe_phones(values: list[str]) -> list[str]:
    seen: set[str] = set()
    cleaned_values: list[str] = []
    for value in values:
        cleaned = _normalize_marketing_phone(value)
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        cleaned_values.append(cleaned)
    return cleaned_values


def _merge_contacts(existing: list[dict[str, str]], incoming: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: list[dict[str, str]] = []
    index_by_key: dict[tuple[str, str], int] = {}
    for raw_contact in [*existing, *incoming]:
        contact = _build_contact(
            name=raw_contact.get("name", ""),
            phone=raw_contact.get("phone", ""),
            email=raw_contact.get("email", ""),
            gender=raw_contact.get("gender", "unknown"),
        )
        if not contact:
            continue
        key = _contact_key(contact)
        if key in index_by_key:
            current = merged[index_by_key[key]]
            if not current["email"] and contact["email"]:
                current["email"] = contact["email"]
            if current["gender"] == "unknown" and contact["gender"] != "unknown":
                current["gender"] = contact["gender"]
            continue
        index_by_key[key] = len(merged)
        merged.append(contact)
    return merged


def _find_tabular_header_row(path: Path) -> int | None:
    preview = pd.read_csv(path, header=None, dtype=str, keep_default_na=False)
    for idx, row in preview.iterrows():
        values = {str(value).strip().lower() for value in row.tolist() if str(value).strip()}
        if "street address" in values and "first name" in values:
            return int(idx)
        if "address" in values:
            return int(idx)
    return None


def _load_tabular_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        lowered = {str(col).strip().lower(): col for col in frame.columns}
        if not any("address" in key for key in lowered):
            header_row = _find_tabular_header_row(path)
            if header_row is not None:
                frame = pd.read_csv(path, skiprows=header_row, dtype=str, keep_default_na=False)
        return frame.fillna("")
    return pd.concat(pd.read_excel(path, sheet_name=None, dtype=str).values(), ignore_index=True).fillna("")


def classify(path: Path, text: str) -> Optional[str]:
    haystack = f"{path.name} {text[:600]}".lower()
    if any(flag in haystack for flag in SENSITIVE):
      return None
    for doc_type in HIGH_SIGNAL:
        if doc_type in haystack:
            return doc_type
    if "rescind" in haystack:
        return "contract"
    return None


def read_text(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md", ".csv"}:
        return path.read_text(encoding="utf-8", errors="ignore")
    if suffix in {".xlsx", ".xls"}:
        frames = pd.read_excel(path, sheet_name=None)
        return "\n".join(df.astype(str).to_csv(index=False) for df in frames.values())
    if suffix == ".pdf" and PdfReader:
        reader = PdfReader(str(path))
        text = "\n".join((page.extract_text() or "") for page in reader.pages)
        # Fall back to Tesseract OCR if the PDF layer is nearly empty (scanned document)
        if len(text.strip()) < 50:
            try:
                from services.ocr_service import ocr_service
                text = ocr_service.extract_text(path)
            except Exception:
                pass
        return text
    return ""


def extract_tabular_signals(path: Path) -> list[ExtractedSignal]:
    suffix = path.suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        return []
    frame = _load_tabular_frame(path)
    lowered = {str(col).strip().lower(): col for col in frame.columns}
    address_col = next((lowered[key] for key in lowered if "address" in key), None)
    if address_col is None:
        return []
    suburb_col = next((lowered[key] for key in lowered if "suburb" in key or "neighbourhood" in key), None)
    postcode_col = next((lowered[key] for key in lowered if "postcode" in key), None)
    owner_col = next(
        (
            lowered[key]
            for key in lowered
            if key in {"full name", "name"}
            or (("owner" in key or "vendor" in key) and "name" in key)
        ),
        None,
    )
    first_name_col = lowered.get("first name")
    surname_col = next((lowered[key] for key in lowered if key in {"surname", "last name"}), None)
    email_col = next((lowered[key] for key in lowered if "email" in key), None)
    phone_col = next((lowered[key] for key in lowered if "phone" in key or "mobile" in key), None)
    gender_col = next((lowered[key] for key in lowered if "gender" in key), None)
    lat_col = next((lowered[key] for key in lowered if key in {"lat", "latitude"}), None)
    lng_col = next((lowered[key] for key in lowered if key in {"lng", "lon", "longitude"}), None)
    image_col = next((lowered[key] for key in lowered if "image" in key or "photo" in key), None)
    is_marketing_report = bool(first_name_col or surname_col or gender_col)

    signals: list[ExtractedSignal] = []
    for _, row in frame.fillna("").iterrows():
        address = _clean_cell(row[address_col])
        if not address:
            continue
        evidence_score, call_today_score, lifecycle_stage = score("suburb report", "spreadsheet neighbourhood row", 1)
        first_name = _clean_cell(row[first_name_col]) if first_name_col else ""
        surname = _clean_cell(row[surname_col]) if surname_col else ""
        full_name = " ".join(part for part in [first_name, surname] if part).strip()
        owner_name = full_name or (_clean_cell(row[owner_col]) if owner_col else "") or "Owner record pending"
        email = _clean_cell(row[email_col]) if email_col else ""
        phone = _normalize_marketing_phone(row[phone_col]) if phone_col else ""
        contact = _build_contact(
            name=owner_name if owner_name != "Owner record pending" else "",
            phone=phone,
            email=email,
            gender=row[gender_col] if gender_col else "unknown",
        )
        signals.append(
            ExtractedSignal(
                canonical_address=canonicalize_address(address),
                owner_name=owner_name,
                trigger_type="Marketing Report Import" if is_marketing_report else "Spreadsheet Import",
                lifecycle_stage="marketing_import" if is_marketing_report else "neighbourhood_import",
                scenario=f"Imported {'marketing report' if is_marketing_report else 'neighbourhood'} row from {path.name}.",
                source_path=str(path),
                date_found=datetime.fromtimestamp(path.stat().st_mtime, SYDNEY_TZ).replace(microsecond=0).isoformat(),
                evidence_score=evidence_score,
                call_today_score=call_today_score,
                suburb=_clean_cell(row[suburb_col]) if suburb_col else "Box Hill",
                postcode=_clean_cell(row[postcode_col]) if postcode_col else "",
                lat=float(row[lat_col]) if lat_col and str(row[lat_col]).strip() else 0,
                lng=float(row[lng_col]) if lng_col and str(row[lng_col]).strip() else 0,
                contact_emails=[email] if email else [],
                contact_phones=[phone] if phone else [],
                contacts=[contact] if contact else [],
                main_image=_clean_cell(row[image_col]) if image_col else "",
            )
        )
    return signals


def infer_date(text: str, fallback: Path) -> str:
    match = DATE_RE.search(text)
    if match:
        raw = match.group(1)
        for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=SYDNEY_TZ).isoformat()
            except ValueError:
                continue
    return datetime.fromtimestamp(fallback.stat().st_mtime, SYDNEY_TZ).replace(microsecond=0).isoformat()


def extract_owner(text: str) -> str:
    match = re.search(r"(vendor|owner|registered proprietor|purchaser)[:\s]+([A-Z][A-Za-z .'-]{3,80})", text, re.I)
    return re.sub(r"\s+", " ", match.group(2)).strip() if match else "Owner record pending"


def score(doc_type: str, text: str, file_count: int) -> tuple[int, int, str]:
    lowered = text.lower()
    stage = "documented_signal"
    bonus = 10
    if "rescind" in lowered:
        stage, bonus = "rescinded", 30
    elif "vacant land" in lowered or "construction" in lowered:
        stage, bonus = "construction_likely", 24
    elif doc_type == "rental appraisal":
        stage, bonus = "investor_review", 18
    elif doc_type == "masterplan":
        stage, bonus = "subdivision_mapping", 20
    evidence = min(100, 30 + file_count * 18 + (15 if "signed" in lowered else 0) + (10 if "box hill" in lowered else 0))
    return evidence, min(100, evidence // 2 + bonus), stage


def _resolve_lot_address(lot_str: str) -> tuple[str, float, float]:
    """Try NSW Spatial Services cadastral resolver; return (street_address, lat, lng) or (lot_str, 0, 0)."""
    try:
        import asyncio
        from services.cadastral_resolver import cadastral_resolver
        result = asyncio.run(cadastral_resolver.resolve(lot_str))
        if result and result.get("address"):
            return result["address"], result.get("lat", 0), result.get("lng", 0)
    except Exception:
        pass
    return lot_str, 0, 0


def extract_signal(path: Path) -> Optional[ExtractedSignal]:
    text = read_text(path)
    if not text.strip():
        return None
    doc_type = classify(path, text)
    if not doc_type:
        return None
    address_match = ADDRESS_RE.search(text) or ADDRESS_RE.search(path.stem)
    lot_match = LOT_RE.search(text) or LOT_RE.search(path.stem)
    address = address_match.group(0) if address_match else (lot_match.group(0) if lot_match else "")
    if not address:
        return None

    lat, lng = 0.0, 0.0
    if not address_match and lot_match:
        # Lot number only — resolve via NSW Spatial Services
        address, lat, lng = _resolve_lot_address(lot_match.group(0))

    evidence_score, call_today_score, lifecycle_stage = score(doc_type, text, 1)
    trigger_type = "Contract of Sale" if doc_type == "contract" else doc_type.title()
    summary = f"{trigger_type} extracted from {path.name} with lifecycle stage {lifecycle_stage.replace('_', ' ')}."
    return ExtractedSignal(
        canonical_address=canonicalize_address(address),
        owner_name=extract_owner(text),
        trigger_type=trigger_type,
        lifecycle_stage=lifecycle_stage,
        scenario=summary,
        source_path=str(path),
        date_found=infer_date(text, path),
        evidence_score=evidence_score,
        call_today_score=call_today_score,
        lat=lat,
        lng=lng,
    )


def iter_candidate_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        lowered = str(path).lower()
        if any(flag in lowered for flag in SENSITIVE):
            continue
        if path.suffix.lower() not in {".pdf", ".xlsx", ".xls", ".csv", ".txt", ".md"}:
            continue
        yield path


from sqlmodel import Session, select
from models.sql_models import Lead

def upsert_signal(conn: sqlite3.Connection, signal: ExtractedSignal) -> None:
    # We'll use a SQLModel session for more reliable JSON handling
    from sqlmodel import create_engine
    engine = create_engine(f"sqlite:///{DB_PATH}")
    
    with Session(engine) as session:
        # Try to find by canonical address first to avoid duplicates with different IDs
        from sqlmodel import select
        stmt = select(Lead).where(Lead.address == signal.canonical_address)
        lead = session.exec(stmt).first()
        
        now = now_iso()
        if not lead:
            # Generate a new UUID if not found
            import uuid
            lead_id = str(uuid.uuid4())
            lead = Lead(id=lead_id, address=signal.canonical_address, created_at=now)
            session.add(lead)
        
        existing_contacts = lead.contacts if lead and lead.contacts else []
        if isinstance(existing_contacts, str):
            try:
                existing_contacts = json.loads(existing_contacts)
            except:
                existing_contacts = []
                
        merged_contacts = _merge_contacts(existing_contacts, signal.contacts or [])
        
        # Dedupe high-level fields using all contacts
        merged_owner_names = _dedupe_owner_names(
            [contact["name"] for contact in merged_contacts] + ([lead.owner_name] if lead and lead.owner_name else []) + [signal.owner_name]
        )
        merged_emails = _dedupe_texts(
            [contact["email"] for contact in merged_contacts] + (lead.contact_emails if lead and lead.contact_emails else []) + (signal.contact_emails or [])
        )
        merged_phones = _dedupe_phones(
            [contact["phone"] for contact in merged_contacts] + (lead.contact_phones if lead and lead.contact_phones else []) + (signal.contact_phones or [])
        )
        
        lead.suburb = signal.suburb
        lead.postcode = signal.postcode
        # Keep a single primary display name to avoid repeated concatenation artifacts
        # like "John Smith & John Smith & J. Smith" after repeated merges.
        lead.owner_name = (merged_owner_names[0] if merged_owner_names else "Owner record pending")
        lead.trigger_type = signal.trigger_type
        lead.heat_score = signal.call_today_score
        lead.scenario = signal.scenario
        lead.strategic_value = "Evidence-backed property opportunity"
        lead.contact_status = "unreviewed"
        lead.confidence_score = signal.evidence_score
        lead.contact_emails = merged_emails
        lead.contact_phones = merged_phones
        lead.contacts = merged_contacts
        lead.lat = signal.lat
        lead.lng = signal.lng
        lead.date_found = signal.date_found
        lead.main_image = signal.main_image
        lead.description_deep = signal.scenario
        lead.likely_scenario = signal.lifecycle_stage.replace("_", " ")
        lead.call_today_score = signal.call_today_score
        lead.evidence_score = signal.evidence_score
        lead.lifecycle_stage = signal.lifecycle_stage
        lead.updated_at = now
        
        # Ensure source evidence is merged
        existing_evidence = lead.source_evidence if lead.source_evidence else []
        if signal.source_path not in existing_evidence:
            lead.source_evidence = (existing_evidence + [signal.source_path])[:10] # limit history
            lead.linked_files = ((lead.linked_files if lead.linked_files else []) + [signal.source_path])[:10]

        session.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest high-signal property stock files into leads.db")
    parser.add_argument("--root", default=str(STOCK_ROOT))
    args = parser.parse_args()
    root = Path(args.root)
    conn = sqlite3.connect(DB_PATH)

    scanned = 0
    inserted = 0
    for path in iter_candidate_files(root):
        scanned += 1
        tabular_signals = extract_tabular_signals(path)
        if tabular_signals:
            for signal in tabular_signals:
                upsert_signal(conn, signal)
                inserted += 1
            continue
        signal = extract_signal(path)
        if signal:
            upsert_signal(conn, signal)
            inserted += 1

    conn.commit()
    conn.close()
    pdf_note = "with PDF parsing" if PdfReader else "without PDF parsing (install pypdf to enable PDF extraction)"
    print(f"Scanned {scanned} files, upserted {inserted} signals {pdf_note}.")


if __name__ == "__main__":
    main()
