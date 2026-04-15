from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


_WS_RE = re.compile(r"\s+")
_NON_WORD_RE = re.compile(r"[^a-z0-9 ]+")
_POSTCODE_RE = re.compile(r"\b(\d{4})\b")
_UNIT_PREFIX_RE = re.compile(r"^(unit|u|apt|apartment|suite|level)\s+", re.IGNORECASE)
_ORDINAL_SUFFIX_RE = re.compile(r"(\d+)(st|nd|rd|th)$")
_STREET_TYPE_MAP = {
    "st": "street",
    "street": "street",
    "rd": "road",
    "road": "road",
    "ave": "avenue",
    "av": "avenue",
    "avenue": "avenue",
    "dr": "drive",
    "drive": "drive",
    "ct": "court",
    "court": "court",
    "pl": "place",
    "place": "place",
    "cres": "crescent",
    "crescent": "crescent",
    "cct": "circuit",
    "circuit": "circuit",
    "pde": "parade",
    "parade": "parade",
    "ln": "lane",
    "lane": "lane",
    "way": "way",
    "blvd": "boulevard",
    "boulevard": "boulevard",
    "hwy": "highway",
    "highway": "highway",
    "terrace": "terrace",
    "tce": "terrace",
    "close": "close",
    "cl": "close",
}


@dataclass
class AddressIdentity:
    raw: str
    normalized: str
    compact: str
    house_number: str
    unit: str
    street_name: str
    street_type: str
    suburb: str
    postcode: str

    @property
    def strict_key(self) -> str:
        return "|".join(
            [
                self.house_number,
                self.street_name,
                self.street_type,
                self.suburb,
                self.postcode,
            ]
        )

    @property
    def loose_key(self) -> str:
        return "|".join([self.house_number, self.street_name, self.street_type, self.suburb])


def _normalize_text(value: Any) -> str:
    lowered = str(value or "").strip().lower()
    lowered = _UNIT_PREFIX_RE.sub("", lowered)
    lowered = _NON_WORD_RE.sub(" ", lowered)
    lowered = _WS_RE.sub(" ", lowered).strip()
    return lowered


def _normalize_postcode(value: Any) -> str:
    match = _POSTCODE_RE.search(str(value or ""))
    return match.group(1) if match else ""


def _split_house_and_street(address: str) -> tuple[str, str, str, str]:
    parts = address.split(" ") if address else []
    if not parts:
        return "", "", "", ""

    house = ""
    unit = ""
    street_tokens: List[str] = parts[:]
    first = parts[0]
    if re.match(r"^[0-9]+[a-z]?(?:/[0-9]+[a-z]?)?$", first):
        house = first.split("/")[-1]
        unit = first.split("/")[0] if "/" in first else ""
        street_tokens = parts[1:]

    if not street_tokens:
        return house, unit, "", ""

    street_type_raw = street_tokens[-1]
    street_type = _STREET_TYPE_MAP.get(street_type_raw, street_type_raw)
    street_name = " ".join(street_tokens[:-1]).strip() or street_tokens[0]

    ord_match = _ORDINAL_SUFFIX_RE.match(house)
    if ord_match:
        house = ord_match.group(1)

    return house, unit, street_name, street_type


def build_address_identity(
    *,
    address: Any,
    suburb: Any = "",
    postcode: Any = "",
) -> AddressIdentity:
    normalized_address = _normalize_text(address)
    suburb_norm = _normalize_text(suburb)
    postcode_norm = _normalize_postcode(postcode)
    house, unit, street_name, street_type = _split_house_and_street(normalized_address)

    return AddressIdentity(
        raw=str(address or "").strip(),
        normalized=normalized_address,
        compact=normalized_address.replace(" ", ""),
        house_number=house,
        unit=unit,
        street_name=street_name,
        street_type=street_type,
        suburb=suburb_norm,
        postcode=postcode_norm,
    )


def classify_match(
    incoming: AddressIdentity,
    candidate: AddressIdentity,
    *,
    candidate_lat: float = 0.0,
    candidate_lng: float = 0.0,
) -> Dict[str, Any]:
    reasons: List[str] = []
    confidence = "ambiguous_review"

    if incoming.strict_key and incoming.strict_key == candidate.strict_key:
        reasons.append("strict_address_key_match")
        confidence = "safe_exact"
    elif incoming.loose_key and incoming.loose_key == candidate.loose_key:
        reasons.append("address_key_match_without_postcode")
    elif incoming.compact and incoming.compact == candidate.compact:
        reasons.append("normalized_address_compact_match")
    else:
        if incoming.house_number and candidate.house_number and incoming.house_number != candidate.house_number:
            reasons.append("house_number_differs")
        if incoming.street_name and candidate.street_name and incoming.street_name != candidate.street_name:
            reasons.append("street_name_differs")
        if incoming.suburb and candidate.suburb and incoming.suburb != candidate.suburb:
            reasons.append("suburb_differs")

    if confidence != "safe_exact" and candidate_lat and candidate_lng:
        reasons.append("geo_cache_available_for_tiebreak")

    return {
        "match_confidence": confidence,
        "requires_confirmation": confidence != "safe_exact",
        "safe_for_merge_all": confidence == "safe_exact",
        "match_reasons": reasons or ["no_strict_match"],
    }

