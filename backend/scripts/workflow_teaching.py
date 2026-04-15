import asyncio
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from playwright.async_api import Page

from workflow_trace import (
    build_selector_candidates,
    capture_safe_screenshot,
    clear_trace,
    get_interactive_search_candidates,
    get_trace,
    install_trace,
    load_workflow,
    save_workflow,
    workflow_path,
)


ALLOWED_FIELDS = [
    "property_type",
    "bedrooms",
    "bathrooms",
    "car_spaces",
    "land_size_sqm",
    "building_size_sqm",
    "last_sale_price",
    "last_sale_date",
    "estimated_value_low",
    "estimated_value_high",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_url_pattern(url: str) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    parts = cleaned.split("?", 1)
    return parts[0]


def _replace_example_value(value: str, example_address: Optional[str]) -> str:
    text = str(value or "")
    if example_address and text.strip().lower() == example_address.strip().lower():
        return "{{full_address}}"
    if example_address and example_address.strip() and example_address.lower() in text.lower():
        return text.replace(example_address, "{{full_address}}")
    return text


def _pick_first(trace: list[dict[str, Any]], event_type: str, predicate=None) -> Optional[dict[str, Any]]:
    for event in trace:
        if event.get("type") != event_type:
            continue
        if predicate and not predicate(event):
            continue
        return event
    return None


def _pick_last(trace: list[dict[str, Any]], event_type: str, predicate=None) -> Optional[dict[str, Any]]:
    for event in reversed(trace):
        if event.get("type") != event_type:
            continue
        if predicate and not predicate(event):
            continue
        return event
    return None


def _event_target(event: dict[str, Any]) -> dict[str, Any]:
    snapshot = event.get("element") or {}
    return {
        "selectors": build_selector_candidates(snapshot),
        "snapshot": snapshot,
        "page_hints": {
            "url": event.get("url"),
            "title": event.get("title"),
        },
    }


def _safe_step_target(event: Optional[dict[str, Any]]) -> dict[str, Any]:
    return _event_target(event or {"element": {}, "url": "", "title": ""})


async def wait_for_operator(prompt: str) -> None:
    await asyncio.to_thread(input, prompt)


async def ask_yes_no(prompt: str, default_yes: bool = True) -> bool:
    answer = await asyncio.to_thread(input, prompt)
    normalized = str(answer or "").strip().lower()
    if not normalized:
        return default_yes
    return normalized in {"y", "yes"}


class WorkflowTeacher:
    def __init__(self, page: Page, artifact_root: Path, base_url: str):
        self.page = page
        self.artifact_root = artifact_root
        self.base_url = base_url

    async def teach(self, workflow_name: str, example_address: Optional[str] = None) -> Path:
        if workflow_name == "cotality_full_enrich":
            return self.compose_full_enrich()
        await install_trace(self.page)
        await clear_trace(self.page)
        if workflow_name == "cotality_search_property":
            if not example_address:
                raise RuntimeError("--example-address is required for cotality_search_property")
            return await self._teach_search(example_address)
        if workflow_name in {"cotality_extract_property_summary", "cotality_extract_property_intelligence"}:
            return await self._teach_extract()
        if workflow_name == "cotality_download_valuation":
            return await self._teach_download_valuation()
        raise RuntimeError(f"Unsupported workflow: {workflow_name}")

    async def _teach_search(self, example_address: str) -> Path:
        print("[teach] Login if needed in the visible browser.", flush=True)
        print(f"[teach] Perform the property search manually using: {example_address}", flush=True)
        print("[teach] Stop when the property details page is visible, then press Enter here.", flush=True)
        await wait_for_operator("[teach] Press Enter after the property details page is open...")
        trace = await get_trace(self.page)
        dom_candidates = await get_interactive_search_candidates(self.page)
        manifest = await compile_search_manifest(trace, example_address, self.page.url, dom_candidates)
        screenshot_path = self.artifact_root / f"teach_cotality_search_property_{int(datetime.now(timezone.utc).timestamp())}.png"
        await capture_safe_screenshot(self.page, screenshot_path)
        manifest["debug_screenshot_path"] = str(screenshot_path)
        path = save_workflow("cotality_search_property", manifest)
        print(f"[teach] saved workflow to {path}", flush=True)
        return path

    async def _teach_extract(self) -> Path:
        print("[teach] Open a property details page first.", flush=True)
        print("[teach] Click the main property summary/details region, then the valuation/estimate region.", flush=True)
        print("[teach] Press Enter here after both sections have been clicked.", flush=True)
        await wait_for_operator("[teach] Press Enter after the summary and valuation sections are visible...")
        trace = await get_trace(self.page)
        manifest = compile_extract_manifest(trace, self.page.url)
        screenshot_path = self.artifact_root / f"teach_cotality_extract_property_summary_{int(datetime.now(timezone.utc).timestamp())}.png"
        await capture_safe_screenshot(self.page, screenshot_path)
        manifest["debug_screenshot_path"] = str(screenshot_path)
        path = save_workflow("cotality_extract_property_summary", manifest)
        print(f"[teach] saved workflow to {path}", flush=True)
        return path

    async def _teach_download_valuation(self) -> Path:
        print("[teach] Navigate to a property details page in the visible browser.", flush=True)
        print("[teach] Click the 'Valuation Estimate' button/link to start the PDF download.", flush=True)
        print("[teach] Wait for the download to complete, then press Enter here.", flush=True)
        await wait_for_operator("[teach] Press Enter after the valuation PDF has downloaded...")
        trace = await get_trace(self.page)
        click_events = [e for e in trace if e.get("type") == "click"]
        download_click = None
        for event in reversed(click_events):
            snapshot = event.get("element") or {}
            haystack = " ".join([
                str(snapshot.get("text") or ""),
                str(snapshot.get("ariaLabel") or ""),
                str(snapshot.get("id") or ""),
                " ".join(snapshot.get("nearbyText") or []),
            ]).lower()
            if any(kw in haystack for kw in ["valuation", "estimate", "avm", "download", "pdf"]):
                download_click = event
                break
        if not download_click:
            download_click = click_events[-1] if click_events else None
        target = _safe_step_target(download_click)
        manifest = {
            "workflow_name": "cotality_download_valuation",
            "version": 1,
            "site": "cotality",
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "entry_url_patterns": [_normalize_url_pattern(self.page.url)],
            "success_url_patterns": [],
            "required_inputs": [],
            "steps": [
                {
                    "type": "click_download",
                    "description": "Click the Valuation Estimate download button",
                    "target": target,
                },
                {
                    "type": "wait_for_download",
                    "description": "Wait for PDF download to complete",
                    "timeout_ms": 30000,
                },
            ],
            "success_criteria": {
                "file_pattern": "Valuation_Estimate_AVM_*.pdf",
                "requires_download": True,
            },
            "page_signatures": {
                "title": self.page.url,
            },
            "last_validated_at": None,
        }
        screenshot_path = self.artifact_root / f"teach_cotality_download_valuation_{int(datetime.now(timezone.utc).timestamp())}.png"
        await capture_safe_screenshot(self.page, screenshot_path)
        manifest["debug_screenshot_path"] = str(screenshot_path)
        path = save_workflow("cotality_download_valuation", manifest)
        print(f"[teach] saved download valuation workflow to {path}", flush=True)
        return path

    def compose_full_enrich(self) -> Path:
        search = load_workflow("cotality_search_property")
        try:
            extract = load_workflow("cotality_extract_property_intelligence")
        except FileNotFoundError:
            try:
                extract = load_workflow("cotality_extract_property_summary")
            except FileNotFoundError:
                extract = {
                    "workflow_name": "cotality_extract_property_intelligence",
                    "version": 1,
                    "site": "cotality",
                    "created_at": utc_now_iso(),
                    "updated_at": utc_now_iso(),
                    "entry_url_patterns": search.get("success_url_patterns") or search.get("entry_url_patterns") or [self.base_url],
                    "success_url_patterns": search.get("success_url_patterns") or [],
                    "required_inputs": [],
                    "steps": [
                        {"type": "collect_first_level_tabs"},
                        {"type": "visit_tab"},
                        {"type": "expand_sections"},
                        {"type": "extract_sections"},
                    ],
                    "success_criteria": {
                        "requires_any_field": True,
                        "url_patterns": search.get("success_url_patterns") or [],
                    },
                    "page_signatures": {
                        "entry_title": search.get("page_signatures", {}).get("entry_title"),
                        "success_title": "",
                    },
                    "last_validated_at": None,
                }
        manifest = {
            "workflow_name": "cotality_full_enrich",
            "version": 1,
            "site": "cotality",
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
            "entry_url_patterns": search.get("entry_url_patterns") or [self.base_url],
            "success_url_patterns": extract.get("success_url_patterns") or search.get("success_url_patterns") or [],
            "required_inputs": ["full_address", "street", "suburb", "state", "postcode"],
            "steps": [*search.get("steps", []), *extract.get("steps", [])],
            "success_criteria": {
                "requires_fields": ALLOWED_FIELDS,
                "url_patterns": extract.get("success_url_patterns") or search.get("success_url_patterns") or [],
            },
            "page_signatures": {
                "search": search.get("page_signatures", {}),
                "extract": extract.get("page_signatures", {}),
            },
            "extract_targets": extract.get("extract_targets", {}),
            "workflow_refs": [
                workflow_path("cotality_search_property").name,
                workflow_path(extract.get("workflow_name") or "cotality_extract_property_intelligence").name,
            ],
            "last_validated_at": None,
        }
        path = save_workflow("cotality_full_enrich", manifest)
        print(f"[teach] composed workflow to {path}", flush=True)
        return path


def _snapshot_from_dom_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "tag": candidate.get("tag"),
        "id": candidate.get("id"),
        "name": candidate.get("name"),
        "ariaLabel": candidate.get("ariaLabel"),
        "placeholder": candidate.get("placeholder"),
        "role": candidate.get("role"),
        "text": candidate.get("text"),
        "value": candidate.get("value"),
        "cssPath": candidate.get("cssPath"),
        "xpath": candidate.get("xpath"),
        "nearbyText": candidate.get("nearbyText") or [],
    }


def _is_searchish_snapshot(snapshot: dict[str, Any], example_address: str = "") -> tuple[bool, str]:
    haystack = " ".join(
        [
            str(snapshot.get("tag") or ""),
            str(snapshot.get("id") or ""),
            str(snapshot.get("name") or ""),
            str(snapshot.get("ariaLabel") or ""),
            str(snapshot.get("placeholder") or ""),
            str(snapshot.get("role") or ""),
            str(snapshot.get("text") or ""),
            " ".join(snapshot.get("nearbyText") or []),
            str(snapshot.get("value") or ""),
        ]
    ).lower()
    keywords = ["search", "address", "property", "suburb", "location"]
    if any(keyword in haystack for keyword in keywords):
        return True, "matched search/address/property keyword"
    if example_address:
        tokens = [token for token in re.split(r"\W+", example_address.lower()) if token and len(token) > 2]
        if sum(1 for token in tokens if token in haystack) >= 2:
            return True, "matched example-address tokens"
    tag = str(snapshot.get("tag") or "").lower()
    role = str(snapshot.get("role") or "").lower()
    if tag in {"input", "textarea"} or role in {"combobox", "searchbox", "textbox"}:
        return True, "generic interactive search control"
    return False, "not search-like enough"


def _is_likely_details_url(url: str) -> bool:
    lowered = (url or "").lower()
    return any(token in lowered for token in ("/property", "/properties", "/address", "/details", "/valuation"))


def _candidate_event_to_target(event: dict[str, Any]) -> dict[str, Any]:
    snapshot = event.get("element") or {}
    return {
        "selectors": build_selector_candidates(snapshot),
        "snapshot": snapshot,
        "page_hints": {
            "url": event.get("url"),
            "title": event.get("title"),
        },
    }


async def compile_search_manifest(
    trace: list[dict[str, Any]],
    example_address: str,
    final_url: str,
    dom_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    input_event = _pick_last(trace, "input", lambda event: (event.get("extra") or {}).get("value"))
    print(f"[teach] typed search event found: {'yes' if input_event else 'no'}", flush=True)
    focus_event = _pick_first(
        trace,
        "focus",
        lambda event: (event.get("element") or {}).get("tag") in {"input", "textarea"} or (event.get("element") or {}).get("role") in {"textbox", "combobox"},
    ) or input_event
    key_event = _pick_first(trace, "keydown", lambda event: (event.get("extra") or {}).get("key") == "Enter")
    click_events = [event for event in trace if event.get("type") == "click"]

    accepted_reason = "captured input event"
    input_target = _safe_step_target(input_event) if input_event else None
    chosen_focus_event = focus_event
    if not input_event:
        infer_ok = await ask_yes_no("[teach] No typed search event detected. Infer workflow from focused search control and current page? [Y/n] ")
        if not infer_ok:
            raise RuntimeError("Search teaching cancelled because no typed input event was captured")
        focus_candidates = [
            event for event in trace
            if event.get("type") in {"focus", "click"} and (event.get("url") or "") != final_url
        ]
        chosen_event = None
        chosen_reason = ""
        for event in reversed(focus_candidates):
            snapshot = event.get("element") or {}
            ok, reason = _is_searchish_snapshot(snapshot, example_address)
            if ok:
                chosen_event = event
                chosen_reason = f"trace {event.get('type')} candidate: {reason}"
                break
        if not chosen_event:
            for candidate in dom_candidates:
                ok, reason = _is_searchish_snapshot(_snapshot_from_dom_candidate(candidate), example_address)
                if ok:
                    chosen_event = {
                        "type": "dom_candidate",
                        "url": candidate.get("url") or final_url,
                        "title": candidate.get("title") or "",
                        "element": _snapshot_from_dom_candidate(candidate),
                    }
                    chosen_reason = f"live DOM candidate: {reason}"
                    break
        if not chosen_event and _is_likely_details_url(final_url):
            all_candidates = [event for event in trace if event.get("type") in {"focus", "click"}]
            for event in reversed(all_candidates):
                snapshot = event.get("element") or {}
                ok, reason = _is_searchish_snapshot(snapshot, example_address)
                if ok:
                    chosen_event = event
                    chosen_reason = f"details-page fallback candidate: {reason}"
                    break
        if not chosen_event:
            raise RuntimeError("Could not infer a search control from trace or current DOM during teach mode")
        chosen_focus_event = chosen_event
        input_target = _candidate_event_to_target(chosen_event)
        accepted_reason = chosen_reason
        print(f"[teach] fallback search candidate chosen: {accepted_reason}", flush=True)
    else:
        print("[teach] fallback search candidate chosen: not needed", flush=True)

    typed_value = _replace_example_value((input_event.get("extra") or {}).get("value") or "", example_address) if input_event else "{{full_address}}"
    if not typed_value:
        typed_value = "{{full_address}}"
    print(f"[teach] search candidate accepted because: {accepted_reason}", flush=True)

    entry_url = (chosen_focus_event or _pick_first(trace, "focus") or {}).get("url") or ""
    success_url_patterns = [token for token in ["/property", "/address", "/details", "/valuation"] if token in (final_url or "").lower()]
    if not success_url_patterns:
        success_url_patterns = ["/property", "/address", "/details", "/valuation"]

    return {
        "workflow_name": "cotality_search_property",
        "version": 1,
        "site": "cotality",
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "entry_url_patterns": [_normalize_url_pattern(entry_url)],
        "success_url_patterns": success_url_patterns,
        "required_inputs": ["full_address", "street", "suburb", "state", "postcode"],
        "steps": [
            {"type": "focus", "target": _safe_step_target(chosen_focus_event)},
            {"type": "fill", "target": input_target or _safe_step_target(chosen_focus_event), "value_template": typed_value or "{{full_address}}"},
            {"type": "press", "key": "Enter"},
            {
                "type": "wait_for_navigation",
                "timeout": 10000,
                "success_url_patterns": success_url_patterns,
                "fallback_texts": ["property", "estimate", "value", "beds", "baths"],
            },
        ],
        "success_criteria": {
            "url_patterns": success_url_patterns,
        },
        "page_signatures": {
            "entry_title": (chosen_focus_event or _pick_first(trace, "focus") or {}).get("title"),
            "success_title": "",
        },
        "debug_inference": {
            "typed_event_found": bool(input_event),
            "accepted_reason": accepted_reason,
            "enter_detected_during_teach": bool(key_event),
            "click_events_seen": len(click_events),
        },
        "last_validated_at": None,
    }


def compile_extract_manifest(trace: list[dict[str, Any]], final_url: str) -> dict[str, Any]:
    clicks = [event for event in trace if event.get("type") == "click"]
    if len(clicks) < 2:
        raise RuntimeError("Teach mode needs two clicks for extract workflow: summary area then valuation area")
    property_click = clicks[-2]
    valuation_click = clicks[-1]
    return {
        "workflow_name": "cotality_extract_property_summary",
        "version": 1,
        "site": "cotality",
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "entry_url_patterns": [_normalize_url_pattern(property_click.get("url") or final_url)],
        "success_url_patterns": [_normalize_url_pattern(final_url)],
        "required_inputs": [],
        "steps": [
            {"type": "wait_for_selector", "target": _safe_step_target(property_click)},
            {"type": "wait_for_selector", "target": _safe_step_target(valuation_click)},
            {
                "type": "extract_group",
                "targets": {
                    "property_summary": _safe_step_target(property_click),
                    "valuation_section": _safe_step_target(valuation_click),
                },
                "fields": list(ALLOWED_FIELDS),
            },
        ],
        "extract_targets": {
            "property_summary": _safe_step_target(property_click),
            "valuation_section": _safe_step_target(valuation_click),
        },
        "success_criteria": {
            "requires_any_field": True,
            "url_patterns": [_normalize_url_pattern(final_url)],
        },
        "page_signatures": {
            "entry_title": property_click.get("title"),
            "success_title": valuation_click.get("title"),
        },
        "last_validated_at": None,
    }
