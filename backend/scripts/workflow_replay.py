import asyncio
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from playwright.async_api import Error as PlaywrightError, Locator, Page, TimeoutError as PlaywrightTimeoutError

try:
    from workflow_trace import capture_safe_screenshot
except ImportError:
    try:
        from scripts.workflow_trace import capture_safe_screenshot
    except ImportError:
        async def capture_safe_screenshot(*args, **kwargs): pass


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


class WorkflowNotTaughtError(RuntimeError):
    pass


class ReplayFailure(RuntimeError):
    pass


class ReviewRequired(RuntimeError):
    pass


def normalize_address(value: str) -> str:
    compact = re.sub(r"[^a-z0-9 ]", " ", (value or "").lower())
    compact = compact.replace("street", "st").replace("road", "rd").replace("avenue", "ave")
    compact = compact.replace("drive", "dr").replace("place", "pl").replace("close", "cl")
    return re.sub(r"\s+", " ", compact).strip()


def address_score(candidate_text: str, variables: dict[str, Any]) -> tuple[float, str]:
    raw = str(candidate_text or "")
    candidate = normalize_address(raw)
    street = normalize_address(str(variables.get("street") or ""))
    suburb = normalize_address(str(variables.get("suburb") or ""))
    state = normalize_address(str(variables.get("state") or ""))
    postcode = normalize_address(str(variables.get("postcode") or ""))
    full_address = normalize_address(str(variables.get("full_address") or ""))
    if not candidate:
        return 0.0, raw
    score = 0.0
    if street and street in candidate:
        score += 5.0
    if suburb and suburb in candidate:
        score += 2.5
    if state and state in candidate:
        score += 1.5
    if postcode and postcode in candidate:
        score += 2.0
    if full_address and full_address in candidate:
        score += 4.0
    if postcode and re.search(r"\b\d{4}\b", candidate) and postcode not in candidate:
        score -= 4.0
    if state and re.search(r"\b[a-z]{2,3}\b", candidate) and state not in candidate:
        score -= 2.0
    overlap = set(candidate.split()) & set(full_address.split())
    score += len(overlap) * 0.3
    return score, raw


def substitute_template(template: str, variables: dict[str, Any]) -> str:
    value = str(template or "")
    for key, item in variables.items():
        value = value.replace(f"{{{{{key}}}}}", str(item or ""))
    return value


def normalize_traversal_key(value: str) -> str:
    compact = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return compact


def _parse_number(value: Any):
    if value in (None, "", []):
        return None
    if isinstance(value, (int, float)):
        return value
    cleaned = re.sub(r"[^\d.\-]", "", str(value))
    if not cleaned:
        return None
    if "." in cleaned:
        return float(cleaned)
    return int(cleaned)


def _extract_text_value(text_blob: str, patterns: list[str], numeric: bool = False):
    for pattern in patterns:
        match = re.search(pattern, text_blob, flags=re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if numeric:
                return _parse_number(value)
            return value
    return None


def parse_property_payload(details_text: str, valuation_text: str) -> dict[str, Any]:
    combined = f"{details_text}\n{valuation_text}"
    estimated_low = estimated_high = None
    range_match = re.search(r"\$?\s*([\d,]+)\s*[-–]\s*\$?\s*([\d,]+)", valuation_text)
    if range_match:
        estimated_low = int(range_match.group(1).replace(",", ""))
        estimated_high = int(range_match.group(2).replace(",", ""))
    payload = {
        "property_type": _extract_text_value(combined, [r"property type[:\s]+([A-Za-z ]+)", r"type[:\s]+([A-Za-z ]+)"]),
        "bedrooms": _extract_text_value(combined, [r"bed(?:rooms?)?[:\s]+([\d.]+)", r"bedrooms?[:\s]+([\d.]+)"], numeric=True),
        "bathrooms": _extract_text_value(combined, [r"bath(?:rooms?)?[:\s]+([\d.]+)", r"bathrooms?[:\s]+([\d.]+)"], numeric=True),
        "car_spaces": _extract_text_value(combined, [r"car(?: spaces?)?[:\s]+([\d.]+)", r"parking[:\s]+([\d.]+)"], numeric=True),
        "land_size_sqm": _extract_text_value(combined, [r"land size[:\s]+([\d,.]+)", r"land area[:\s]+([\d,.]+)"], numeric=True),
        "building_size_sqm": _extract_text_value(combined, [r"(?:building|floor|internal) (?:size|area)[:\s]+([\d,.]+)"], numeric=True),
        "last_sale_price": _extract_text_value(combined, [r"last sale price[:\s]+\$?([\d,]+)", r"sold for[:\s]+\$?([\d,]+)"]),
        "last_sale_date": _extract_text_value(combined, [r"last sale date[:\s]+([0-9A-Za-z/\- ]+)", r"sold on[:\s]+([0-9A-Za-z/\- ]+)"]),
        "estimated_value_low": estimated_low,
        "estimated_value_high": estimated_high,
    }
    return {key: value for key, value in payload.items() if value not in (None, "")}


@dataclass
class ReplayResult:
    matched_address: Optional[str]
    match_confidence: float
    proposed_updates: dict[str, Any]
    raw_payload: dict[str, Any]
    evidence: dict[str, Any]


class WorkflowReplayEngine:
    def __init__(self, page: Page, pace, detect_unusual_state, artifact_root: Path, ensure_session=None):
        self.page = page
        self.pace = pace
        self.detect_unusual_state = detect_unusual_state
        self.artifact_root = artifact_root
        self.ensure_session = ensure_session

    async def safe_body_text(self) -> str:
        try:
            return await self.page.locator("body").inner_text(timeout=3000)
        except Exception:
            return ""

    async def capture_debug_screenshot(self, prefix: str) -> str:
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        path = self.artifact_root / f"{prefix}.png"
        await capture_safe_screenshot(self.page, path)
        return str(path)

    async def ensure_normal_state(self) -> None:
        if self.ensure_session:
            await self.ensure_session()
        unusual = await self.detect_unusual_state(self.page)
        if unusual:
            raise ReplayFailure(f"blocked_or_unusual:{unusual}")

    async def wait_for_settle(self) -> None:
        try:
            await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
            await self.page.wait_for_load_state("networkidle", timeout=8000)
        except (PlaywrightTimeoutError, AttributeError):
            pass
        await self.pace.pause("workflow settle")

    async def maybe_scroll(self) -> None:
        if random.random() < 0.35:
            offset = random.randint(120, 460)
            print(f"[replay] scrolling {offset}px", flush=True)
            await self.page.mouse.wheel(0, offset)
            await self.pace.pause("post-scroll", 900, 2200)

    async def _resolve_candidate(self, candidate: dict[str, Any]) -> tuple[Optional[Locator], str]:
        locator: Optional[Locator] = None
        kind = candidate.get("type")
        value = str(candidate.get("value") or "")
        if not value:
            return None, "empty selector"
        try:
            if kind == "css":
                locator = self.page.locator(value)
            elif kind == "text":
                locator = self.page.get_by_text(value, exact=False)
            elif kind == "xpath":
                locator = self.page.locator(f"xpath={value}")
            elif kind == "label":
                locator = self.page.get_by_label(value, exact=False)
            elif kind == "placeholder":
                locator = self.page.get_by_placeholder(value, exact=False)
            elif kind == "role":
                locator = self.page.get_by_role(value, name=str(candidate.get("name") or "") or None)
            elif kind == "tag":
                locator = self.page.locator(value)
            else:
                return None, f"unsupported candidate type={kind}"
            if locator is None:
                return None, "candidate produced no locator"
            count = await locator.count()
            if count > 0:
                try:
                    visible = await locator.first.is_visible(timeout=1200)
                except Exception:
                    visible = False
                return locator.first, f"matched count={count} visible={visible}"
            return None, "count=0"
        except PlaywrightError as error:
            return None, f"playwright_error={error}"
        except Exception as error:
            return None, f"error={error}"

    async def _search_like_fallback_locator(self, target: dict[str, Any], step_type: str) -> tuple[Optional[Locator], str]:
        if step_type not in {"focus", "fill"}:
            return None, "step is not focus/fill"
        snapshot = (target or {}).get("snapshot") or {}
        hints = [
            str(snapshot.get("placeholder") or ""),
            str(snapshot.get("ariaLabel") or ""),
            str(snapshot.get("name") or ""),
            str(snapshot.get("id") or ""),
            str(snapshot.get("role") or ""),
            str(snapshot.get("text") or ""),
            " ".join(snapshot.get("nearbyText") or []),
        ]
        hint_blob = " ".join(hints).lower()
        search_terms = ["search", "address", "property", "suburb", "location"]
        selectors: list[tuple[str, str]] = []
        role_hint = str(snapshot.get("role") or "").lower()
        if role_hint == "combobox":
            selectors.append(('role:combobox', '[role="combobox"]'))
        selectors.extend(
            [
                ('role:combobox', '[role="combobox"]'),
                ('input:text', 'input[type="text"]'),
                ('input:search', 'input[type="search"]'),
                ('searchbox', '[role="searchbox"]'),
                ('input:placeholder-search', 'input[placeholder*="search" i]'),
                ('input:placeholder-address', 'input[placeholder*="address" i]'),
                ('input:placeholder-property', 'input[placeholder*="property" i]'),
                ('input:placeholder-suburb', 'input[placeholder*="suburb" i]'),
                ('input:placeholder-location', 'input[placeholder*="location" i]'),
                ('input:aria-search', 'input[aria-label*="search" i]'),
                ('input:aria-address', 'input[aria-label*="address" i]'),
                ('input:name-search', 'input[name*="search" i]'),
                ('input:name-address', 'input[name*="address" i]'),
            ]
        )
        attempted: list[str] = []
        for reason, selector in selectors:
            try:
                locator = self.page.locator(selector)
                count = min(await locator.count(), 8)
                for index in range(count):
                    item = locator.nth(index)
                    try:
                        if not await item.is_visible(timeout=800):
                            continue
                    except Exception:
                        continue
                    attrs = await item.evaluate(
                        """
                        (element) => ({
                          role: element.getAttribute('role') || '',
                          placeholder: element.getAttribute('placeholder') || '',
                          ariaLabel: element.getAttribute('aria-label') || '',
                          name: element.getAttribute('name') || '',
                          id: element.id || '',
                          text: (element.innerText || element.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 160)
                        })
                        """
                    )
                    attrs_blob = " ".join(str(attrs.get(key) or "") for key in ("role", "placeholder", "ariaLabel", "name", "id", "text")).lower()
                    if any(term in attrs_blob for term in search_terms) or any(term in hint_blob for term in search_terms):
                        return item, f"{reason} matched attrs={attrs_blob[:120]}"
                    if role_hint == "combobox" and selector == '[role="combobox"]':
                        return item, f"{reason} matched saved role hint"
                attempted.append(f"{reason}:none-visible-match")
            except Exception as error:
                attempted.append(f"{reason}:{error}")
        return None, "; ".join(attempted) if attempted else "no fallback selectors attempted"

    async def resolve_locator(self, target: dict[str, Any], step_type: str = "unknown") -> Locator:
        candidates = (target or {}).get("selectors") or []
        page_title = ""
        try:
            page_title = await self.page.title()
        except Exception:
            pass
        print(f"[replay] resolve target step={step_type} url={self.page.url} title={page_title}", flush=True)
        for candidate in candidates:
            locator, reason = await self._resolve_candidate(candidate)
            print(f"[replay] candidate step={step_type} type={candidate.get('type')} value={candidate.get('value')} -> {reason}", flush=True)
            if locator is not None:
                return locator
        fallback_locator, fallback_reason = await self._search_like_fallback_locator(target, step_type)
        if fallback_locator is not None:
            print(f"[replay] fallback target chosen: {fallback_reason}", flush=True)
            return fallback_locator
        print(f"[replay] target resolution failed step={step_type} fallback_reason={fallback_reason}", flush=True)
        raise ReplayFailure("target_resolution_failed")

    async def resolve_group_items(self, target_group: dict[str, Any]) -> list[Locator]:
        candidates = (target_group or {}).get("selectors") or []
        locators: list[Locator] = []
        for candidate in candidates:
            kind = candidate.get("type")
            value = str(candidate.get("value") or "")
            if not value:
                continue
            try:
                if kind == "css":
                    locator = self.page.locator(value)
                elif kind == "text":
                    locator = self.page.get_by_text(value, exact=False)
                elif kind == "xpath":
                    locator = self.page.locator(f"xpath={value}")
                else:
                    continue
                count = min(await locator.count(), 10)
                for index in range(count):
                    locators.append(locator.nth(index))
                if locators:
                    return locators
            except PlaywrightError:
                continue
        raise ReplayFailure("result_group_resolution_failed")

    async def click_locator(self, locator: Locator, reason: str) -> None:
        await self.wait_for_settle()
        await self.maybe_scroll()
        await locator.scroll_into_view_if_needed()
        await self.pace.pause(f"before {reason}")
        await locator.click(timeout=12000)
        await self.pace.pause(f"after {reason}", 1200, 3200)

    async def focus_locator(self, locator: Locator, reason: str) -> None:
        await self.wait_for_settle()
        await self.pace.pause(f"before {reason}", 1400, 2600)
        await locator.focus()
        await self.pace.pause(f"after {reason}", 900, 2200)

    async def _locator_traits(self, locator: Locator) -> dict[str, str]:
        def _read_js():
            return """
            (element) => ({
              tag: (element.tagName || '').toLowerCase(),
              role: element.getAttribute('role') || '',
              ariaExpanded: element.getAttribute('aria-expanded') || '',
              inputType: element.getAttribute('type') || '',
              isPlainInput: ['input', 'textarea'].includes((element.tagName || '').toLowerCase())
            })
            """
        try:
            traits = await locator.evaluate(_read_js())
            return traits if isinstance(traits, dict) else {}
        except Exception:
            return {}

    async def _set_plain_input_value(self, locator: Locator, value: str) -> bool:
        try:
            await locator.evaluate(
                """
                (element, nextValue) => {
                  const tag = (element.tagName || '').toLowerCase();
                  if (!['input', 'textarea'].includes(tag)) return false;
                  element.focus();
                  element.value = nextValue;
                  element.dispatchEvent(new Event('input', { bubbles: true }));
                  element.dispatchEvent(new Event('change', { bubbles: true }));
                  return true;
                }
                """,
                value,
            )
            return True
        except Exception:
            return False

    async def fill_locator(self, locator: Locator, value: str) -> None:
        await self.wait_for_settle()
        await self.maybe_scroll()
        await locator.scroll_into_view_if_needed()
        traits = await self._locator_traits(locator)
        role = str(traits.get("role") or "").lower()
        tag = str(traits.get("tag") or "").lower()
        aria_expanded = str(traits.get("ariaExpanded") or "")
        keyboard_first = role in {"combobox", "searchbox"} or bool(aria_expanded) or tag in {"input", "textarea"}

        if not keyboard_first:
            try:
                await self.click_locator(locator, "fill target")
            except PlaywrightError as error:
                print(f"[replay] click intercepted on fill target; falling back to keyboard-first entry: {error}", flush=True)

        await self.pace.pause("before focus fill target", 900, 2200)
        await locator.focus()
        if role == "combobox" or aria_expanded:
            try:
                await locator.press("Escape")
                await self.pace.pause("after escape", 400, 900)
            except Exception:
                pass
            await locator.focus()

        try:
            await locator.press("Control+A")
        except Exception:
            pass
        try:
            await locator.press("Backspace")
        except Exception:
            pass
        try:
            await locator.press("Delete")
        except Exception:
            pass

        await self.pace.pause("before typing", 900, 2200)
        try:
            await locator.press_sequentially(value, delay=random.randint(70, 160))
        except Exception:
            try:
                await locator.fill(value)
            except Exception:
                if not await self._set_plain_input_value(locator, value):
                    raise
        await self.pace.pause("after typing", 1300, 3000)

    async def click_best_match(self, target_group: dict[str, Any], variables: dict[str, Any]) -> tuple[str, float]:
        candidates = await self.resolve_group_items(target_group)
        if not candidates:
            raise ReplayFailure("no_search_results_found")
        best_locator: Optional[Locator] = None
        best_text = ""
        best_score = -1.0
        second_score = -1.0
        for locator in candidates:
            try:
                text_value = (await locator.inner_text(timeout=2500)).strip()
            except Exception:
                continue
            score, raw = address_score(text_value, variables)
            if score > best_score:
                second_score = best_score
                best_score = score
                best_locator = locator
                best_text = raw
            elif score > second_score:
                second_score = score
        if not best_locator or best_score <= 0:
            raise ReplayFailure("no_good_property_match")
        if second_score > 0 and (best_score - second_score) < 1.2:
            raise ReviewRequired(f"ambiguous_match:{best_text}")
        await self.click_locator(best_locator, "best property match")
        return best_text, min(0.99, max(0.55, best_score / 7.0))

    def _is_likely_property_url(self, url: str, patterns: list[str]) -> bool:
        lowered = (url or "").lower()
        return any(pattern.lower() in lowered for pattern in patterns if pattern)

    async def _visible_result_candidates(self) -> list[Locator]:
        selectors = [
            '[role="option"]',
            '[role="listitem"]',
            'a[href*="/property"]',
            'a[href*="/address"]',
            'li',
            'tr',
        ]
        results: list[Locator] = []
        for selector in selectors:
            try:
                locator = self.page.locator(selector)
                count = min(await locator.count(), 12)
                for index in range(count):
                    item = locator.nth(index)
                    text_value = (await item.inner_text(timeout=1200)).strip()
                    if len(text_value) >= 8:
                        results.append(item)
                if results:
                    return results
            except Exception:
                continue
        return results

    async def discover_first_level_tabs(self) -> list[dict[str, Any]]:
        selectors = [
            '[role="tab"]',
            "button[role='tab']",
            "[aria-controls]",
        ]
        tabs: list[dict[str, Any]] = []
        for selector in selectors:
            try:
                locator = self.page.locator(selector)
                count = min(await locator.count(), 12)
                for index in range(count):
                    item = locator.nth(index)
                    try:
                        if not await item.is_visible(timeout=1200):
                            continue
                    except Exception:
                        continue
                    try:
                        label = (await item.inner_text(timeout=1500)).strip()
                    except Exception:
                        label = ""
                    if not label:
                        continue
                    tabs.append(
                        {
                            "label": label,
                            "key": normalize_traversal_key(label),
                            "selector": selector,
                            "index": index,
                        }
                    )
                if tabs:
                    break
            except Exception:
                continue
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for tab in tabs:
            key = tab["key"]
            if key in seen:
                continue
            seen.add(key)
            deduped.append(tab)
        return deduped

    async def visit_first_level_tab(self, tab_key: str, discovered_tabs: list[dict[str, Any]]) -> Optional[str]:
        normalized = normalize_traversal_key(tab_key)
        chosen = None
        for tab in discovered_tabs:
            if normalized and normalized in {tab["key"], normalize_traversal_key(tab["label"])}:
                chosen = tab
                break
        if chosen is None and discovered_tabs:
            chosen = discovered_tabs[0]
        if chosen is None:
            return None
        try:
            locator = self.page.get_by_role("tab", name=chosen["label"]).first
            await self.click_locator(locator, "first-level tab")
        except Exception:
            locator = self.page.locator(f"[role='tab']").nth(chosen["index"])
            await self.click_locator(locator, "first-level tab")
        return chosen["key"]

    async def expand_first_level_sections(self) -> list[str]:
        selectors = [
            "button[aria-expanded='false']",
            'button[aria-expanded="false"]',
            "summary",
            'button:has-text("Show more")',
            'button:has-text("More")',
            'button:has-text("Expand")',
            'button:has-text("View more")',
        ]
        expanded: list[str] = []
        seen: set[str] = set()
        for selector in selectors:
            try:
                locator = self.page.locator(selector)
                count = min(await locator.count(), 10)
                for index in range(count):
                    item = locator.nth(index)
                    try:
                        if not await item.is_visible(timeout=1200):
                            continue
                    except Exception:
                        continue
                    try:
                        label = (await item.inner_text(timeout=1500)).strip()
                    except Exception:
                        label = ""
                    normalized = normalize_traversal_key(label or selector)
                    if normalized in seen:
                        continue
                    seen.add(normalized)
                    await self.click_locator(item, "expand first-level section")
                    expanded.append(label or selector)
            except Exception:
                continue
        return expanded

    async def extract_current_section_payload(self, active_tab_key: str | None) -> dict[str, Any]:
        body_text = await self.safe_body_text()
        key = normalize_traversal_key(active_tab_key or "") or "active_section"
        return {
            key: {
                "key": key,
                "title": key.replace("_", " ").title(),
                "text": body_text[:20000],
            }
        }

    async def _resolve_search_results(self, variables: dict[str, Any]) -> tuple[Optional[str], float]:
        candidates = await self._visible_result_candidates()
        if not candidates:
            return None, 0.0
        scored: list[tuple[float, str, Locator]] = []
        for locator in candidates:
            try:
                text_value = (await locator.inner_text(timeout=1500)).strip()
            except Exception:
                continue
            score, raw = address_score(text_value, variables)
            if score > 0:
                scored.append((score, raw, locator))
        if not scored:
            return None, 0.0
        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best_text, best_locator = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else -99.0
        candidate_list = [item[1] for item in scored[:5]]
        if best_score <= 0:
            return None, 0.0
        if second_score > 0 and (best_score - second_score) < 1.5:
            raise ReviewRequired(f"ambiguous_match:{json.dumps(candidate_list)}")
        await self.click_locator(best_locator, "search result match")
        return best_text, min(0.99, max(0.55, best_score / 9.0))

    async def wait_for_navigation_or_property(
        self,
        timeout_ms: int,
        success_url_patterns: list[str],
        fallback_texts: list[str],
        variables: dict[str, Any],
    ) -> tuple[Optional[str], float]:
        start_url = self.page.url
        deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)
        while asyncio.get_running_loop().time() < deadline:
            await self.ensure_normal_state()
            current_url = self.page.url
            body_text = (await self.safe_body_text()).lower()
            if current_url != start_url and self._is_likely_property_url(current_url, success_url_patterns):
                return None, 0.8
            if self._is_likely_property_url(current_url, success_url_patterns):
                return None, 0.75
            if any(text.lower() in body_text for text in fallback_texts):
                return None, 0.7
            matched_address, confidence = await self._resolve_search_results(variables)
            if matched_address:
                await self.wait_for_settle()
                return matched_address, confidence
            await asyncio.sleep(0.5)
        body_text = (await self.safe_body_text()).lower()
        if any(text.lower() in body_text for text in fallback_texts):
            return None, 0.6
        raise ReplayFailure("navigation_or_property_page_not_detected")

    async def extract_from_current_property_page(self) -> tuple[dict[str, Any], dict[str, Any]]:
        title = ""
        try:
            title = await self.page.title()
        except Exception:
            pass
        body_text = await self.safe_body_text()
        combined = "\n".join(part for part in [title, body_text] if part).strip()
        proposed = parse_property_payload(combined, combined)
        raw_payload = {
            "details_url": self.page.url,
            "page_title": title,
            "property_page_text": body_text[:20000],
        }
        return proposed, raw_payload

    async def execute(self, manifest: dict[str, Any], variables: dict[str, Any]) -> ReplayResult:
        await self.ensure_normal_state()
        matched_address: Optional[str] = None
        match_confidence = 0.0
        raw_payload: dict[str, Any] = {
            "workflow_name": manifest.get("workflow_name"),
            "steps": [],
            "discovered_tabs": [],
            "section_order": [],
            "sections": {},
            "expanded_sections": [],
        }
        proposed_updates: dict[str, Any] = {}
        traversal_state: dict[str, Any] = {"tabs": [], "active_tab_key": None}

        for step in manifest.get("steps") or []:
            step_type = step.get("type")
            print(f"[replay] step {step_type}", flush=True)
            await self.ensure_normal_state()
            raw_payload["steps"].append({"type": step_type, "url": self.page.url})

            if step_type == "navigate_if_needed":
                url = substitute_template(step.get("url") or "", variables)
                patterns = step.get("url_patterns") or []
                if url and not any(pattern and pattern in self.page.url for pattern in patterns):
                    await self.page.goto(url, wait_until="domcontentloaded")
                    await self.wait_for_settle()
                continue

            if step_type == "focus":
                locator = await self.resolve_locator(step.get("target") or {}, step_type=step_type)
                await self.focus_locator(locator, "focus target")
                continue

            if step_type == "click":
                locator = await self.resolve_locator(step.get("target") or {}, step_type=step_type)
                await self.click_locator(locator, "click target")
                continue

            if step_type == "fill":
                locator = await self.resolve_locator(step.get("target") or {}, step_type=step_type)
                value = substitute_template(step.get("value_template") or "", variables)
                await self.fill_locator(locator, value)
                continue

            if step_type == "press":
                await self.wait_for_settle()
                await self.pace.pause("before press", 1200, 2600)
                await self.page.keyboard.press(step.get("key") or "Enter")
                await self.pace.pause("after press", 1200, 2400)
                continue

            if step_type == "wait_for_url":
                patterns = [item for item in (step.get("url_patterns") or []) if item]
                if not patterns:
                    continue
                await self.page.wait_for_function(
                    """(patterns) => patterns.some((pattern) => window.location.href.includes(pattern))""",
                    arg=patterns,
                    timeout=20000,
                )
                continue

            if step_type == "wait_for_navigation":
                nav_matched_address, confidence = await self.wait_for_navigation_or_property(
                    timeout_ms=int(step.get("timeout") or 10000),
                    success_url_patterns=step.get("success_url_patterns") or [],
                    fallback_texts=step.get("fallback_texts") or [],
                    variables=variables,
                )
                if nav_matched_address:
                    match_confidence = max(match_confidence, confidence)
                    matched_address = nav_matched_address
                else:
                    match_confidence = max(match_confidence, confidence)
                continue

            if step_type == "wait_for_text":
                await self.page.get_by_text(step.get("text") or "", exact=False).first.wait_for(timeout=15000)
                continue

            if step_type == "wait_for_selector":
                locator = await self.resolve_locator(step.get("target") or {}, step_type=step_type)
                await locator.wait_for(timeout=15000)
                continue

            if step_type == "click_best_match":
                matched_address, match_confidence = await self.click_best_match(step.get("target_group") or {}, variables)
                continue

            if step_type == "extract_field":
                continue

            if step_type == "extract_group":
                targets = step.get("targets") or manifest.get("extract_targets") or {}
                details_locator = await self.resolve_locator(targets.get("property_summary") or {}, step_type="extract_group.property_summary")
                valuation_locator = await self.resolve_locator(targets.get("valuation_section") or {}, step_type="extract_group.valuation_section")
                details_text = await details_locator.inner_text(timeout=8000)
                valuation_text = await valuation_locator.inner_text(timeout=8000)
                proposed_updates = parse_property_payload(details_text, valuation_text)
                raw_payload.update(
                    {
                        "details_url": self.page.url,
                        "details_text": details_text,
                        "valuation_text": valuation_text,
                    }
                )
                continue

            if step_type == "collect_first_level_tabs":
                traversal_state["tabs"] = await self.discover_first_level_tabs()
                raw_payload["discovered_tabs"] = [tab["label"] for tab in traversal_state["tabs"]]
                raw_payload["tab_candidates"] = traversal_state["tabs"]
                continue

            if step_type == "visit_tab":
                if not traversal_state["tabs"]:
                    traversal_state["tabs"] = await self.discover_first_level_tabs()
                    raw_payload["discovered_tabs"] = [tab["label"] for tab in traversal_state["tabs"]]
                tab_key = step.get("tab_key") or step.get("tab_label") or ""
                active_tab_key = await self.visit_first_level_tab(tab_key, traversal_state["tabs"])
                if active_tab_key:
                    traversal_state["active_tab_key"] = active_tab_key
                    raw_payload["section_order"].append(active_tab_key)
                continue

            if step_type == "expand_sections":
                expanded = await self.expand_first_level_sections()
                raw_payload["expanded_sections"].extend(expanded)
                continue

            if step_type == "extract_sections":
                extracted = await self.extract_current_section_payload(traversal_state.get("active_tab_key"))
                raw_payload["sections"].update(extracted)
                continue

            raise ReplayFailure(f"unsupported_step:{step_type}")

        filtered = {key: value for key, value in proposed_updates.items() if key in ALLOWED_FIELDS}
        if not filtered:
            extracted_updates, extracted_payload = await self.extract_from_current_property_page()
            filtered = {key: value for key, value in extracted_updates.items() if key in ALLOWED_FIELDS}
            raw_payload.update(extracted_payload)
        evidence = {
            "final_url": self.page.url,
            "body_excerpt": (await self.safe_body_text())[:600],
            "matched_address": matched_address,
            "match_confidence": match_confidence,
        }
        return ReplayResult(
            matched_address=matched_address,
            match_confidence=match_confidence,
            proposed_updates=filtered,
            raw_payload=raw_payload,
            evidence=evidence,
        )
