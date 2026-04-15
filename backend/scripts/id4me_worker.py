import argparse
import csv
import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

load_dotenv()

AUTH_FILE = Path("auth.json")
RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)

ID4ME_URL = "https://id4me.me"
DASHBOARD_URL = "https://id4me.me/dashboard"
MAX_SELECT = 10

ID4ME_EMAIL = os.getenv("ID4ME_EMAIL", "").strip()
ID4ME_PASSWORD = os.getenv("ID4ME_PASSWORD", "").strip()
LOCAL_APP_BASE_URL = os.getenv("LOCAL_APP_BASE_URL", "").rstrip("/")
LOCAL_APP_API_KEY = os.getenv("LOCAL_APP_API_KEY", "").strip()
API_BASE_URL = os.getenv("API_BASE_URL", LOCAL_APP_BASE_URL).rstrip("/")
ENRICHMENT_MACHINE_TOKEN = os.getenv("ENRICHMENT_MACHINE_TOKEN", "").strip()
ENRICHMENT_MACHINE_ID = os.getenv("ENRICHMENT_MACHINE_ID", f"{os.getenv('COMPUTERNAME', 'id4me-worker')}-id4me").strip()
ID4ME_IDLE_POLL_SECONDS = max(5, int(os.getenv("ID4ME_IDLE_POLL_SECONDS", "10")))
BACKEND_API_KEY = (
    os.getenv("BACKEND_API_KEY", "").strip()
    or os.getenv("API_KEY", "").strip()
    or LOCAL_APP_API_KEY
)
CSV_PREVIEW_MAX_ROWS = max(10, int(os.getenv("ID4ME_CSV_PREVIEW_MAX_ROWS", "120")))
NETWORK_HITS_PREVIEW_MAX = max(1, int(os.getenv("ID4ME_NETWORK_HITS_PREVIEW_MAX", "8")))


def clean_text(value: str) -> str:
    return " ".join((value or "").replace("\xa0", " ").split())


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D+", "", value)
    if digits.startswith("61") and len(digits) == 11:
        digits = "0" + digits[2:]
    return digits


def extract_phones(text: str) -> list[str]:
    matches = re.findall(r"(?:\+61|0)[\d\s]{8,20}", text or "")
    out: list[str] = []
    for match in matches:
        phone = normalize_phone(match)
        if len(phone) >= 10:
            out.append(phone)
    return sorted(set(out))


def extract_emails(text: str) -> list[str]:
    return sorted(
        set(re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", text or "", flags=re.IGNORECASE))
    )


def merge_contacts(raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []

    for item in raw:
        matched = False
        for existing in merged:
            same_phone = bool(set(existing["phones"]) & set(item["phones"]))
            same_email = bool(set(existing["emails"]) & set(item["emails"]))
            same_name = clean_text(existing["name"]).lower() == clean_text(item["name"]).lower()

            if same_phone or same_email or same_name:
                existing["phones"] = sorted(set(existing["phones"]) | set(item["phones"]))
                existing["emails"] = sorted(set(existing["emails"]) | set(item["emails"]))
                if not existing.get("date_of_birth") and item.get("date_of_birth"):
                    existing["date_of_birth"] = item["date_of_birth"]
                matched = True
                break

        if not matched:
            merged.append(
                {
                    "name": item["name"],
                    "date_of_birth": item.get("date_of_birth"),
                    "phones": sorted(set(item["phones"])),
                    "emails": sorted(set(item["emails"])),
                }
            )

    return merged


def parse_csv_file(path: Path) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        field_map = {clean_text(name).lower(): name for name in fieldnames}

        name_key = field_map.get("name")
        dob_key = field_map.get("date of birth") or field_map.get("dob")
        mobile_key = field_map.get("mobile") or field_map.get("phone") or field_map.get("mobile phone")
        email_key = field_map.get("email") or field_map.get("email address")
        landline_key = field_map.get("landline")

        for row in reader:
            name = clean_text(row.get(name_key, "") if name_key else "")
            dob = clean_text(row.get(dob_key, "") if dob_key else "")
            mobile = clean_text(row.get(mobile_key, "") if mobile_key else "")
            email = clean_text(row.get(email_key, "") if email_key else "")
            landline = clean_text(row.get(landline_key, "") if landline_key else "")

            phones = extract_phones(" ".join([mobile, landline]))
            emails = extract_emails(email)

            if not name:
                continue
            if not phones and not emails:
                continue

            results.append(
                {
                    "name": name,
                    "date_of_birth": dob or None,
                    "phones": phones,
                    "emails": emails,
                }
            )

    return merge_contacts(results)


def parse_csv_rows_preview(path: Path, *, max_rows: int = CSV_PREVIEW_MAX_ROWS) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if len(rows) >= max_rows:
                break
            clean_row: dict[str, str] = {}
            for key, value in (row or {}).items():
                col = clean_text(str(key or ""))
                clean_row[col or "_"] = clean_text(str(value or ""))
            rows.append(clean_row)
    return rows


def build_manual_enrich_payload(
    contacts: list[dict[str, Any]],
    *,
    last_seen: str | None,
) -> dict[str, Any]:
    all_phones = sorted({phone for contact in contacts for phone in contact["phones"]})
    all_emails = sorted({email for contact in contacts for email in contact["emails"]})

    owner_name = None
    date_of_birth = None
    if len(contacts) == 1:
        owner_name = contacts[0]["name"]
        date_of_birth = contacts[0].get("date_of_birth")

    return {
        "owner_name": owner_name,
        "phones": all_phones,
        "emails": all_emails,
        "date_of_birth": date_of_birth,
        "last_seen": last_seen,
    }


def _capture_search_responses(page):
    hits: list[dict[str, Any]] = []

    def _on_response(resp):
        try:
            if resp.request.resource_type not in {"xhr", "fetch"}:
                return
            ctype = (resp.header_value("content-type") or "").lower()
            if "json" not in ctype:
                return
            payload = resp.json()
            blob = json.dumps(payload, ensure_ascii=False).lower()
            if any(
                token in blob
                for token in ("date of birth", "last seen", "mobile", "email", "landline", "name")
            ):
                hits.append(
                    {
                        "url": resp.url,
                        "status": resp.status,
                        "payload": payload,
                    }
                )
        except Exception:
            pass

    page.on("response", _on_response)
    return hits


def _count_payload_rows(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("rows", "results", "items", "data", "contacts"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
            if isinstance(value, dict):
                nested = _count_payload_rows(value)
                if nested:
                    return nested
    return 0


class Id4MeSession:
    def __init__(self, *, headless: bool):
        self.headless = headless
        self.pw = None
        self.browser = None
        self.context = None
        self.page = None

    def start(self):
        if self.page:
            return

        self.pw = sync_playwright().start()
        self.browser = self.pw.chromium.launch(headless=self.headless)

        if AUTH_FILE.exists():
            self.context = self.browser.new_context(
                storage_state=str(AUTH_FILE),
                accept_downloads=True,
            )
        else:
            self.context = self.browser.new_context(accept_downloads=True)

        self.page = self.context.new_page()
        self.ensure_authenticated()

    def stop(self):
        try:
            if self.context:
                self.context.storage_state(path=str(AUTH_FILE))
        except Exception:
            pass

        if self.browser:
            self.browser.close()
        if self.pw:
            self.pw.stop()

        self.pw = None
        self.browser = None
        self.context = None
        self.page = None

    def is_dashboard(self) -> bool:
        try:
            return "/dashboard" in self.page.url or self.page.get_by_role("searchbox").count() > 0
        except Exception:
            return False

    def is_login(self) -> bool:
        try:
            return (
                self.page.locator("#loginemail").count() > 0
                and self.page.locator("#loginpassword").count() > 0
            )
        except Exception:
            return False

    def ensure_authenticated(self):
        self.page.goto(DASHBOARD_URL, wait_until="load")
        self.page.wait_for_load_state("domcontentloaded")
        self.page.wait_for_load_state("load")

        if self.is_dashboard():
            return

        self.page.goto(ID4ME_URL, wait_until="load")
        self.page.wait_for_load_state("domcontentloaded")
        self.page.wait_for_load_state("load")

        if not self.is_login():
            raise RuntimeError(f"Login page not detected. URL={self.page.url}")

        if not ID4ME_EMAIL or not ID4ME_PASSWORD:
            raise RuntimeError("Missing ID4ME_EMAIL or ID4ME_PASSWORD in environment")

        self.page.locator("#loginemail").fill(ID4ME_EMAIL)
        self.page.locator("#loginpassword").fill(ID4ME_PASSWORD)
        self.page.get_by_role("button", name="Sign in").click()
        self.page.wait_for_url("**/dashboard", timeout=30000)
        self.context.storage_state(path=str(AUTH_FILE))

    def search(self, address: str):
        self.ensure_authenticated()
        search = self.page.get_by_role("searchbox", name=re.compile("Type in natural language", re.I))
        search.wait_for(state="visible", timeout=15000)
        search.click()
        search.fill("")
        search.fill(address)
        search.press("Enter")
        self.page.wait_for_timeout(3500)
        return self.page


def score_row_text(row_text: str) -> int:
    score = 0
    if extract_phones(row_text):
        score += 10
    if extract_emails(row_text):
        score += 8
    if len(row_text) > 40:
        score += 1
    return score


def _grid_container(page):
    candidates = [
        page.locator("[role='grid']").first,
        page.locator(".MuiDataGrid-virtualScroller").first,
        page.locator(".MuiDataGrid-main").first,
    ]
    for candidate in candidates:
        try:
            if candidate.count() > 0:
                return candidate
        except Exception:
            pass
    return None


def collect_rows_with_scroll(page) -> dict[str, int]:
    seen_scores: dict[str, int] = {}
    stable_rounds = 0
    container = _grid_container(page)

    for _ in range(35):
        rows = page.get_by_role("row")
        before = len(seen_scores)

        for index in range(rows.count()):
            try:
                row = rows.nth(index)
                text = clean_text(row.inner_text())
                if text and len(text) > 10:
                    seen_scores[text] = score_row_text(text)
            except Exception:
                continue

        try:
            if container:
                container.evaluate("(el) => el.scrollBy(0, 1500)")
            else:
                page.mouse.wheel(0, 1500)
        except Exception:
            page.mouse.wheel(0, 1500)

        page.wait_for_timeout(1200)

        if len(seen_scores) == before:
            stable_rounds += 1
        else:
            stable_rounds = 0

        if stable_rounds >= 3:
            break

    return seen_scores


def select_any_rows(page) -> int:
    row_scores = collect_rows_with_scroll(page)
    target_texts = [
        text for text, _score in sorted(row_scores.items(), key=lambda item: (-item[1], item[0]))[:MAX_SELECT]
    ]
    target_set = set(target_texts)

    container = _grid_container(page)
    try:
        if container:
            container.evaluate("(el) => el.scrollTo(0, 0)")
        else:
            page.keyboard.press("Home")
    except Exception:
        pass

    page.wait_for_timeout(800)

    selected = 0
    selected_texts: set[str] = set()
    stable_rounds = 0

    while selected < min(MAX_SELECT, len(target_set)) and stable_rounds < 4:
        before_selected = selected
        rows = page.get_by_role("row")

        for index in range(rows.count()):
            if selected >= MAX_SELECT:
                break

            try:
                row = rows.nth(index)
                row_text = clean_text(row.inner_text())

                if not row_text or row_text not in target_set or row_text in selected_texts:
                    continue

                if row.get_by_label("Unselect row").count() > 0:
                    selected_texts.add(row_text)
                    continue

                checkbox = row.get_by_label("Select row")
                if checkbox.count() == 0:
                    continue

                checkbox.check(timeout=2000)
                page.wait_for_timeout(150)

                if row.get_by_label("Unselect row").count() > 0:
                    selected += 1
                    selected_texts.add(row_text)

            except Exception:
                continue

        try:
            if container:
                container.evaluate("(el) => el.scrollBy(0, 1200)")
            else:
                page.mouse.wheel(0, 1200)
        except Exception:
            page.mouse.wheel(0, 1200)

        page.wait_for_timeout(1000)

        if selected == before_selected:
            stable_rounds += 1
        else:
            stable_rounds = 0

    return selected


def _save_json(path: Path, payload: Any) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def _export_csv(page) -> Path:
    export_btn = page.get_by_role("button", name="Export CSV")
    export_btn.wait_for(state="visible", timeout=10000)

    with page.expect_download(timeout=20000) as download_info:
        export_btn.click()

    download = download_info.value
    csv_path = RESULTS_DIR / f"export_{int(time.time())}.csv"
    download.save_as(str(csv_path))
    return csv_path


def enrich_address(session: Id4MeSession, address: str) -> dict[str, Any]:
    page = session.page
    network_hits = _capture_search_responses(page)
    page = session.search(address)

    body_text = page.locator("body").inner_text()
    if "No results found" in body_text:
        result = {
            "status": "no_results",
            "address": address,
            "results": [],
            "count": 0,
            "timestamp": int(time.time()),
        }
        result_path = RESULTS_DIR / f"result_{int(time.time())}.json"
        _save_json(result_path, result)
        result["file"] = str(result_path)
        return result

    selected_count = select_any_rows(page)
    if selected_count == 0:
        raise RuntimeError("No selectable rows found")

    csv_path = _export_csv(page)
    contacts = parse_csv_file(csv_path)
    debug_network_file = RESULTS_DIR / f"network_{int(time.time())}.json"
    _save_json(debug_network_file, network_hits)

    network_row_count_guess = max((_count_payload_rows(hit["payload"]) for hit in network_hits), default=0)

    result = {
        "status": "ok",
        "address": address,
        "results": contacts,
        "count": len(contacts),
        "timestamp": int(time.time()),
        "file": str(csv_path),
        "selected_count": selected_count,
        "source": "csv_export_selected_rows",
        "debug_network_file": str(debug_network_file),
        "network_row_count_guess": network_row_count_guess,
        "coverage_gap": max(0, network_row_count_guess - len(contacts)),
    }

    result_path = RESULTS_DIR / f"result_{int(time.time())}.json"
    _save_json(result_path, result)
    result["result_file"] = str(result_path)
    return result


def post_manual_enrichment(lead_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not LOCAL_APP_BASE_URL:
        raise RuntimeError("Missing LOCAL_APP_BASE_URL")
    if not LOCAL_APP_API_KEY:
        raise RuntimeError("Missing LOCAL_APP_API_KEY")

    url = f"{LOCAL_APP_BASE_URL}/api/leads/{lead_id}/enrich-manual"
    response = requests.post(
        url,
        headers={"x-api-key": LOCAL_APP_API_KEY},
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _machine_headers() -> dict[str, str]:
    if not API_BASE_URL:
        raise RuntimeError("Missing API_BASE_URL")
    if not ENRICHMENT_MACHINE_TOKEN:
        raise RuntimeError("Missing ENRICHMENT_MACHINE_TOKEN")
    headers = {
        "X-Enrichment-Machine-Token": ENRICHMENT_MACHINE_TOKEN,
        "X-Enrichment-Machine-Id": ENRICHMENT_MACHINE_ID,
    }
    if BACKEND_API_KEY:
        headers["X-API-KEY"] = BACKEND_API_KEY
    return headers


def claim_next_id4me_job() -> dict[str, Any] | None:
    response = requests.get(
        f"{API_BASE_URL}/api/enrichment-jobs/next",
        headers=_machine_headers(),
        params={"provider": "id4me"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json().get("job")


def claim_next_id4me_bundle() -> dict[str, Any]:
    response = requests.get(
        f"{API_BASE_URL}/api/enrichment-jobs/next",
        headers=_machine_headers(),
        params={"provider": "id4me"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def update_job_status(
    job_id: str,
    *,
    status: str,
    matched_address: str | None = None,
    error_message: str | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    response = requests.post(
        f"{API_BASE_URL}/api/enrichment-jobs/{job_id}/status",
        headers=_machine_headers(),
        json={
            "status": status,
            "matched_address": matched_address,
            "error_message": error_message,
            "note": note,
            "machine_id": ENRICHMENT_MACHINE_ID,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def submit_id4me_result(
    job_id: str,
    *,
    status: str,
    payload: dict[str, Any],
    raw_result: dict[str, Any],
    matched_address: str | None,
    csv_path: str | None,
    error_message: str | None = None,
) -> dict[str, Any]:
    response = requests.post(
        f"{API_BASE_URL}/api/enrichment-jobs/{job_id}/id4me-result",
        headers=_machine_headers(),
        json={
            "status": status,
            "matched_address": matched_address,
            "payload": payload,
            "raw_result": raw_result,
            "csv_path": csv_path,
            "error_message": error_message,
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def run_once(lead_id: str, address: str, *, headless: bool) -> dict[str, Any]:
    session = Id4MeSession(headless=headless)
    try:
        session.start()
        enrichment_result = enrich_address(session, address)

        if enrichment_result["status"] != "ok":
            return enrichment_result

        payload = build_manual_enrich_payload(
            enrichment_result["results"],
            last_seen=None,
        )
        posted = post_manual_enrichment(lead_id, payload)

        final = {
            "status": "ok",
            "lead_id": lead_id,
            "address": address,
            "payload": payload,
            "remote_result": enrichment_result,
            "local_response": posted,
        }
        output_path = RESULTS_DIR / f"posted_{int(time.time())}.json"
        _save_json(output_path, final)
        final["posted_file"] = str(output_path)
        return final

    finally:
        session.stop()


def run_worker(*, headless: bool) -> None:
    if not API_BASE_URL:
        raise RuntimeError("API_BASE_URL is required for worker mode")
    _machine_headers()

    session = Id4MeSession(headless=headless)
    session.start()
    print(json.dumps({
        "status": "worker_started",
        "machine_id": ENRICHMENT_MACHINE_ID,
        "api_base_url": API_BASE_URL,
        "headless": headless,
        "idle_poll_seconds": ID4ME_IDLE_POLL_SECONDS,
    }, ensure_ascii=False))

    while True:
        try:
            bundle = claim_next_id4me_bundle()
            job = bundle.get("job")
            lead = bundle.get("lead") or {}
            if not job:
                time.sleep(ID4ME_IDLE_POLL_SECONDS)
                continue

            lead_id = str(job["lead_id"])
            address_parts = [str(lead.get("address") or "").strip(), str(lead.get("suburb") or "").strip(), str(lead.get("state") or "").strip(), str(lead.get("postcode") or "").strip()]
            address = " ".join(part for part in address_parts if part).strip() or str(lead.get("address") or "").strip()
            if not address:
                update_job_status(job["id"], status="failed", error_message="Lead address missing")
                continue

            enrichment_result = enrich_address(session, address)
            if enrichment_result["status"] == "ok":
                payload = build_manual_enrich_payload(
                    enrichment_result["results"],
                    last_seen=None,
                )
                submit_id4me_result(
                    job["id"],
                    status="completed",
                    payload=payload,
                    raw_result=enrichment_result,
                    matched_address=address,
                    csv_path=enrichment_result.get("file"),
                )
            elif enrichment_result["status"] == "no_results":
                update_job_status(
                    job["id"],
                    status="no_results",
                    matched_address=address,
                    note="ID4ME search completed with no exported rows",
                )
            else:
                update_job_status(
                    job["id"],
                    status="failed",
                    matched_address=address,
                    error_message=str(enrichment_result),
                )
        except KeyboardInterrupt:
            raise
        except PlaywrightTimeoutError as exc:
            print(json.dumps({"status": "worker_error", "error": str(exc)}, ensure_ascii=False))
            time.sleep(ID4ME_IDLE_POLL_SECONDS)
        except Exception as exc:
            print(json.dumps({"status": "worker_error", "error": str(exc)}, ensure_ascii=False))
            time.sleep(ID4ME_IDLE_POLL_SECONDS)
        finally:
            try:
                if session.page is None:
                    session.start()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Run one id4me enrichment and post it back to the local app.")
    parser.add_argument("--lead-id")
    parser.add_argument("--address")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--worker", action="store_true")
    args = parser.parse_args()

    if args.worker:
        run_worker(headless=args.headless)
        return

    if not args.lead_id or not args.address:
        raise SystemExit("--lead-id and --address are required unless --worker is used")

    try:
        result = run_once(args.lead_id, args.address, headless=args.headless)
    except PlaywrightTimeoutError as exc:
        result = {"status": "error", "error": str(exc), "lead_id": args.lead_id, "address": args.address}
    except Exception as exc:
        result = {"status": "error", "error": str(exc), "lead_id": args.lead_id, "address": args.address}

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
