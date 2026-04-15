import argparse
import asyncio
import json
import os
import random
import re
import shutil
import sys
import tempfile
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
from playwright.async_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, async_playwright

from workflow_replay import ReplayFailure, ReviewRequired, WorkflowNotTaughtError, WorkflowReplayEngine
from workflow_teaching import WorkflowTeacher
from workflow_trace import capture_safe_screenshot, list_workflows, load_workflow, page_contains_sensitive_input, workflow_path, write_placeholder_screenshot


ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = ROOT / "backend" / "artifacts" / "cotality"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunnerConfig:
    api_base_url: str = os.getenv("API_BASE_URL", "http://localhost:8001").rstrip("/")
    machine_id: str = os.getenv("ENRICHMENT_MACHINE_ID", "local-cotality-runner")
    machine_token: str = os.getenv("ENRICHMENT_MACHINE_TOKEN", "")
    base_url: str = os.getenv("COTALITY_BASE_URL", "https://rpp.corelogic.com.au/").rstrip("/")
    profile_dir: Path = Path(os.getenv("COTALITY_PROFILE_DIR", str(ROOT / "backend" / "scripts" / ".cotality-temp-profiles")))
    headless: bool = os.getenv("COTALITY_HEADLESS", "false").lower() == "true"
    login_wait_seconds: int = max(60, int(os.getenv("COTALITY_LOGIN_WAIT_SECONDS", "900")))
    max_jobs_per_hour: int = max(1, int(os.getenv("ENRICHMENT_MAX_JOBS_PER_HOUR", "12")))
    delay_min_ms: int = max(250, int(os.getenv("ENRICHMENT_DELAY_MIN_MS", "1500")))
    delay_max_ms: int = max(250, int(os.getenv("ENRICHMENT_DELAY_MAX_MS", "6000")))
    cooldown_after_n_jobs: int = max(1, int(os.getenv("ENRICHMENT_COOLDOWN_AFTER_N_JOBS", "5")))
    cooldown_seconds: int = max(30, int(os.getenv("ENRICHMENT_COOLDOWN_SECONDS", "120")))
    idle_poll_seconds_min: int = 5
    idle_poll_seconds_max: int = 20
    max_retries: int = 3
    rpdata_username: str = os.getenv("RPDATA_USERNAME", "")
    rpdata_password: str = os.getenv("RPDATA_PASSWORD", "")


@dataclass
class PaceController:
    config: RunnerConfig
    completed_jobs: int = 0
    recent_job_timestamps: deque[float] = field(default_factory=deque)

    async def pause(self, reason: str, minimum_ms: Optional[int] = None, maximum_ms: Optional[int] = None) -> None:
        minimum = minimum_ms if minimum_ms is not None else self.config.delay_min_ms
        maximum = maximum_ms if maximum_ms is not None else self.config.delay_max_ms
        if random.random() < 0.15:
            minimum = max(minimum, 8000)
            maximum = max(maximum, 12000)
        delay_ms = random.randint(minimum, maximum)
        print(f"[pace] {reason}: sleeping {delay_ms}ms", flush=True)
        await asyncio.sleep(delay_ms / 1000)

    async def idle_pause(self, reason: str = "idle polling gap") -> None:
        seconds = random.randint(self.config.idle_poll_seconds_min, self.config.idle_poll_seconds_max)
        print(f"[pace] {reason}: sleeping {seconds}s", flush=True)
        await asyncio.sleep(seconds)

    async def enforce_hourly_limit(self) -> None:
        now = time.time()
        cutoff = now - 3600
        while self.recent_job_timestamps and self.recent_job_timestamps[0] < cutoff:
            self.recent_job_timestamps.popleft()
        if len(self.recent_job_timestamps) < self.config.max_jobs_per_hour:
            return
        wait_seconds = max(5, int(self.recent_job_timestamps[0] + 3600 - now))
        print(f"[pace] hourly cap hit ({self.config.max_jobs_per_hour}/hour); sleeping {wait_seconds}s", flush=True)
        await asyncio.sleep(wait_seconds)

    async def record_job_completion(self) -> None:
        self.completed_jobs += 1
        self.recent_job_timestamps.append(time.time())
        if self.completed_jobs % self.config.cooldown_after_n_jobs == 0:
            cooldown = random.randint(self.config.cooldown_seconds, max(self.config.cooldown_seconds, self.config.cooldown_seconds * 2))
            print(f"[pace] cooldown after {self.completed_jobs} jobs: sleeping {cooldown}s", flush=True)
            await asyncio.sleep(cooldown)


class CotalityRunner:
    def __init__(self, config: RunnerConfig):
        self.config = config
        self.playwright = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.browser_backend = "uninitialized"
        self.temp_profile_dir: Optional[Path] = None
        self.http = httpx.AsyncClient(
            base_url=self.config.api_base_url,
            timeout=httpx.Timeout(60.0, connect=20.0),
            headers={
                "X-Enrichment-Machine-Token": self.config.machine_token,
                "X-Enrichment-Machine-Id": self.config.machine_id,
            },
        )
        self.pace = PaceController(config)

    async def start(self) -> None:
        ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
        self.temp_profile_dir = Path(tempfile.mkdtemp(prefix="cotality-rp-data-", dir=str(self.config.profile_dir.parent)))
        self.playwright = await async_playwright().start()
        launch_options = {
            "user_data_dir": str(self.temp_profile_dir),
            "headless": self.config.headless,
            "viewport": {"width": 1440, "height": 960},
            "args": ["--start-maximized"],
        }
        self.context = await self._launch_persistent_browser(launch_options)
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        print(f"[runner] browser launched with fresh profile {self.temp_profile_dir}", flush=True)

    async def _launch_persistent_browser(self, launch_options: dict) -> BrowserContext:
        candidates: list[tuple[str, dict[str, Any]]] = [
            ("chrome", {"channel": "chrome"}),
            ("msedge", {"channel": "msedge"}),
        ]
        chrome_path = shutil.which("chrome") or shutil.which("chrome.exe")
        edge_path = shutil.which("msedge") or shutil.which("msedge.exe")
        if chrome_path:
            candidates.append((chrome_path, {"executable_path": chrome_path}))
        if edge_path:
            candidates.append((edge_path, {"executable_path": edge_path}))
        candidates.append(("playwright-chromium", {}))

        last_error: Optional[Exception] = None
        for label, extra in candidates:
            try:
                context = await self.playwright.chromium.launch_persistent_context(**launch_options, **extra)
                self.browser_backend = label
                print(f"[runner] browser backend: {label}", flush=True)
                return context
            except Exception as error:
                last_error = error
                print(f"[runner] browser launch failed for {label}: {error}", flush=True)
        raise RuntimeError("No supported browser could be launched. Install Chrome or Edge, or run: playwright install chromium") from last_error

    async def close(self) -> None:
        await self.http.aclose()
        if self.context:
            await self.context.close()
        if self.playwright:
            await self.playwright.stop()
        if self.temp_profile_dir and self.temp_profile_dir.exists():
            shutil.rmtree(self.temp_profile_dir, ignore_errors=True)

    async def ensure_page(self) -> Page:
        if not self.page:
            raise RuntimeError("Browser page is not available")
        return self.page

    async def safe_body_text(self, page: Page) -> str:
        try:
            return await page.locator("body").inner_text(timeout=3000)
        except Exception:
            return ""

    async def safe_title(self, page: Page) -> str:
        try:
            return await page.title()
        except Exception:
            return ""

    async def wait_for_settle(self, page: Page) -> None:
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=15000)
            await page.wait_for_load_state("networkidle", timeout=8000)
        except PlaywrightTimeoutError:
            pass
        await self.pace.pause("visual settle")

    async def goto_base(self) -> Page:
        page = await self.ensure_page()
        await page.goto(self.config.base_url, wait_until="domcontentloaded")
        await self.wait_for_settle(page)
        return page

    def is_cotality_url(self, url: str) -> bool:
        current = (url or "").lower()
        base = self.config.base_url.lower()
        if not current:
            return False
        return ("corelogic" in current) or ("rpp." in current) or current.startswith(base)

    async def first_visible_search_locator(self, page: Page):
        candidates = [
            'input[placeholder*="Search" i]',
            'input[placeholder*="address" i]',
            'input[aria-label*="Search" i]',
            'input[aria-label*="address" i]',
            'input[name*="search" i]',
            'input[name*="address" i]',
            '[role="searchbox"]',
            '[role="combobox"] input',
            'input[type="search"]',
        ]
        try:
            manifest = self.load_workflow_manifest("cotality_search_property")
            for step in manifest.get("steps") or []:
                if step.get("type") not in {"focus", "fill"}:
                    continue
                for candidate in (step.get("target") or {}).get("selectors") or []:
                    if candidate.get("type") == "css":
                        candidates.insert(0, candidate.get("value"))
        except Exception:
            pass
        seen = set()
        for selector in candidates:
            if not selector or selector in seen:
                continue
            seen.add(selector)
            try:
                locator = page.locator(selector).first
                if await locator.count() > 0 and await locator.is_visible(timeout=1500):
                    return locator
            except Exception:
                continue
        return None

    async def is_authenticated_search_page(self, page: Page) -> bool:
        url = page.url.lower()
        search_locator = await self.first_visible_search_locator(page)
        if search_locator is not None:
            return True
        if any(token in url for token in ("/search", "property-search", "property", "/home", "/dashboard")) and self.is_cotality_url(url):
            return True
        return False

    async def _first_visible_locator(self, page: Page, selectors: list[str]):
        seen = set()
        for selector in selectors:
            if not selector or selector in seen:
                continue
            seen.add(selector)
            try:
                locator = page.locator(selector).first
                if await locator.count() > 0 and await locator.is_visible(timeout=1500):
                    return locator
            except Exception:
                continue
        return None

    async def _first_visible_role_button(self, page: Page, names: list[str]):
        for name in names:
            try:
                locator = page.get_by_role("button", name=name).first
                if await locator.count() > 0 and await locator.is_visible(timeout=1500):
                    return locator
            except Exception:
                continue
        return None

    async def detect_login_variant(self, page: Page) -> Optional[str]:
        if await self.is_authenticated_search_page(page):
            return None
        url = (page.url or "").lower()
        title = (await self.safe_title(page)).lower()
        text_content = (await self.safe_body_text(page)).lower()
        password_field = await self._first_visible_locator(page, ["input[type=password]"])
        has_password = password_field is not None
        login_signals = 0
        if any(token in url for token in ("login", "signin", "authenticate")):
            login_signals += 1
        if any(token in title for token in ("sign in", "login", "log in", "rp data")):
            login_signals += 1
        if has_password:
            login_signals += 1
        if any(token in text_content for token in ("sign in", "log in", "email", "username", "password")):
            login_signals += 1
        if login_signals < 2:
            return None
        if any(token in text_content for token in ("username", "user name")):
            return "username"
        if any(token in text_content for token in ("email", "email address")):
            return "email"
        username_locator = await self._first_visible_locator(page, ['input[name*="user" i]', 'input[id*="user" i]'])
        if username_locator is not None:
            return "username"
        email_locator = await self._first_visible_locator(page, ["input[type=email]", 'input[name*="email" i]', 'input[placeholder*="email" i]'])
        if email_locator is not None:
            return "email"
        return "email"

    async def is_login_page(self, page: Page) -> bool:
        return (await self.detect_login_variant(page)) is not None

    async def detect_login_challenge(self, page: Page) -> Optional[str]:
        text_content = (await self.safe_body_text(page)).lower()
        url = (page.url or "").lower()
        title = (await self.safe_title(page)).lower()
        signals = {
            "mfa": ("authenticator app", "verification code", "two-factor", "2fa", "multi-factor", "one-time code", "otp"),
            "captcha": ("captcha", "verify you are human", "i'm not a robot", "recaptcha"),
            "sso": ("single sign-on", "sso", "okta", "microsoft", "azure ad", "identity provider"),
            "challenge": ("verify your identity", "security challenge", "additional verification"),
        }
        combined = " ".join([text_content, url, title])
        for label, markers in signals.items():
            if any(marker in combined for marker in markers):
                return label
        return None

    async def wait_for_login_success(self, page: Page, timeout_seconds: int = 20) -> bool:
        original_url = (page.url or "").lower()
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            await asyncio.sleep(1)
            await self.wait_for_settle(page)
            if await self.is_authenticated_search_page(page):
                return True
            current_url = (page.url or "").lower()
            if current_url != original_url and not await self.is_login_page(page):
                return True
            if await self.detect_login_challenge(page):
                return False
        return False

    async def auto_login(self, page: Page, job_id: Optional[str] = None) -> bool:
        variant = await self.detect_login_variant(page)
        if not variant:
            return False
        print("[runner] login page detected", flush=True)
        challenge = await self.detect_login_challenge(page)
        if challenge:
            print("[runner] manual challenge required", flush=True)
            return False
        username = self.config.rpdata_username
        password = self.config.rpdata_password
        if not username or not password:
            print("[runner] manual challenge required", flush=True)
            return False
        username_selectors = {
            "username": [
                'input[name*="user" i]',
                'input[id*="user" i]',
                'input[placeholder*="username" i]',
                'input[aria-label*="username" i]',
                "input[type=email]",
                'input[name*="email" i]',
                'input[placeholder*="email" i]',
            ],
            "email": [
                "input[type=email]",
                'input[name*="email" i]',
                'input[placeholder*="email" i]',
                'input[aria-label*="email" i]',
                'input[name*="user" i]',
                'input[id*="user" i]',
                'input[placeholder*="username" i]',
            ],
        }
        username_field = await self._first_visible_locator(page, username_selectors.get(variant, username_selectors["email"]))
        password_field = await self._first_visible_locator(page, ["input[type=password]"])
        if username_field is None or password_field is None:
            print("[runner] manual challenge required", flush=True)
            return False
        print(f"[runner] login variant selected: {variant}", flush=True)
        await username_field.fill(username)
        await password_field.fill(password)
        print("[runner] credentials submitted", flush=True)
        try:
            await password_field.press("Enter")
        except Exception:
            pass
        if not await self.wait_for_login_success(page):
            submit_button = await self._first_visible_role_button(page, ["Login", "Log in", "Sign in", "Sign In"])
            if submit_button is not None:
                try:
                    await submit_button.click(timeout=5000)
                except Exception:
                    pass
            if not await self.wait_for_login_success(page):
                print("[runner] manual challenge required", flush=True)
                return False
        print("[runner] login success", flush=True)
        if job_id:
            await self.update_job_status(job_id, "running")
        return True

    async def detect_unusual_state(self, page: Page) -> Optional[str]:
        url = page.url.lower()
        text_content = (await self.safe_body_text(page)).lower()
        markers = [
            "captcha",
            "access denied",
            "temporarily blocked",
            "unusual traffic",
            "verify you are human",
        ]
        for marker in markers:
            if marker in text_content or marker in url:
                return marker
        return None

    async def wait_for_manual_login(self, job_id: Optional[str] = None) -> bool:
        page = await self.ensure_page()
        print("[runner] manual challenge required", flush=True)
        if job_id:
            await self.update_job_status(job_id, "login_required")
        deadline = time.time() + self.config.login_wait_seconds
        while time.time() < deadline:
            await asyncio.sleep(5)
            await self.wait_for_settle(page)
            unusual = await self.detect_unusual_state(page)
            if unusual:
                raise RuntimeError(f"Blocked or unusual page detected during login wait: {unusual}")
            if not await self.is_login_page(page):
                print("[runner] login success", flush=True)
                if job_id:
                    await self.update_job_status(job_id, "running")
                return True
        return False

    async def ensure_authenticated(self, job_id: Optional[str] = None) -> None:
        page = await self.ensure_page()
        if await self.is_authenticated_search_page(page):
            print("[runner] authenticated/search page detected", flush=True)
            return
        if await self.is_login_page(page):
            if await self.auto_login(page, job_id):
                return
            if not await self.wait_for_manual_login(job_id):
                raise RuntimeError("Timed out waiting for manual login")

    async def ensure_base_or_current_page(self) -> Page:
        page = await self.ensure_page()
        if page.url and page.url != "about:blank":
            await self.wait_for_settle(page)
            return page
        return await self.goto_base()

    async def ensure_base_or_current_page_for_replay(self) -> Page:
        page = await self.ensure_page()
        current_url = (page.url or "").strip()
        if current_url and current_url != "about:blank" and self.is_cotality_url(current_url):
            await self.wait_for_settle(page)
            return page
        await page.goto(self.config.base_url, wait_until="domcontentloaded")
        await self.wait_for_settle(page)
        await self.ensure_authenticated()
        return page

    def load_workflow_manifest(self, workflow_name: str) -> dict[str, Any]:
        try:
            return load_workflow(workflow_name)
        except FileNotFoundError as error:
            raise WorkflowNotTaughtError(f"{workflow_name} not taught: {workflow_path(workflow_name)}") from error

    def build_variables(self, lead: Optional[dict[str, Any]] = None, example_address: Optional[str] = None) -> dict[str, Any]:
        if example_address:
            address = example_address.strip()
            parts = [part.strip() for part in address.split(",") if part.strip()]
            suburb = ""
            state = ""
            postcode = ""
            if len(parts) >= 2:
                tail = parts[-1]
                match = re.match(r"(.+?)\s+([A-Z]{2,3})\s+(\d{4})$", tail)
                if match:
                    suburb = match.group(1).strip()
                    state = match.group(2).strip()
                    postcode = match.group(3).strip()
                else:
                    suburb = tail
            return {
                "full_address": address,
                "street": parts[0] if parts else address,
                "suburb": suburb,
                "state": state,
                "postcode": postcode,
            }
        lead = lead or {}
        street = str(lead.get("address") or "").strip()
        suburb = str(lead.get("suburb") or "").strip()
        state = str(lead.get("state") or "").strip()
        postcode = str(lead.get("postcode") or "").strip()
        full_address = " ".join(part for part in [street, suburb, state, postcode] if part).strip()
        return {
            "full_address": full_address,
            "street": street,
            "suburb": suburb,
            "state": state,
            "postcode": postcode,
        }

    async def capture_screenshot(self, prefix: str) -> str:
        page = await self.ensure_page()
        ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
        path = ARTIFACT_ROOT / f"{prefix}_{int(time.time())}.png"
        if await self.is_login_page(page) or await self.detect_login_challenge(page) or await page_contains_sensitive_input(page):
            write_placeholder_screenshot(path)
            return str(path)
        await capture_safe_screenshot(page, path)
        return str(path)

    async def post_result(self, job_id: str, payload: dict[str, Any]) -> None:
        response = await self.http.post(f"/api/enrichment-jobs/{job_id}/result", json=payload)
        response.raise_for_status()

    async def update_job_status(self, job_id: str, status: str, **extra) -> None:
        payload = {"status": status, **extra}
        response = await self.http.post(f"/api/enrichment-jobs/{job_id}/status", json=payload)
        response.raise_for_status()

    async def claim_next_job(self) -> Optional[dict[str, Any]]:
        response = await self.http.get("/api/enrichment-jobs/next")
        response.raise_for_status()
        payload = response.json()
        if payload.get("job"):
            print(
                f"[runner] claimed queued job id={payload['job']['id']} lead_id={payload['job']['lead_id']} provider={payload['job'].get('provider')} status={payload['job'].get('status')}",
                flush=True,
            )
        return payload.get("job") and payload

    async def preflight(self) -> None:
        page = await self.ensure_base_or_current_page()
        print(f"[preflight] API_BASE_URL={self.config.api_base_url}", flush=True)
        print(f"[preflight] browser_backend={self.browser_backend}", flush=True)
        print(f"[preflight] profile_dir={self.config.profile_dir}", flush=True)
        print(f"[preflight] workflows={', '.join(list_workflows()) if list_workflows() else 'missing'}", flush=True)

        try:
            response = await self.http.get("/docs")
            response.raise_for_status()
            print("[preflight] backend=ok", flush=True)
        except Exception as error:
            raise RuntimeError(f"Backend not reachable at {self.config.api_base_url}: {error}") from error

        if not self.config.machine_token:
            raise RuntimeError("ENRICHMENT_MACHINE_TOKEN is missing in the runner environment")
        print("[preflight] machine_token=configured", flush=True)

        try:
            response = await self.http.get("/api/enrichment-jobs/auth-check")
            response.raise_for_status()
            print("[preflight] token_auth=ok", flush=True)
        except httpx.HTTPStatusError as error:
            detail = error.response.text
            raise RuntimeError(f"Machine token rejected by backend: {detail}") from error

        await page.goto(self.config.base_url, wait_until="domcontentloaded")
        await self.wait_for_settle(page)
        if await self.is_login_page(page):
            print("[preflight] cotality_page=login_required", flush=True)
        elif await self.is_authenticated_search_page(page):
            print("[preflight] cotality_page=authenticated_search_ready", flush=True)
        else:
            print("[preflight] cotality_page=loaded", flush=True)

    async def _run_workflow(self, workflow_name: str, variables: dict[str, Any], job_id: Optional[str] = None) -> dict[str, Any]:
        page = await self.ensure_page()
        manifest = self.load_workflow_manifest(workflow_name)
        engine = WorkflowReplayEngine(
            page=page,
            pace=self.pace,
            detect_unusual_state=self.detect_unusual_state,
            artifact_root=ARTIFACT_ROOT,
            ensure_session=lambda: self.ensure_authenticated(job_id),
        )
        await self.ensure_authenticated(job_id)
        result = await engine.execute(manifest, variables)
        return {
            "matched_address": result.matched_address,
            "match_confidence": result.match_confidence,
            "proposed_updates_json": result.proposed_updates,
            "raw_payload_json": {**result.raw_payload, "evidence": result.evidence},
        }

    async def teach_workflow(self, workflow_name: str, example_address: Optional[str]) -> None:
        page = await self.ensure_base_or_current_page()
        await self.ensure_authenticated()
        teacher = WorkflowTeacher(page, ARTIFACT_ROOT, self.config.base_url)
        path = await teacher.teach(workflow_name, example_address=example_address)
        print(f"[teach] workflow saved: {path}", flush=True)
        if workflow_name != "cotality_full_enrich" or example_address:
            try:
                await self.validate_workflow(
                    workflow_name if workflow_name != "cotality_full_enrich" else "cotality_search_property",
                    example_address=example_address,
                )
            except Exception as error:
                print(f"[teach] validation warning: {error}", flush=True)

    async def validate_workflow(self, workflow_name: str, example_address: Optional[str]) -> None:
        await self.ensure_base_or_current_page_for_replay()
        await self.ensure_authenticated()
        if workflow_name in {"cotality_search_property", "cotality_full_enrich"} and not example_address:
            raise RuntimeError("--example-address is required for this workflow")
        variables = self.build_variables(example_address=example_address)
        result = await self._run_workflow(workflow_name, variables)
        print(json.dumps(result, indent=2), flush=True)
        print(f"[validate] workflow replay succeeded: {workflow_name}", flush=True)

    async def run_workflow(self, workflow_name: str, example_address: Optional[str]) -> None:
        await self.ensure_base_or_current_page_for_replay()
        await self.ensure_authenticated()
        if workflow_name in {"cotality_search_property", "cotality_full_enrich"} and not example_address:
            raise RuntimeError("--example-address is required for this workflow")
        variables = self.build_variables(example_address=example_address)
        result = await self._run_workflow(workflow_name, variables)
        print(json.dumps(result, indent=2), flush=True)

    async def process_job(self, payload: dict[str, Any]) -> None:
        page = await self.goto_base()
        await self.ensure_authenticated(payload["job"]["id"])
        job = payload["job"]
        lead = payload["lead"]
        job_id = job["id"]
        variables = self.build_variables(lead=lead)

        try:
            result = await self._run_workflow("cotality_search_property", variables, job_id=job_id)
            proposed = {
                key: value
                for key, value in result["proposed_updates_json"].items()
                if key in (job.get("requested_fields") or [])
            }
            if not proposed:
                screenshot = await self.capture_screenshot(f"job_{job_id}_empty")
                await self.post_result(
                    job_id,
                    {
                        "matched_address": result.get("matched_address"),
                        "raw_payload_json": result["raw_payload_json"],
                        "proposed_updates_json": {},
                        "confidence": 0.2,
                        "screenshot_path": screenshot,
                        "final_status": "failed",
                        "error_message": "No whitelisted Cotality fields could be extracted",
                    },
                )
                return
            await self.post_result(
                job_id,
                {
                    "matched_address": result.get("matched_address") or variables["full_address"],
                    "raw_payload_json": result["raw_payload_json"],
                    "proposed_updates_json": proposed,
                    "confidence": min(0.99, max(0.55, result.get("match_confidence") or 0.75)),
                    "screenshot_path": None,
                    "final_status": "completed",
                },
            )
        except WorkflowNotTaughtError as error:
            screenshot = await self.capture_screenshot(f"job_{job_id}_workflow_not_taught")
            await self.post_result(
                job_id,
                {
                    "matched_address": None,
                    "raw_payload_json": {"url": page.url, "reason": str(error)},
                    "proposed_updates_json": {},
                    "confidence": 0.0,
                    "screenshot_path": screenshot,
                    "final_status": "workflow_not_taught",
                    "error_message": str(error),
                },
            )
        except ReviewRequired as error:
            screenshot = await self.capture_screenshot(f"job_{job_id}_review_required")
            await self.post_result(
                job_id,
                {
                    "matched_address": None,
                    "raw_payload_json": {"url": page.url, "reason": str(error)},
                    "proposed_updates_json": {},
                    "confidence": 0.35,
                    "screenshot_path": screenshot,
                    "final_status": "review_required",
                    "error_message": str(error),
                },
            )
        except ReplayFailure as error:
            screenshot = await self.capture_screenshot(f"job_{job_id}_replay_failed")
            await self.post_result(
                job_id,
                {
                    "matched_address": None,
                    "raw_payload_json": {"url": page.url, "reason": str(error)},
                    "proposed_updates_json": {},
                    "confidence": 0.0,
                    "screenshot_path": screenshot,
                    "final_status": "replay_failed",
                    "error_message": str(error),
                },
            )

    async def run_forever(self) -> None:
        if not self.config.machine_token:
            raise RuntimeError("ENRICHMENT_MACHINE_TOKEN is required for runner mode")
        while True:
            await self.pace.enforce_hourly_limit()
            claimed = await self.claim_next_job()
            if not claimed:
                await self.pace.idle_pause()
                continue
            job_id = claimed["job"]["id"]
            try:
                await self.process_job(claimed)
                await self.pace.record_job_completion()
            except Exception as error:
                print(f"[runner] job {job_id} failed: {error}", flush=True)
                screenshot = await self.capture_screenshot(f"job_{job_id}_failed")
                await self.post_result(
                    job_id,
                    {
                        "matched_address": None,
                        "raw_payload_json": {"url": (await self.ensure_page()).url},
                        "proposed_updates_json": {},
                        "confidence": 0.0,
                        "screenshot_path": screenshot,
                        "final_status": "failed",
                        "error_message": str(error),
                    },
                )
            await self.pace.idle_pause("between jobs")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--teach", action="store_true", help="Legacy alias for --teach-workflow cotality_search_property")
    parser.add_argument("--validate", action="store_true", help="Alias for --validate-workflow cotality_search_property")
    parser.add_argument("--teach-workflow", help="Teach a workflow and save it under backend/scripts/workflows")
    parser.add_argument("--run-workflow", help="Replay a saved workflow once in the visible browser")
    parser.add_argument("--validate-workflow", help="Replay and validate a saved workflow")
    parser.add_argument("--list-workflows", action="store_true", help="List taught workflows")
    parser.add_argument("--preflight", action="store_true", help="Check backend, token, browser, profile, workflows, and base-page reachability")
    parser.add_argument("--example-address", help="Example address used for teaching or validation")
    args = parser.parse_args()

    if args.list_workflows:
        for name in list_workflows():
            print(name)
        return 0

    runner = CotalityRunner(RunnerConfig())
    await runner.start()
    try:
        if args.teach or args.teach_workflow:
            workflow_name = args.teach_workflow or "cotality_search_property"
            await runner.teach_workflow(workflow_name, args.example_address)
        elif args.validate:
            await runner.validate_workflow("cotality_search_property", args.example_address)
        elif args.preflight:
            await runner.preflight()
        elif args.run_workflow:
            await runner.run_workflow(args.run_workflow, args.example_address)
        elif args.validate_workflow:
            await runner.validate_workflow(args.validate_workflow, args.example_address)
        else:
            await runner.run_forever()
    finally:
        await runner.close()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
