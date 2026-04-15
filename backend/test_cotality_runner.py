import base64
import sys
from pathlib import Path
import uuid

import pytest


SCRIPTS_DIR = Path(__file__).resolve().parent / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from cotality_runner import CotalityRunner, RunnerConfig  # noqa: E402
from workflow_replay import WorkflowReplayEngine  # noqa: E402
from workflow_teaching import WorkflowTeacher  # noqa: E402


class FakeSingleLocator:
    def __init__(self, *, visible=True, count=1, inner_text="", attr_type="text", attr_name="", placeholder=""):
        self.visible = visible
        self._count = count
        self._inner_text = inner_text
        self.attr_type = attr_type
        self.attr_name = attr_name
        self.placeholder = placeholder
        self.filled_values = []
        self.pressed_keys = []
        self.clicked = False

    @property
    def first(self):
        return self

    async def count(self):
        return self._count

    async def is_visible(self, timeout=None):
        return self.visible

    async def inner_text(self, timeout=None):
        return self._inner_text

    async def fill(self, value):
        self.filled_values.append(value)

    async def press(self, key):
        self.pressed_keys.append(key)

    async def click(self, timeout=None):
        self.clicked = True

    async def scroll_into_view_if_needed(self):
        return None

    async def wait_for(self, timeout=None):
        return None

    async def screenshot(self, path, full_page=True):
        Path(path).write_bytes(b"fake-png")


class FakeLocatorCollection:
    def __init__(self, locator=None):
        self.locator = locator

    @property
    def first(self):
        return self.locator or FakeSingleLocator(count=0, visible=False)

    async def count(self):
        return 0 if self.locator is None else await self.locator.count()

    async def inner_text(self, timeout=None):
        if self.locator is None:
            return ""
        return await self.locator.inner_text(timeout=timeout)


class FakeLocatorGroup:
    def __init__(self, locators=None):
        self.locators = list(locators or [])

    @property
    def first(self):
        return self.locators[0] if self.locators else FakeSingleLocator(count=0, visible=False)

    def nth(self, index):
        return self.locators[index]

    async def count(self):
        return len(self.locators)


class FakeKeyboard:
    def __init__(self):
        self.pressed = []

    async def press(self, key):
        self.pressed.append(key)


class FakeMouse:
    async def wheel(self, dx, dy):
        return None


class FakePage:
    def __init__(self, *, url="https://rpp.corelogic.com.au/login", title="Sign in", body_text="", selectors=None):
        self.url = url
        self._title = title
        self.body_text = body_text
        self.selectors = selectors or {}
        self.keyboard = FakeKeyboard()
        self.mouse = FakeMouse()

    async def title(self):
        return self._title

    def locator(self, selector):
        if selector == "body":
            return FakeLocatorCollection(FakeSingleLocator(inner_text=self.body_text))
        return FakeLocatorCollection(self.selectors.get(selector))

    def get_by_role(self, role, name=None):
        key = ("role", role, (name or "").lower())
        return FakeLocatorCollection(self.selectors.get(key))

    async def wait_for_load_state(self, state, timeout=None):
        return None

    async def screenshot(self, path, full_page=True):
        Path(path).write_bytes(b"fake-png")


class FakeChallengePage(FakePage):
    pass


class FakeTraversalLocator(FakeSingleLocator):
    def __init__(self, *, label="", visible=True, click_handler=None):
        super().__init__(visible=visible, count=1, inner_text=label)
        self._label = label
        self._click_handler = click_handler
        self.attributes = {}

    async def get_attribute(self, name):
        return self.attributes.get(name)

    async def click(self, timeout=None):
        self.clicked = True
        if self._click_handler:
            self._click_handler(self._label)


class FakeTraversalPage(FakePage):
    def __init__(self, *, tabs=None, tab_text_by_label=None, show_more=None, section_text=""):
        super().__init__(url="https://rpp.corelogic.com.au/property/10-example-street", title="Property intelligence", body_text=section_text)
        self._tabs = list(tabs or [])
        self._tab_text_by_label = dict(tab_text_by_label or {})
        self._show_more = list(show_more or [])
        self._section_text = section_text
        self.active_tab = None

    def _select_tab(self, label):
        self.active_tab = label
        self.body_text = self._tab_text_by_label.get(label, self._section_text)

    def locator(self, selector):
        if selector in {"[role='tab']", '[role="tab"]'}:
            return FakeLocatorGroup(self._tabs)
        if selector in {"button[aria-expanded='false']", 'button[aria-expanded="false"]', "summary", "button:has-text(\"Show more\")"}:
            return FakeLocatorGroup(self._show_more)
        if selector == "body":
            return FakeLocatorCollection(FakeSingleLocator(inner_text=self.body_text))
        return super().locator(selector)

    def get_by_role(self, role, name=None):
        if role == "tab" and name is not None:
            for locator in self._tabs:
                if locator._label.lower() == str(name).lower():
                    return FakeLocatorCollection(locator)
            return FakeLocatorCollection()
        if role == "button" and name is not None:
            for locator in self._show_more:
                if locator._label.lower() == str(name).lower():
                    return FakeLocatorCollection(locator)
            return FakeLocatorCollection()
        return super().get_by_role(role, name=name)


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.setenv("RPDATA_USERNAME", "agent@example.com")
    monkeypatch.setenv("RPDATA_PASSWORD", "secret-password")
    profile_dir = Path(r"D:\woonona-lead-machine") / f"cotality_runner_test_profiles_{uuid.uuid4().hex}"
    config = RunnerConfig(
        profile_dir=profile_dir,
        rpdata_username="agent@example.com",
        rpdata_password="secret-password",
    )
    instance = CotalityRunner(config)
    try:
        yield instance
    finally:
        if profile_dir.exists():
            for child in profile_dir.glob("**/*"):
                if child.is_file():
                    child.unlink(missing_ok=True)
            for child in sorted(profile_dir.glob("**/*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            profile_dir.rmdir()


@pytest.fixture
def replay_engine():
    async def _noop(*args, **kwargs):
        return None

    class DummyPace:
        async def pause(self, *args, **kwargs):
            return None

    page = FakeTraversalPage(
        tabs=[
            FakeTraversalLocator(label="Overview"),
            FakeTraversalLocator(label="Sales history"),
            FakeTraversalLocator(label="Valuation"),
        ],
        tab_text_by_label={
            "Overview": "Overview section text",
            "Sales history": "Sales history section text",
            "Valuation": "Valuation section text",
        },
        show_more=[
            FakeTraversalLocator(label="Show more", click_handler=lambda _label: None),
        ],
        section_text="Overview section text",
    )
    for tab in page._tabs:
        tab._click_handler = page._select_tab
    return WorkflowReplayEngine(
        page=page,
        pace=DummyPace(),
        detect_unusual_state=_noop,
        artifact_root=Path(r"D:\woonona-lead-machine") / f"cotality_runner_traversal_{uuid.uuid4().hex}",
    )


@pytest.mark.asyncio
async def test_is_login_page_detects_variant_from_title_and_password_field(runner, monkeypatch):
    page = FakePage(
        url="https://rpp.corelogic.com.au/authenticate",
        title="RP Data Sign In",
        body_text="Email address Password Sign in",
        selectors={"input[type=password]": FakeSingleLocator(attr_type="password")},
    )
    async def fake_search_locator(_page):
        return None

    monkeypatch.setattr(runner, "first_visible_search_locator", fake_search_locator)

    assert await runner.is_login_page(page) is True


@pytest.mark.asyncio
async def test_ensure_authenticated_skips_when_search_ready(runner, monkeypatch):
    page = FakePage(url="https://rpp.corelogic.com.au/search", title="RP Data")
    runner.page = page
    called = {"auto": 0, "manual": 0}

    async def fake_search_locator(_page):
        return FakeSingleLocator()

    async def fake_auto_login(_page):
        called["auto"] += 1
        return True

    async def fake_manual_login(job_id=None):
        called["manual"] += 1
        return True

    monkeypatch.setattr(runner, "first_visible_search_locator", fake_search_locator)
    monkeypatch.setattr(runner, "auto_login", fake_auto_login)
    monkeypatch.setattr(runner, "wait_for_manual_login", fake_manual_login)

    await runner.ensure_authenticated()

    assert called == {"auto": 0, "manual": 0}


@pytest.mark.asyncio
async def test_ensure_authenticated_auto_logs_in_from_login_page(runner, monkeypatch):
    username = FakeSingleLocator(attr_type="email", attr_name="username")
    password = FakeSingleLocator(attr_type="password")
    submit = FakeSingleLocator()
    page = FakePage(
        url="https://rpp.corelogic.com.au/login",
        title="Sign in",
        body_text="Username Password Sign in",
        selectors={
            "input[type=password]": password,
            "input[type=email]": username,
            ("role", "button", "sign in"): submit,
        },
    )
    runner.page = page

    async def fake_wait_for_settle(_page):
        return None

    async def fake_is_authenticated_search_page(_page):
        return False

    async def fake_wait_for_login_success(_page, timeout_seconds=20):
        return True

    monkeypatch.setattr(runner, "wait_for_settle", fake_wait_for_settle)
    monkeypatch.setattr(runner, "is_authenticated_search_page", fake_is_authenticated_search_page)
    monkeypatch.setattr(runner, "wait_for_login_success", fake_wait_for_login_success)

    await runner.ensure_authenticated()

    assert username.filled_values == ["agent@example.com"]
    assert password.filled_values == ["secret-password"]
    assert submit.clicked is True or password.pressed_keys == ["Enter"]


@pytest.mark.asyncio
async def test_ensure_authenticated_requires_manual_login_for_mfa(runner, monkeypatch, capsys):
    page = FakePage(
        url="https://rpp.corelogic.com.au/login",
        title="Verification required",
        body_text="Enter the code from your authenticator app",
        selectors={"input[type=password]": FakeSingleLocator(attr_type="password")},
    )
    runner.page = page
    called = {"manual": 0}

    async def fake_wait_for_manual_login(job_id=None):
        called["manual"] += 1
        return True

    async def fake_wait_for_settle(_page):
        return None

    monkeypatch.setattr(runner, "wait_for_manual_login", fake_wait_for_manual_login)
    monkeypatch.setattr(runner, "wait_for_settle", fake_wait_for_settle)

    await runner.ensure_authenticated()

    assert called["manual"] == 1
    assert "[runner] manual challenge required" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_capture_screenshot_writes_placeholder_on_login_page(runner, monkeypatch):
    artifact_root = Path(r"D:\woonona-lead-machine") / f"cotality_runner_artifacts_{uuid.uuid4().hex}"
    page = FakePage(
        url="https://rpp.corelogic.com.au/login",
        title="Sign in",
        body_text="Email Password Sign in",
        selectors={"input[type=password]": FakeSingleLocator(attr_type="password")},
    )
    runner.page = page
    monkeypatch.setattr("cotality_runner.ARTIFACT_ROOT", artifact_root)
    
    async def fake_search_locator(_page):
        return None

    monkeypatch.setattr(runner, "first_visible_search_locator", fake_search_locator)

    try:
        path = await runner.capture_screenshot("login_safe")

        assert Path(path).exists()
        assert Path(path).suffix == ".png"
        assert Path(path).read_bytes() == base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+i9o8AAAAASUVORK5CYII="
        )
    finally:
        if artifact_root.exists():
            for child in artifact_root.glob("**/*"):
                if child.is_file():
                    child.unlink(missing_ok=True)
            for child in sorted(artifact_root.glob("**/*"), reverse=True):
                if child.is_dir():
                    child.rmdir()
            artifact_root.rmdir()


@pytest.mark.asyncio
async def test_detect_login_variant_uses_username_handler_when_username_label_present(runner, monkeypatch):
    page = FakePage(
        url="https://rpp.corelogic.com.au/login",
        title="RP Data Sign In",
        body_text="Username Password Sign in",
        selectors={
            "input[type=password]": FakeSingleLocator(attr_type="password"),
            'input[name*="user" i]': FakeSingleLocator(attr_name="username"),
        },
    )

    async def fake_search_locator(_page):
        return None

    monkeypatch.setattr(runner, "first_visible_search_locator", fake_search_locator)

    assert await runner.detect_login_variant(page) == "username"


@pytest.mark.asyncio
async def test_auto_login_uses_variant_specific_username_selector(runner, monkeypatch):
    username = FakeSingleLocator(attr_name="username")
    password = FakeSingleLocator(attr_type="password")
    page = FakePage(
        url="https://rpp.corelogic.com.au/login",
        title="RP Data Sign In",
        body_text="Username Password Sign in",
        selectors={
            'input[name*="user" i]': username,
            "input[type=password]": password,
        },
    )

    async def fake_wait_for_settle(_page):
        return None

    async def fake_wait_for_login_success(_page, timeout_seconds=20):
        return True

    async def fake_search_locator(_page):
        return None

    monkeypatch.setattr(runner, "wait_for_settle", fake_wait_for_settle)
    monkeypatch.setattr(runner, "wait_for_login_success", fake_wait_for_login_success)
    monkeypatch.setattr(runner, "first_visible_search_locator", fake_search_locator)

    assert await runner.auto_login(page) is True
    assert username.filled_values == ["agent@example.com"]
    assert password.filled_values == ["secret-password"]


def test_compose_full_enrich_synthesizes_first_level_traversal_manifest(monkeypatch):
    captured = {}

    def fake_load_workflow(name):
        if name == "cotality_search_property":
            return {
                "entry_url_patterns": ["https://rpp.corelogic.com.au/"],
                "success_url_patterns": ["/property"],
                "steps": [{"type": "focus"}],
                "page_signatures": {"search": {}},
            }
        raise FileNotFoundError(name)

    def fake_save_workflow(name, manifest):
        captured["name"] = name
        captured["manifest"] = manifest
        return Path(r"D:\woonona-lead-machine\backend\scripts\workflows\cotality_full_enrich.json")

    monkeypatch.setattr("workflow_teaching.load_workflow", fake_load_workflow)
    monkeypatch.setattr("workflow_teaching.save_workflow", fake_save_workflow)
    monkeypatch.setattr("workflow_teaching.workflow_path", lambda name: Path(f"{name}.json"))

    teacher = WorkflowTeacher(page=None, artifact_root=Path("."), base_url="https://rpp.corelogic.com.au/")

    path = teacher.compose_full_enrich()

    assert path.name == "cotality_full_enrich.json"
    assert captured["name"] == "cotality_full_enrich"
    assert [step["type"] for step in captured["manifest"]["steps"][:4]] == [
        "focus",
        "collect_first_level_tabs",
        "visit_tab",
        "expand_sections",
    ]
    assert captured["manifest"]["steps"][-1]["type"] == "extract_sections"
    assert captured["manifest"]["workflow_refs"] == [
        "cotality_search_property.json",
        "cotality_extract_property_intelligence.json",
    ]


@pytest.mark.asyncio
async def test_replay_collects_tabs_and_section_payloads(replay_engine):
    manifest = {
        "workflow_name": "cotality_full_enrich",
        "steps": [
            {"type": "collect_first_level_tabs"},
            {"type": "visit_tab", "tab_key": "sales_history"},
            {"type": "expand_sections"},
            {"type": "extract_sections"},
        ],
    }

    result = await replay_engine.execute(manifest, {"full_address": "10 Example Street"})

    assert result.raw_payload["discovered_tabs"] == ["Overview", "Sales history", "Valuation"]
    assert result.raw_payload["section_order"] == ["sales_history"]
    assert result.raw_payload["sections"]["sales_history"]["text"] == "Sales history section text"
    assert result.raw_payload["expanded_sections"] == ["Show more"]
