import json
import base64
from pathlib import Path
from typing import Any

from playwright.async_api import Page


WORKFLOW_ROOT = Path(__file__).resolve().parent / "workflows"
REDACTED = "[REDACTED]"
SAFE_PLACEHOLDER_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+i9o8AAAAASUVORK5CYII="
)
TRACE_SCRIPT = r"""
() => {
  if (window.__cotalityWorkflowTraceInstalled) {
    window.__cotalityWorkflowTrace = [];
    return;
  }

  window.__cotalityWorkflowTraceInstalled = true;
  window.__cotalityWorkflowTrace = [];

  function shortText(value, limit) {
    return String(value || "").replace(/\s+/g, " ").trim().slice(0, limit);
  }

  function cssPath(element) {
    if (!element || !(element instanceof Element)) return "";
    if (element.id) return `#${CSS.escape(element.id)}`;
    const parts = [];
    let node = element;
    while (node && node.nodeType === Node.ELEMENT_NODE && parts.length < 8) {
      let selector = node.tagName.toLowerCase();
      if (node.classList && node.classList.length) {
        selector += "." + Array.from(node.classList)
          .slice(0, 2)
          .map((item) => CSS.escape(item))
          .join(".");
      }
      const parent = node.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children).filter((child) => child.tagName === node.tagName);
        if (siblings.length > 1) {
          selector += `:nth-of-type(${siblings.indexOf(node) + 1})`;
        }
      }
      parts.unshift(selector);
      node = parent;
    }
    return parts.join(" > ");
  }

  function xpathFor(element) {
    if (!element || !(element instanceof Element)) return "";
    const parts = [];
    let node = element;
    while (node && node.nodeType === Node.ELEMENT_NODE) {
      let index = 1;
      let sibling = node.previousElementSibling;
      while (sibling) {
        if (sibling.tagName === node.tagName) index += 1;
        sibling = sibling.previousElementSibling;
      }
      parts.unshift(`${node.tagName.toLowerCase()}[${index}]`);
      node = node.parentElement;
    }
    return "/" + parts.join("/");
  }

  function nearbyText(element) {
    const texts = [];
    const candidates = [
      element,
      element?.parentElement,
      element?.closest("label"),
      element?.previousElementSibling,
      element?.nextElementSibling,
      element?.closest("[role='row']"),
      element?.closest("[role='listitem']"),
      element?.closest("li"),
      element?.closest("tr"),
      element?.closest("section"),
      element?.closest("article"),
      element?.closest("form"),
    ];
    for (const candidate of candidates) {
      if (!candidate) continue;
      const value = shortText(candidate.innerText || candidate.textContent || "", 240);
      if (value && !texts.includes(value)) {
        texts.push(value);
      }
      if (texts.length >= 4) break;
    }
    return texts;
  }

  function sensitiveText(value) {
    return String(value || "").toLowerCase();
  }

  function isSensitiveElement(element) {
    if (!element || !(element instanceof Element)) return false;
    const type = sensitiveText(element.getAttribute("type"));
    const name = sensitiveText(element.getAttribute("name"));
    const id = sensitiveText(element.id);
    const ariaLabel = sensitiveText(element.getAttribute("aria-label"));
    const placeholder = sensitiveText(element.getAttribute("placeholder"));
    const autocomplete = sensitiveText(element.getAttribute("autocomplete"));
    const fields = [type, name, id, ariaLabel, placeholder, autocomplete];
    if (type === "password") return true;
    return fields.some((field) => field.includes("password") || field.includes("passcode") || field.includes("passphrase"));
  }

  function elementSnapshot(element) {
    if (!element || !(element instanceof Element)) return null;
    const rect = element.getBoundingClientRect();
    const role = element.getAttribute("role") || "";
    const sensitive = isSensitiveElement(element);
    return {
      tag: element.tagName.toLowerCase(),
      id: element.id || "",
      name: element.getAttribute("name") || "",
      ariaLabel: element.getAttribute("aria-label") || "",
      placeholder: element.getAttribute("placeholder") || "",
      type: element.getAttribute("type") || "",
      role,
      text: shortText(element.innerText || element.textContent || "", 180),
      value: sensitive ? "[REDACTED]" : ("value" in element ? shortText(element.value, 240) : ""),
      cssPath: cssPath(element),
      xpath: xpathFor(element),
      nearbyText: nearbyText(element),
      rect: {
        x: rect.x,
        y: rect.y,
        width: rect.width,
        height: rect.height
      }
    };
  }

  function pushEvent(type, element, extra) {
    window.__cotalityWorkflowTrace.push({
      type,
      at: new Date().toISOString(),
      url: location.href,
      title: document.title,
      element: elementSnapshot(element),
      extra: extra || {}
    });
  }

  document.addEventListener("focusin", (event) => {
    const element = event.target instanceof Element ? event.target.closest("*") : null;
    if (element) pushEvent("focus", element);
  }, true);

  document.addEventListener("click", (event) => {
    const element = event.target instanceof Element ? event.target.closest("*") : null;
    if (element) pushEvent("click", element);
  }, true);

  document.addEventListener("input", (event) => {
    const element = event.target instanceof Element ? event.target.closest("*") : null;
    if (element) {
      pushEvent("input", element, {
        value: isSensitiveElement(element) ? "[REDACTED]" : ("value" in element ? shortText(element.value, 300) : "")
      });
    }
  }, true);

  document.addEventListener("keydown", (event) => {
    const element = event.target instanceof Element ? event.target.closest("*") : null;
    pushEvent("keydown", element, {
      key: event.key,
      code: event.code
    });
  }, true);

  document.addEventListener("submit", (event) => {
    const element = event.target instanceof Element ? event.target.closest("*") : null;
    pushEvent("submit", element);
  }, true);
}
"""


async def install_trace(page: Page) -> None:
    await page.add_init_script(TRACE_SCRIPT)
    await page.evaluate(TRACE_SCRIPT)


async def clear_trace(page: Page) -> None:
    await page.evaluate("window.__cotalityWorkflowTrace = []")


async def get_trace(page: Page) -> list[dict[str, Any]]:
    trace = await page.evaluate("window.__cotalityWorkflowTrace || []")
    return [sanitize_trace_event(event) for event in trace]


async def get_interactive_search_candidates(page: Page) -> list[dict[str, Any]]:
    candidates = await page.evaluate(
        """
        () => {
          function shortText(value, limit) {
            return String(value || "").replace(/\\s+/g, " ").trim().slice(0, limit);
          }
          function cssPath(element) {
            if (!element || !(element instanceof Element)) return "";
            if (element.id) return `#${CSS.escape(element.id)}`;
            const parts = [];
            let node = element;
            while (node && node.nodeType === Node.ELEMENT_NODE && parts.length < 8) {
              let selector = node.tagName.toLowerCase();
              if (node.classList && node.classList.length) {
                selector += "." + Array.from(node.classList).slice(0, 2).map((item) => CSS.escape(item)).join(".");
              }
              const parent = node.parentElement;
              if (parent) {
                const siblings = Array.from(parent.children).filter((child) => child.tagName === node.tagName);
                if (siblings.length > 1) {
                  selector += `:nth-of-type(${siblings.indexOf(node) + 1})`;
                }
              }
              parts.unshift(selector);
              node = parent;
            }
            return parts.join(" > ");
          }
          function xpathFor(element) {
            if (!element || !(element instanceof Element)) return "";
            const parts = [];
            let node = element;
            while (node && node.nodeType === Node.ELEMENT_NODE) {
              let index = 1;
              let sibling = node.previousElementSibling;
              while (sibling) {
                if (sibling.tagName === node.tagName) index += 1;
                sibling = sibling.previousElementSibling;
              }
              parts.unshift(`${node.tagName.toLowerCase()}[${index}]`);
              node = node.parentElement;
            }
            return "/" + parts.join("/");
          }
          function visible(element) {
            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return rect.width > 0 && rect.height > 0 && style.visibility !== "hidden" && style.display !== "none";
          }
          function nearbyText(element) {
            const texts = [];
            const candidates = [
              element,
              element?.parentElement,
              element?.closest("label"),
              element?.previousElementSibling,
              element?.nextElementSibling,
              element?.closest("[role='search']"),
              element?.closest("form"),
              element?.closest("section"),
            ];
            for (const candidate of candidates) {
              if (!candidate) continue;
              const value = shortText(candidate.innerText || candidate.textContent || "", 240);
              if (value && !texts.includes(value)) texts.push(value);
              if (texts.length >= 4) break;
            }
            return texts;
          }
          function sensitiveText(value) {
            return String(value || "").toLowerCase();
          }
          function isSensitiveElement(element) {
            if (!element || !(element instanceof Element)) return false;
            const type = sensitiveText(element.getAttribute("type"));
            const name = sensitiveText(element.getAttribute("name"));
            const id = sensitiveText(element.id);
            const ariaLabel = sensitiveText(element.getAttribute("aria-label"));
            const placeholder = sensitiveText(element.getAttribute("placeholder"));
            const autocomplete = sensitiveText(element.getAttribute("autocomplete"));
            const fields = [type, name, id, ariaLabel, placeholder, autocomplete];
            if (type === "password") return true;
            return fields.some((field) => field.includes("password") || field.includes("passcode") || field.includes("passphrase"));
          }
          const selector = [
            'input',
            'textarea',
            '[role="combobox"]',
            '[role="searchbox"]',
            '[contenteditable="true"]'
          ].join(',');
          return Array.from(document.querySelectorAll(selector))
            .filter((element) => visible(element))
            .filter((element) => !isSensitiveElement(element))
            .map((element) => ({
              tag: element.tagName.toLowerCase(),
              id: element.id || "",
              name: element.getAttribute("name") || "",
              ariaLabel: element.getAttribute("aria-label") || "",
              placeholder: element.getAttribute("placeholder") || "",
              role: element.getAttribute("role") || "",
              text: shortText(element.innerText || element.textContent || "", 180),
              value: "value" in element ? shortText(element.value, 240) : "",
              cssPath: cssPath(element),
              xpath: xpathFor(element),
              nearbyText: nearbyText(element),
              url: location.href,
              title: document.title,
            }));
        }
        """
    )
    sanitized: list[dict[str, Any]] = []
    for candidate in candidates:
        safe_candidate = sanitize_dom_candidate(candidate)
        if safe_candidate is not None:
            sanitized.append(safe_candidate)
    return sanitized


async def get_interactive_tab_candidates(page: Page) -> list[dict[str, Any]]:
    candidates = await page.evaluate(
        """
        () => {
          function shortText(value, limit) {
            return String(value || "").replace(/\\s+/g, " ").trim().slice(0, limit);
          }
          function cssPath(element) {
            if (!element || !(element instanceof Element)) return "";
            if (element.id) return `#${CSS.escape(element.id)}`;
            const parts = [];
            let node = element;
            while (node && node.nodeType === Node.ELEMENT_NODE && parts.length < 8) {
              let selector = node.tagName.toLowerCase();
              if (node.classList && node.classList.length) {
                selector += "." + Array.from(node.classList).slice(0, 2).map((item) => CSS.escape(item)).join(".");
              }
              const parent = node.parentElement;
              if (parent) {
                const siblings = Array.from(parent.children).filter((child) => child.tagName === node.tagName);
                if (siblings.length > 1) {
                  selector += `:nth-of-type(${siblings.indexOf(node) + 1})`;
                }
              }
              parts.unshift(selector);
              node = parent;
            }
            return parts.join(" > ");
          }
          function xpathFor(element) {
            if (!element || !(element instanceof Element)) return "";
            const parts = [];
            let node = element;
            while (node && node.nodeType === Node.ELEMENT_NODE) {
              let index = 1;
              let sibling = node.previousElementSibling;
              while (sibling) {
                if (sibling.tagName === node.tagName) index += 1;
                sibling = sibling.previousElementSibling;
              }
              parts.unshift(`${node.tagName.toLowerCase()}[${index}]`);
              node = node.parentElement;
            }
            return "/" + parts.join("/");
          }
          function visible(element) {
            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return rect.width > 0 && rect.height > 0 && style.visibility !== "hidden" && style.display !== "none";
          }
          const selector = [
            '[role="tab"]',
            'button[role="tab"]',
            '[aria-controls]',
          ].join(',');
          return Array.from(document.querySelectorAll(selector))
            .filter((element) => visible(element))
            .map((element) => ({
              tag: element.tagName.toLowerCase(),
              id: element.id || "",
              name: element.getAttribute("name") || "",
              ariaLabel: element.getAttribute("aria-label") || "",
              ariaControls: element.getAttribute("aria-controls") || "",
              ariaSelected: element.getAttribute("aria-selected") || "",
              placeholder: element.getAttribute("placeholder") || "",
              role: element.getAttribute("role") || "",
              text: shortText(element.innerText || element.textContent || "", 180),
              value: "value" in element ? shortText(element.value, 240) : "",
              cssPath: cssPath(element),
              xpath: xpathFor(element),
              nearbyText: [],
              url: location.href,
              title: document.title,
            }));
        }
        """
    )
    sanitized: list[dict[str, Any]] = []
    for candidate in candidates:
        safe_candidate = sanitize_dom_candidate(candidate)
        if safe_candidate is not None:
            sanitized.append(safe_candidate)
    return sanitized


async def get_expandable_section_candidates(page: Page) -> list[dict[str, Any]]:
    candidates = await page.evaluate(
        """
        () => {
          function shortText(value, limit) {
            return String(value || "").replace(/\\s+/g, " ").trim().slice(0, limit);
          }
          function cssPath(element) {
            if (!element || !(element instanceof Element)) return "";
            if (element.id) return `#${CSS.escape(element.id)}`;
            const parts = [];
            let node = element;
            while (node && node.nodeType === Node.ELEMENT_NODE && parts.length < 8) {
              let selector = node.tagName.toLowerCase();
              if (node.classList && node.classList.length) {
                selector += "." + Array.from(node.classList).slice(0, 2).map((item) => CSS.escape(item)).join(".");
              }
              const parent = node.parentElement;
              if (parent) {
                const siblings = Array.from(parent.children).filter((child) => child.tagName === node.tagName);
                if (siblings.length > 1) {
                  selector += `:nth-of-type(${siblings.indexOf(node) + 1})`;
                }
              }
              parts.unshift(selector);
              node = parent;
            }
            return parts.join(" > ");
          }
          function xpathFor(element) {
            if (!element || !(element instanceof Element)) return "";
            const parts = [];
            let node = element;
            while (node && node.nodeType === Node.ELEMENT_NODE) {
              let index = 1;
              let sibling = node.previousElementSibling;
              while (sibling) {
                if (sibling.tagName === node.tagName) index += 1;
                sibling = sibling.previousElementSibling;
              }
              parts.unshift(`${node.tagName.toLowerCase()}[${index}]`);
              node = node.parentElement;
            }
            return "/" + parts.join("/");
          }
          function visible(element) {
            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return rect.width > 0 && rect.height > 0 && style.visibility !== "hidden" && style.display !== "none";
          }
          const selector = [
            'button[aria-expanded="false"]',
            'button[aria-expanded="true"]',
            'summary',
            'button:has-text("Show more")',
            'button:has-text("More")',
            'button:has-text("Expand")',
            'button:has-text("View more")',
          ].join(',');
          return Array.from(document.querySelectorAll(selector))
            .filter((element) => visible(element))
            .map((element) => ({
              tag: element.tagName.toLowerCase(),
              id: element.id || "",
              name: element.getAttribute("name") || "",
              ariaLabel: element.getAttribute("aria-label") || "",
              ariaExpanded: element.getAttribute("aria-expanded") || "",
              placeholder: element.getAttribute("placeholder") || "",
              role: element.getAttribute("role") || "",
              text: shortText(element.innerText || element.textContent || "", 180),
              value: "value" in element ? shortText(element.value, 240) : "",
              cssPath: cssPath(element),
              xpath: xpathFor(element),
              nearbyText: [],
              url: location.href,
              title: document.title,
            }));
        }
        """
    )
    sanitized: list[dict[str, Any]] = []
    for candidate in candidates:
        safe_candidate = sanitize_dom_candidate(candidate)
        if safe_candidate is not None:
            sanitized.append(safe_candidate)
    return sanitized


def _contains_sensitive_hint(*values: Any) -> bool:
    return any(
        token in str(value or "").lower()
        for value in values
        for token in ("password", "passcode", "passphrase", "current-password", "new-password")
    )


def is_sensitive_snapshot(snapshot: dict[str, Any] | None) -> bool:
    if not snapshot:
        return False
    return _contains_sensitive_hint(
        snapshot.get("type"),
        snapshot.get("name"),
        snapshot.get("id"),
        snapshot.get("ariaLabel"),
        snapshot.get("placeholder"),
        snapshot.get("autocomplete"),
    )


def sanitize_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not snapshot:
        return snapshot
    clean = dict(snapshot)
    if is_sensitive_snapshot(snapshot):
        clean["value"] = REDACTED
    return clean


def sanitize_trace_event(event: dict[str, Any]) -> dict[str, Any]:
    clean = dict(event)
    clean["element"] = sanitize_snapshot(event.get("element"))
    extra = dict(event.get("extra") or {})
    if is_sensitive_snapshot(event.get("element")) and "value" in extra:
        extra["value"] = REDACTED
    clean["extra"] = extra
    return clean


def sanitize_dom_candidate(candidate: dict[str, Any]) -> dict[str, Any] | None:
    if is_sensitive_snapshot(candidate):
        return None
    clean = dict(candidate)
    if "value" in clean:
        clean["value"] = ""
    return clean


async def page_contains_sensitive_input(page: Page) -> bool:
    try:
        return bool(
            await page.evaluate(
                """
                () => Array.from(document.querySelectorAll('input, textarea')).some((element) => {
                  const read = (value) => String(value || '').toLowerCase();
                  const fields = [
                    read(element.getAttribute('type')),
                    read(element.getAttribute('name')),
                    read(element.id),
                    read(element.getAttribute('aria-label')),
                    read(element.getAttribute('placeholder')),
                    read(element.getAttribute('autocomplete')),
                  ];
                  if (fields[0] === 'password') return true;
                  return fields.some((field) => field.includes('password') || field.includes('passcode') || field.includes('passphrase'));
                })
                """
            )
        )
    except Exception:
        return False


async def capture_safe_screenshot(page: Page, path: Path) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if await page_contains_sensitive_input(page):
        write_placeholder_screenshot(path)
        return False
    await page.screenshot(path=str(path), full_page=True)
    return True


def write_placeholder_screenshot(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(SAFE_PLACEHOLDER_PNG)


def dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
      key = (str(candidate.get("type") or ""), str(candidate.get("value") or ""))
      if not key[0] or not key[1] or key in seen:
          continue
      seen.add(key)
      deduped.append(candidate)
    return deduped


def build_selector_candidates(snapshot: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not snapshot:
        return []
    candidates: list[dict[str, Any]] = []
    tag = str(snapshot.get("tag") or "").strip()
    role = str(snapshot.get("role") or "").strip()
    if snapshot.get("id"):
        candidates.append({"type": "css", "value": f"#{snapshot['id']}", "weight": 1.0})
    if snapshot.get("name"):
        value = str(snapshot["name"]).replace('"', '\\"')
        candidates.append({"type": "css", "value": f'[name="{value}"]', "weight": 0.95})
    if snapshot.get("ariaLabel"):
        aria = str(snapshot["ariaLabel"]).replace('"', '\\"')
        candidates.append({"type": "css", "value": f'[aria-label="{aria}"]', "weight": 0.92})
        candidates.append({"type": "label", "value": snapshot["ariaLabel"], "weight": 0.88})
    if snapshot.get("placeholder"):
        placeholder = str(snapshot["placeholder"]).replace('"', '\\"')
        candidates.append({"type": "css", "value": f'[placeholder="{placeholder}"]', "weight": 0.9})
        candidates.append({"type": "placeholder", "value": snapshot["placeholder"], "weight": 0.86})
    if snapshot.get("text"):
        candidates.append({"type": "text", "value": snapshot["text"], "weight": 0.8})
    if role:
        candidates.append({"type": "role", "value": role, "name": snapshot.get("ariaLabel") or snapshot.get("text") or "", "weight": 0.78})
    if tag:
        candidates.append({"type": "tag", "value": tag, "weight": 0.3})
    if snapshot.get("cssPath"):
        candidates.append({"type": "css", "value": snapshot["cssPath"], "weight": 0.45})
    if snapshot.get("xpath"):
        candidates.append({"type": "xpath", "value": snapshot["xpath"], "weight": 0.25})
    for text in snapshot.get("nearbyText") or []:
        candidates.append({"type": "nearby_text", "value": text, "weight": 0.5})
    return dedupe_candidates(candidates)


def ensure_workflow_root() -> Path:
    WORKFLOW_ROOT.mkdir(parents=True, exist_ok=True)
    return WORKFLOW_ROOT


def workflow_path(name: str) -> Path:
    return ensure_workflow_root() / f"{name}.json"


def save_workflow(name: str, manifest: dict[str, Any]) -> Path:
    path = workflow_path(name)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return path


def load_workflow(name: str) -> dict[str, Any]:
    path = workflow_path(name)
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def list_workflows() -> list[str]:
    root = ensure_workflow_root()
    return sorted(path.stem for path in root.glob("*.json"))
