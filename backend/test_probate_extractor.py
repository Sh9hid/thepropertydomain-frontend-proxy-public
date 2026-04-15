import pytest

from services import probate_scraper


@pytest.mark.asyncio
async def test_extract_probate_notice_fields_prefers_llm(monkeypatch):
    notice = {
        "title": "Estate of John Citizen",
        "content": "Executor Mary Citizen. Property at 12 Example Street Windsor NSW 2756. Lot 7 DP 123456.",
    }

    async def fake_call(prompt, schema, system="", **kwargs):
        assert schema["type"] == "object"
        assert kwargs["model"] == probate_scraper.PROBATE_EXTRACT_MODEL
        return {
            "deceased_name": "John Citizen",
            "suburb": "Windsor",
            "postcode": "2756",
            "executor": "Mary Citizen",
            "property_refs": ["Lot 7 DP 123456"],
            "address": "12 Example Street Windsor NSW 2756",
            "confidence": 91,
        }

    monkeypatch.setattr(probate_scraper, "_call_gemini_json", fake_call)
    parsed = await probate_scraper.extract_probate_notice_fields(notice)

    assert parsed.deceased_name == "John Citizen"
    assert parsed.suburb == "Windsor"
    assert parsed.postcode == "2756"
    assert parsed.executor == "Mary Citizen"
    assert parsed.property_refs == ["Lot 7 DP 123456"]
    assert parsed.address == "12 Example Street Windsor NSW 2756"
    assert parsed.confidence == 91


@pytest.mark.asyncio
async def test_extract_probate_notice_fields_merges_llm_with_heuristics(monkeypatch):
    notice = {
        "title": "Estate of Sarah Citizen",
        "content": (
            "Probate notice for Sarah Citizen. Executor Michael Citizen. "
            "Property at 8 River Road South Windsor NSW 2756."
        ),
    }

    async def fake_call(*args, **kwargs):
        return {
            "deceased_name": "Sarah Citizen",
            "suburb": "",
            "postcode": "",
            "executor": "",
            "property_refs": [],
            "address": "",
            "confidence": 42,
        }

    monkeypatch.setattr(probate_scraper, "_call_gemini_json", fake_call)
    parsed = await probate_scraper.extract_probate_notice_fields(notice)

    assert parsed.deceased_name == "Sarah Citizen"
    assert parsed.postcode == "2756"
    assert parsed.suburb == "South Windsor"
    assert parsed.executor == "Michael Citizen"
    assert parsed.address.startswith("8 River Road")
    assert parsed.confidence >= 68


@pytest.mark.asyncio
async def test_extract_probate_notice_fields_falls_back_when_llm_unavailable(monkeypatch):
    notice = {
        "title": "Estate of Peter Example",
        "content": (
            "Estate of Peter Example. Administrator Jane Example. "
            "Property at 3 Garden Avenue Thirroul NSW 2515. DP 456789."
        ),
    }

    async def fake_call(*args, **kwargs):
        return None

    monkeypatch.setattr(probate_scraper, "_call_gemini_json", fake_call)
    parsed = await probate_scraper.extract_probate_notice_fields(notice)

    assert parsed.deceased_name == "Estate of Peter Example"
    assert parsed.executor == "Jane Example"
    assert parsed.property_refs == ["DP 456789"]
    assert parsed.address.startswith("3 Garden Avenue")
    assert parsed.postcode == "2515"
