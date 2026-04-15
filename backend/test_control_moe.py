import os

from services.control_moe import build_control_preview, parse_patch_payload, parse_planning_payload


def test_build_control_preview_for_code_mission_selects_named_experts(monkeypatch):
    monkeypatch.setenv("NVIDIA_API_KEY", "test-nvidia")

    preview = build_control_preview(
        command="Implement the real MOE runtime and generate a patch artifact.",
        objective="Ship the named-expert control stack.",
        target_type="portfolio",
        target_id=None,
        target_label="Oakville Portfolio",
        autonomy_mode="approve_sends_code",
        context={"scope": "portfolio", "portfolio": {"total_leads": 12, "open_leads": 5}, "scorecard": {"bookings_30d": 3}},
    )

    expert_names = [expert["name"] for expert in preview["experts"]]
    assert preview["downgrade_required"] is False
    assert preview["preview_hash"]
    # V0 roster for code missions: Assistant + Heads + Code Geniuses
    expected = ["Turing", "Hopper", "Ogilvy", "Ziglar", "Curie", "Bayes", "Cicero", "Linus", "Tesla", "Jobs"]
    for name in expected:
        assert name in expert_names


def test_build_control_preview_marks_downgrade_when_nvidia_unavailable(monkeypatch):
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)

    preview = build_control_preview(
        command="Implement a control-plane patch generator.",
        objective="Create a reviewable code artifact.",
        target_type="portfolio",
        target_id=None,
        target_label="Oakville Portfolio",
        autonomy_mode="approve_sends_code",
        context={"scope": "portfolio", "portfolio": {"total_leads": 12}, "scorecard": {"bookings_30d": 3}},
    )

    assert preview["downgrade_required"] is True
    assert any(request["expert_key"] == "turing" for request in preview["model_plan"]["downgrade_requests"])
    assert any("Approval is required to downgrade" in warning for warning in preview["warnings"])


def test_parse_planning_payload_normalizes_steps_and_experts():
    preview = {
        "experts": [
            {"expert_key": "turing", "name": "Turing", "role": "Mission architect", "department": "Executive", "provider_alias": "openai_planner_high", "model_alias": "openai_planner_high", "planned_model": "o4-mini", "fallback_chain": [], "purpose": "plan"},
            {"expert_key": "hopper", "name": "Hopper", "role": "Code builder", "department": "Engineering", "provider_alias": "openai_builder_medium", "model_alias": "openai_builder_medium", "planned_model": "gpt-4o-mini", "fallback_chain": [], "purpose": "build"},
        ],
        "model_plan": {"code_mission": True},
        "complexity": "high",
    }
    raw_output = """
    {
      "director_summary": "Turing will split planning from implementation.",
      "consensus_plan": "Hopper produces the patch, then Popper reviews it.",
      "expert_statuses": [
        {
          "expert_key": "turing",
          "summary": "Own the plan.",
          "findings": ["Need patch artifact", "Need verification"],
          "status": "active"
        }
      ],
      "recommended_steps": [
        {
          "title": "Generate patch artifact",
          "owner": "Hopper",
          "department": "Hopper",
          "reason": "Builder lane for code work.",
          "priority": "high",
          "approval_required": true
        }
      ]
    }
    """

    parsed = parse_planning_payload(raw_output, preview)

    assert parsed["director_summary"].startswith("Turing")
    assert parsed["recommended_steps"][0]["owner"] == "Hopper"
    assert parsed["recommended_steps"][0]["department"] == "Engineering"
    assert parsed["recommended_steps"][0]["id"]

    assert parsed["department_statuses"][0]["department"] == "Executive"


def test_parse_patch_payload_extracts_unified_diff():
    raw_output = """
    {
      "artifact_title": "Patch artifact",
      "summary": "Adds the preview endpoint.",
      "diff": "diff --git a/app.py b/app.py\\n--- a/app.py\\n+++ b/app.py\\n@@\\n+print('hi')\\n",
      "files": ["app.py"],
      "verification_steps": ["Run pytest"],
      "warnings": []
    }
    """

    parsed = parse_patch_payload(raw_output)

    assert parsed["artifact_title"] == "Patch artifact"
    assert parsed["files"] == ["app.py"]
    assert parsed["diff"].startswith("diff --git")
