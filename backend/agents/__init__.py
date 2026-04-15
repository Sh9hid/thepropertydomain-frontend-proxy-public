"""
Claude Agent SDK integration for HERMES multi-agent orchestration.

Tier 1 agents (7) run autonomous tool-use loops via the Anthropic API.
Tier 2 agents (23) remain as single-shot Hermes prompt templates via ai_router.

MCP tool servers wrap existing services:
  - mcp_lead_db: agent_tool_layer + hermes memory
  - mcp_rea: rea_listing_worker functions
  - mcp_comms: outreach/SMS/email services
"""
from __future__ import annotations

__all__ = [
    "TIER1_AGENT_IDS",
    "is_tier1_agent",
]

# Department IDs promoted to SDK agent loop
TIER1_AGENT_IDS: frozenset[str] = frozenset({
    "real_estate.lead_ops",
    "real_estate.content",      # Outreach Composer
    "real_estate.sales",        # Outreach Composer (merged)
    "real_estate.deal_tracker",
    "real_estate.research",     # Market Intel
    "real_estate.rea_listings", # REA ATLAS
    "shared.risk",              # Compliance Reviewer
})


def is_tier1_agent(dept_id: str) -> bool:
    """Check whether a department should run via the SDK agent loop."""
    return dept_id in TIER1_AGENT_IDS
