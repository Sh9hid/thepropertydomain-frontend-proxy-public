from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext
from typing import Optional
import os

# Define the lead schema for the agent
class LeadScore(BaseModel):
    score: int = Field(..., description="Lead score from 0-100")
    reason: str = Field(..., description="Short explanation of the score")
    priority: str = Field(..., description="High, Medium, Low")
    outreach_strategy: str = Field(..., description="Suggested next action")

# Create the Agent (2026 standard for agentic workflows)
# Using 'openai' or 'anthropic' as a placeholder; Pydantic AI handles model swapping easily.
lead_scoring_agent = Agent(
    'openai:gpt-4o',
    result_type=LeadScore,
    system_prompt=(
        "You are an expert real estate lead analyst for Laing+Simmons Oakville | Windsor. "
        "Analyze the property lead data and determine the quality of the lead. "
        "The principal is Nitin Puri. Focus on high-yield appraisals and developer potential."
    ),
)

@lead_scoring_agent.tool
async def get_market_trends(ctx: RunContext[None], suburb: str) -> str:
    """Gets the latest local market trends for a given suburb."""
    # In a real scenario, this would call our DuckDB intel_engine
    return f"The {suburb} market is currently seeing strong developer interest with 12% YoY growth."

async def analyze_lead(lead_data: dict) -> LeadScore:
    """
    Asynchronously runs the agent to score a lead.
    """
    result = await lead_scoring_agent.run(f"Score this lead: {lead_data}")
    return result.data
