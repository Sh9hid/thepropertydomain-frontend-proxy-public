import os
import glob
import asyncio
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field
from openai import AsyncOpenAI

from core.config import NVIDIA_API_KEY, PROJECT_ROOT, STOCK_ROOT

class QwenResponse(BaseModel):
    answer: str = Field(..., description="The ultra-intelligent, data-driven answer from Qwen")
    confidence: float = Field(default=1.0, description="Confidence score from 0.0 to 1.0")
    sources: List[str] = Field(default_factory=list, description="Project files or data sources referenced")

# Initialize NVIDIA NIM Client
nv_client = AsyncOpenAI(
    api_key=NVIDIA_API_KEY,
    base_url="https://integrate.api.nvidia.com/v1"
)

def load_project_knowledge() -> str:
    """
    Reads all .md files from PROJECT_ROOT and Obsidian_Expertise 
    to 'teach' Qwen the project context.
    """
    knowledge_blocks = []
    
    # 1. Project Root Docs
    for md_file in glob.glob(str(PROJECT_ROOT / "*.md")):
        try:
            content = Path(md_file).read_text(encoding='utf-8', errors='ignore')
            knowledge_blocks.append(f"--- FILE: {Path(md_file).name} ---\n{content}")
        except Exception:
            pass
            
    # 2. Obsidian Expertise
    obsidian_path = PROJECT_ROOT / "Obsidian_Expertise"
    if obsidian_path.exists():
        for md_file in glob.glob(str(obsidian_path / "*.md")):
            try:
                content = Path(md_file).read_text(encoding='utf-8', errors='ignore')
                knowledge_blocks.append(f"--- EXPERTISE: {Path(md_file).name} ---\n{content}")
            except Exception:
                pass
                
    return "\n\n".join(knowledge_blocks)

async def ask_qwen(query: str) -> QwenResponse:
    """
    Primary interface to talk to Qwen (NVIDIA NIM 397B).
    """
    knowledge = load_project_knowledge()
    
    system_prompt = (
        "You are 'QWEN', the ultra-intelligence core of the Woonona Lead Machine. "
        "Your engine is the Qwen 3.5 (397B) model running on NVIDIA NIM. "
        "You have been taught the entire project context provided below. "
        "Your mission is to provide surgical, data-backed advice to the operator (Shahid). "
        "Avoid AI slop. Be direct, professional, and slightly aggressive in pursuit of market dominance.\n\n"
        f"--- PROJECT CONTEXT ---\n{knowledge}"
    )

    try:
        response = await nv_client.chat.completions.create(
            model="qwen/qwen3.5-397b-a17b",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ],
            temperature=0.2,
            max_tokens=1024
        )
        
        answer = response.choices[0].message.content
        return QwenResponse(
            answer=answer, 
            sources=["Project Documentation", "Obsidian Expertise"]
        )
    except Exception as e:
        print(f"Direct Qwen call failed: {e}")
        return QwenResponse(
            answer=f"Qwen encountered an internal logic error: {str(e)}",
            confidence=0.0
        )

if __name__ == "__main__":
    async def test():
        if not NVIDIA_API_KEY:
            print("NVIDIA_API_KEY not set")
            return
        resp = await ask_qwen("Summarize our 'Closed Loop' strategy.")
        print(f"QWEN: {resp.answer}")
    asyncio.run(test())
