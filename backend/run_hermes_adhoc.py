import os
import sys
import asyncio
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

sys.path.append(r"D:\woonona-lead-machine\backend")
import dotenv
dotenv.load_dotenv(r"D:\woonona-lead-machine\backend\.env")

os.environ["HERMES_LLM_PROVIDER"] = "nvidia_nim"

from hermes.integrations.nvidia_nim import build_llm_provider

async def main():
    queries = ["realtor CRM Australia", "proptech AI", "real estate automation voice bot", "Australian property data scraper", "cold call AI agent"]
    repos = []
    headers = {"User-Agent": "HermesAgent/1.0", "Accept": "application/vnd.github.v3+json"}
    six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
    
    print("Searching GitHub...")
    for q in queries:
        url = f"https://api.github.com/search/repositories?q={urllib.parse.quote(q)}+pushed:>{six_months_ago}&sort=updated&order=desc"
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                repos.extend(data.get("items", [])[:5])
        except Exception as e:
            pass
            
    unique_repos = {r["full_name"]: r for r in repos}
    top_10 = list(unique_repos.values())[:10]
    
    if not top_10:
        print("No active repositories found matching those queries.")
        return

    repo_descriptions = []
    for r in top_10:
        repo_descriptions.append(f"Repo: {r['full_name']}\nDesc: {r['description']}\nURL: {r['html_url']}")
        
    context = "\n---\n".join(repo_descriptions)
    
    prompt = f'''Role: Act as a Senior AI Engineer & Technical Lead specializing in automating Realtor and Cold Caller Sales processes in Australia.

Task:
Analyze the top relevant projects provided below.
For each, analyze the repository description.
Identify unique, high-value, actionable features or AI capabilities that can be integrated into our platform.

Output Format: A Markdown list of unique project ideas (no duplicates). For each idea, provide:
Idea: [Concise Name]
Source: [Repo Name]
Value: [How it improves AUS realtor efficiency]
Technical Approach: [How to build it]
Constraint: Focus on innovations that improve lead conversion or automated outreach in the Australian market.

Data context:
{context}
'''
    
    provider = build_llm_provider()
    print("Invoking NVIDIA NIM...")
    try:
        res = await provider._chat(
            model="meta/llama3-70b-instruct", 
            system="You are Hermes, the senior AI engineer for Woonona Lead Machine.",
            user=prompt,
            temperature=0.2,
            max_tokens=2048
        )
        print("\n=== HERMES OUTPUT ===")
        print(res)
        
        print("\n=== SENDING WHATSAPP VIA PYWHATKIT ===")
        import pywhatkit
        import time
        # Send instantly, wait 20 secs for browser to open, then close tab after 3 secs
        pywhatkit.sendwhatmsg_instantly("+61485857881", res, 20, True, 3)
        print("WhatsApp Sent!")
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
