# Domain: Services
## Purpose:
The "Intelligence Layer" and the "Brain" of the Lead Machine.
## Rules:
- **Domain-Specific:** Every service belongs to a sub-domain (Outreach, Intelligence, Ingestion, Agent).
- **Just-in-Time (JIT):** Every service must be "Trigger-Based," not "Static Scheduler." 
- **Error-Resilience:** All services must have a `try/except` block and log to the Activity History.
- **Human-Aware:** Services must "Request" time from the Principal, not command it.
- **Modular Interface:** A service in `outreach` should not know *how* `intelligence` calculates a score; it just asks for the score.
