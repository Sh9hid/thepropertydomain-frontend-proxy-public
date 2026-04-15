# Control Layer

## What This Is

The control layer is the repo's internal mission runtime for agent-style planning, debate, execution packets, and explicit operator approvals.

It is not a generic chatbot wrapper. It is a bookings-first control plane that:

- turns operator commands or system triggers into `Mission` records
- allocates a named expert roster
- persists an org run with agent nodes and heartbeats
- generates debated work items and review gates
- blocks silent model downgrades, silent code apply, and auto-send behavior

The live runtime loop is started from `backend/main.py` via `_control_runtime_loop()`.

## Current Org Chart

At runtime the org chart is persisted in `agent_nodes` and exposed through the control APIs.

```text
Operator
  |
  +-- Turing
      Mission architect / director
      Owns planning, synthesis, tie-breaks, and mission framing
      |
      +-- Shannon
      |   Scout decomposer
      |   Owns cheap triage, retrieval, and dependency mapping
      |
      +-- Hopper
      |   Code builder
      |   Owns patch generation and implementation packets
      |
      +-- Popper
      |   Adversarial reviewer
      |   Owns risk review, critique, and verification pressure
      |
      +-- Woolf
          Operator packet writer
          Owns summaries, approval packets, and human-readable control output
```

The expert roster is defined in `backend/services/control_moe.py`.

## Runtime Phases

Each mission becomes an `OrgRun` and moves through explicit phases:

1. `queued`
2. `routing`
3. `debating`
4. `executing`
5. `waiting_review`
6. `approved`
7. `completed`

There is also a guarded pause state:

- `awaiting_model_approval`

That state exists specifically to prevent silent provider/model substitution when a preferred alias is unavailable.

## Mission Lifecycle

1. Operator or trigger creates a mission through `POST /api/control/command`.
2. A context snapshot and preview hash are built.
3. Turing plans the mission and assigns the active expert roster.
4. Agent nodes are seeded into `agent_nodes`.
5. Debate output is persisted into `debate_sessions` and `debate_turns`.
6. Execution packets are broken into `work_items`.
7. Review boundaries are materialized as `review_gates`.
8. Operator approves the mission, downgrade, or individual work items.
9. Generated artifacts remain explicit and reviewable.
10. Patch artifacts can be applied only through the explicit artifact apply route.

## Guardrails

The control layer is deliberately constrained.

- It does not auto-send email.
- It does not auto-send SMS.
- It does not auto-apply code without an explicit artifact apply action.
- It does not silently downgrade models.
- It does not "resume" historical external Codex sessions unless those runs still exist in the local runtime state.

This means the system is autonomous in analysis, planning, and queue generation, but not in irreversible actions.

## What Is Stored

Core persisted objects:

- `Mission`
- `MissionRun`
- `OrgRun`
- `AgentNode`
- `AgentHeartbeat`
- `WorkItem`
- `DebateSession`
- `DebateTurn`
- `ReviewGate`
- `RunArtifact`
- `ExecutionAttempt`
- `ControlTrigger`
- `FactPack`
- `ImprovementCandidate`
- `LearningEvaluation`

These models live in `backend/models/control_models.py`.

The API payloads live in `backend/models/control_schemas.py`.

## Main Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/control/command/preview` | Preview roster, models, warnings, and cost |
| `POST` | `/api/control/command` | Create a mission |
| `GET` | `/api/control/live` | Get live runtime snapshot |
| `GET` | `/api/control/missions` | List recent missions |
| `GET` | `/api/control/missions/{mission_id}` | Get mission detail and org data |
| `POST` | `/api/control/missions/{mission_id}/approve` | Approve a queued mission |
| `POST` | `/api/control/missions/{mission_id}/restart` | Fork a fresh run from an older mission |
| `POST` | `/api/control/missions/{mission_id}/approve-downgrade` | Approve model fallback |
| `GET` | `/api/control/runs/{run_id}` | Get org-run detail |
| `GET` | `/api/control/work-items` | List reviewable work items |
| `POST` | `/api/control/work-items/{work_item_id}/approve` | Approve a work item |
| `POST` | `/api/control/work-items/{work_item_id}/reject` | Reject a work item |
| `POST` | `/api/control/artifacts/{artifact_id}/apply` | Apply a generated patch artifact |

## Frontend Surface

The operator-facing control surface is `frontend/src/views/ControlCenter.tsx`.

It now exposes:

- runtime health
- active and recent missions
- mission restart lineage
- model plan
- mission org chart from persisted `agent_nodes`
- lane ownership cards
- live activity timeline
- debate turns
- review queue
- generated artifacts

## What Is Real Vs Not Real

Real:

- persisted mission records
- persisted org runs
- persisted agent hierarchy
- mission restart as a new tracked run with lineage metadata
- persisted debate and work items
- background runtime loop
- explicit approval and artifact apply flow

Not yet fully real:

- self-improving closed-loop learning from outcomes
- automatic finishing of the whole product without operator prioritization
- cross-session resume of unknown historical agent processes that are no longer alive

## Practical Resume Rule

The repo contains prior AI memory, prompts, and operating notes in places like:

- `PROJECT_VISION_LOG.md`
- `.memory/`
- `.claude/`
- `.gemini/`

Those artifacts are enough to reconstruct intent and continue work. They are not the same thing as a live resumable runtime process.

If an old run still exists in the database, inspect it through the control APIs.
If it does not, create a new mission from the reconstructed context and continue from there.
