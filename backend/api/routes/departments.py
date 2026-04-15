"""
Department management API endpoints.
"""

from typing import Optional

from fastapi import APIRouter

from api.routes._deps import APIKeyDep, SessionDep

router = APIRouter()


@router.get("/api/departments")
async def list_departments(workspace: Optional[str] = None, api_key: APIKeyDep = ""):
    """List all departments, optionally filtered by workspace."""
    from hermes.departments import get_workspace_departments, list_all_departments

    if workspace:
        return get_workspace_departments(workspace)
    return list_all_departments()


@router.post("/api/departments/{dept_id}/run")
async def run_department(dept_id: str, api_key: APIKeyDep, session: SessionDep):
    """Trigger a department cycle and return the result."""
    from hermes.department_runner import run_department_cycle

    return await run_department_cycle(dept_id, session)


@router.post("/api/departments/workspace/{workspace}/run-all")
async def run_workspace_departments(workspace: str, api_key: APIKeyDep, session: SessionDep):
    """Run all departments for a workspace."""
    from hermes.department_runner import run_all_workspace_departments

    results = await run_all_workspace_departments(workspace, session)
    return {"workspace": workspace, "results": results}


@router.get("/api/departments/findings")
async def get_department_findings(
    workspace: Optional[str] = None,
    limit: int = 20,
    api_key: APIKeyDep = "",
    session: SessionDep = None,
):
    """Get recent department cycle findings."""
    from sqlalchemy import text

    params: dict = {"limit": limit}
    where = "WHERE signal_type = 'department_cycle'"
    if workspace:
        where += " AND source_id LIKE :workspace_prefix"
        params["workspace_prefix"] = f"{workspace}.%"
    try:
        rows = (
            await session.execute(
                text(
                    f"""
            SELECT id, topic, summary, why_it_matters, confidence_score,
                   actionability_score, created_at, source_id, source_name
            FROM hermes_findings
            {where}
            ORDER BY created_at DESC
            LIMIT :limit
        """
                ),
                params,
            )
        ).mappings().all()
        return [dict(r) for r in rows]
    except Exception:
        return []
