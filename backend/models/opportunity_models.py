"""Opportunity action persistence — tracks dismiss / snooze / complete per lead."""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlmodel import SQLModel, Field


class OpportunityAction(SQLModel, table=True):
    __tablename__ = "opportunity_actions"

    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    lead_id: str = Field(index=True)
    action: str  # dismiss | snooze | complete
    expires_at: Optional[str] = Field(default=None, index=True)  # ISO — snooze expiry, null = permanent
    detector_key: Optional[str] = Field(default=None)  # which detector triggered the action
    note: Optional[str] = Field(default=None)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
