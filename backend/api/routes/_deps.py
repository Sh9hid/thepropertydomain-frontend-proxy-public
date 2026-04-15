from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_session
from core.security import get_api_key


SessionDep = Annotated[AsyncSession, Depends(get_session)]
APIKeyDep = Annotated[str, Depends(get_api_key)]


__all__ = ["APIKeyDep", "SessionDep"]
