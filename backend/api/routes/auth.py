from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from core.admin_auth import (
    admin_login_ready,
    authenticate_user,
    build_session_token,
    session_cookie_settings,
)
from core.security import get_authenticated_user


router = APIRouter()


class LoginRequest(BaseModel):
    identifier: str
    password: str


@router.get("/api/auth/session")
async def get_session(request: Request):
    user = get_authenticated_user(request)
    if not user:
        return {"authenticated": False}
    return {"authenticated": True, "identifier": user["identifier"], "role": user["role"]}


@router.post("/api/auth/login")
async def login(body: LoginRequest, response: Response):
    if not admin_login_ready():
        raise HTTPException(status_code=503, detail="Login is not configured")

    user = authenticate_user(body.identifier, body.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    response.set_cookie(
        value=build_session_token(user["identifier"], user["role"]),
        **session_cookie_settings(),
    )
    return {"authenticated": True, "identifier": user["identifier"], "role": user["role"]}


@router.post("/api/auth/logout")
async def logout(response: Response):
    response.delete_cookie(
        session_cookie_settings()["key"],
        path="/",
        secure=session_cookie_settings()["secure"],
        samesite=session_cookie_settings()["samesite"],
    )
    return {"authenticated": False}
