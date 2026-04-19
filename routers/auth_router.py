"""
PDDikti Dosen Explorer — Auth Router
Handles login, logout, session check, and heartbeat.
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select, update, and_
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    hash_password, verify_password, create_access_token,
    get_current_user, check_single_user_lock, log_activity,
    get_admin_user
)
from database import get_db
from models import User, UserSession

router = APIRouter(prefix="/api/auth", tags=["auth"])


# ── Schemas ──

class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    user: dict
    message: str


class MeResponse(BaseModel):
    user: dict
    is_locked: bool = False
    locked_by: Optional[str] = None


# ── Endpoints ──

@router.post("/login", response_model=LoginResponse)
async def login(
    req: LoginRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Login user. Single-user lock: only one non-admin user at a time."""

    # Find user
    result = await db.execute(
        select(User).where(User.username == req.username)
    )
    user = result.scalar_one_or_none()

    if not user or not verify_password(req.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Username atau password salah"
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Akun telah dinonaktifkan. Hubungi admin."
        )

    # Single-user lock check (admins bypass this)
    if user.role != "admin":
        active_user = await check_single_user_lock(db, exclude_user_id=user.id)
        if active_user:
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail=f"Sistem sedang digunakan oleh {active_user.display_name}. "
                       f"Silakan tunggu hingga user tersebut selesai atau logout."
            )

    # Invalidate old sessions for this user
    result = await db.execute(
        select(UserSession).where(
            and_(UserSession.user_id == user.id, UserSession.is_active == True)
        )
    )
    old_sessions = result.scalars().all()
    for s in old_sessions:
        s.is_active = False

    # Create JWT
    token = create_access_token({"sub": str(user.id), "role": user.role})

    # Create session
    session = UserSession(
        user_id=user.id,
        token=token,
        is_active=True,
        last_activity=datetime.now(timezone.utc),
    )
    db.add(session)

    # Update user status
    user.is_online = True
    user.last_login = datetime.now(timezone.utc)
    user.last_activity = datetime.now(timezone.utc)

    await db.commit()

    # Log activity
    client_ip = request.client.host if request.client else None
    await log_activity(db, user.id, "login", f"Login dari {client_ip}", client_ip)

    return LoginResponse(
        token=token,
        user={
            "id": user.id,
            "username": user.username,
            "display_name": user.display_name,
            "role": user.role,
        },
        message="Login berhasil"
    )


@router.post("/logout")
async def logout(
    request: Request,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Logout current user."""
    # Invalidate all sessions
    result = await db.execute(
        select(UserSession).where(
            and_(UserSession.user_id == user.id, UserSession.is_active == True)
        )
    )
    sessions = result.scalars().all()
    for s in sessions:
        s.is_active = False

    user.is_online = False
    user.last_logout = datetime.now(timezone.utc)
    await db.commit()

    client_ip = request.client.host if request.client else None
    await log_activity(db, user.id, "logout", "Manual logout", client_ip)

    return {"message": "Logout berhasil"}


@router.get("/me")
async def get_me(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get current user info + lock status."""
    locked_user = await check_single_user_lock(db, exclude_user_id=user.id)

    return {
        "user": {
            "id": user.id,
            "username": user.username,
            "display_name": user.display_name,
            "role": user.role,
            "is_online": user.is_online,
            "last_activity": user.last_activity.isoformat() if user.last_activity else None,
        },
        "is_locked": locked_user is not None and user.role != "admin",
        "locked_by": locked_user.display_name if locked_user and user.role != "admin" else None,
    }


@router.post("/heartbeat")
async def heartbeat(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Heartbeat — update activity timestamp to prevent auto-logout."""
    user.last_activity = datetime.now(timezone.utc)

    # Also update session
    result = await db.execute(
        select(UserSession).where(
            and_(UserSession.user_id == user.id, UserSession.is_active == True)
        )
    )
    session = result.scalar_one_or_none()
    if session:
        session.last_activity = datetime.now(timezone.utc)

    await db.commit()
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get("/check-lock")
async def check_lock(db: AsyncSession = Depends(get_db)):
    """
    Public endpoint — check if system is locked by a user.
    Used on login page to show lock status.
    """
    active_user = await check_single_user_lock(db)
    if active_user:
        return {
            "is_locked": True,
            "locked_by": active_user.display_name,
            "message": f"Sistem sedang digunakan oleh {active_user.display_name}"
        }
    return {"is_locked": False, "locked_by": None, "message": "Sistem tersedia"}
