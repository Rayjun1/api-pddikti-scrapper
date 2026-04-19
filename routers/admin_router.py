"""
PDDikti Dosen Explorer — Admin Router
User CRUD + activity monitoring for admin.
"""

from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select, func, and_, desc, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from auth import get_admin_user, hash_password, log_activity
from database import get_db
from models import User, UserSession, UserActivity

router = APIRouter(prefix="/api/admin", tags=["admin"])


# ── Schemas ──

class CreateUserRequest(BaseModel):
    username: str
    password: str
    display_name: str
    role: str = "user"


class UpdateUserRequest(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    display_name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


# ── User CRUD ──

@router.get("/users")
async def list_users(
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """List all users with online status."""
    result = await db.execute(
        select(User).order_by(User.created_at.desc())
    )
    users = result.scalars().all()

    # Check active sessions
    session_result = await db.execute(
        select(UserSession).where(UserSession.is_active == True)
    )
    active_sessions = {s.user_id: s for s in session_result.scalars().all()}

    user_list = []
    for u in users:
        has_session = u.id in active_sessions
        session = active_sessions.get(u.id)

        user_list.append({
            "id": u.id,
            "username": u.username,
            "display_name": u.display_name,
            "role": u.role,
            "is_active": u.is_active,
            "is_online": has_session,
            "last_login": u.last_login.isoformat() if u.last_login else None,
            "last_logout": u.last_logout.isoformat() if u.last_logout else None,
            "last_activity": session.last_activity.isoformat() if session and session.last_activity else (
                u.last_activity.isoformat() if u.last_activity else None
            ),
            "created_at": u.created_at.isoformat() if u.created_at else None,
        })

    return {"users": user_list, "total": len(user_list)}


@router.post("/users")
async def create_user(
    req: CreateUserRequest,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new user."""
    # Check if username exists
    existing = await db.execute(
        select(User).where(User.username == req.username)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Username '{req.username}' sudah digunakan"
        )

    user = User(
        username=req.username,
        password_hash=hash_password(req.password),
        display_name=req.display_name,
        role=req.role if req.role in ("admin", "user") else "user",
        is_active=True,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)

    await log_activity(db, admin.id, "create_user", f"Created user: {req.username}")

    return {
        "message": f"User '{req.username}' berhasil dibuat",
        "user": {
            "id": user.id,
            "username": user.username,
            "display_name": user.display_name,
            "role": user.role,
        }
    }


@router.put("/users/{user_id}")
async def update_user(
    user_id: int,
    req: UpdateUserRequest,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Update user info."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    if req.username is not None:
        # Check duplicate
        dup = await db.execute(
            select(User).where(and_(User.username == req.username, User.id != user_id))
        )
        if dup.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Username sudah digunakan")
        user.username = req.username

    if req.password is not None:
        user.password_hash = hash_password(req.password)
    if req.display_name is not None:
        user.display_name = req.display_name
    if req.role is not None and req.role in ("admin", "user"):
        user.role = req.role
    if req.is_active is not None:
        user.is_active = req.is_active
        if not req.is_active:
            # Deactivated: kill all sessions
            sess_result = await db.execute(
                select(UserSession).where(
                    and_(UserSession.user_id == user_id, UserSession.is_active == True)
                )
            )
            for s in sess_result.scalars().all():
                s.is_active = False
            user.is_online = False

    await db.commit()
    await log_activity(db, admin.id, "update_user", f"Updated user #{user_id}: {user.username}")

    return {"message": f"User '{user.username}' berhasil diupdate"}


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: int,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a user."""
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    if user.role == "admin" and user.username == "admin":
        raise HTTPException(status_code=403, detail="Tidak bisa menghapus admin utama")

    username = user.username
    await db.delete(user)
    await db.commit()
    await log_activity(db, admin.id, "delete_user", f"Deleted user: {username}")

    return {"message": f"User '{username}' berhasil dihapus"}


@router.post("/users/{user_id}/force-logout")
async def force_logout(
    user_id: int,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Force-logout a user."""
    result = await db.execute(
        select(UserSession).where(
            and_(UserSession.user_id == user_id, UserSession.is_active == True)
        )
    )
    sessions = result.scalars().all()
    for s in sessions:
        s.is_active = False

    await db.execute(
        update(User).where(User.id == user_id).values(
            is_online=False,
            last_logout=datetime.now(timezone.utc)
        )
    )
    await db.commit()
    await log_activity(db, admin.id, "force_logout", f"Force-logout user #{user_id}")

    return {"message": "User berhasil di-logout"}


# ── Activity Monitoring ──

@router.get("/activities")
async def get_activities(
    user_id: Optional[int] = Query(None),
    action: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Get user activity log."""
    query = select(UserActivity).order_by(desc(UserActivity.created_at))

    if user_id:
        query = query.where(UserActivity.user_id == user_id)
    if action:
        query = query.where(UserActivity.action == action)

    # Count total
    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar()

    # Paginate
    query = query.offset((page - 1) * per_page).limit(per_page)
    result = await db.execute(query)
    activities = result.scalars().all()

    # Get user names
    user_ids = list(set(a.user_id for a in activities))
    if user_ids:
        users_result = await db.execute(select(User).where(User.id.in_(user_ids)))
        users_map = {u.id: u for u in users_result.scalars().all()}
    else:
        users_map = {}

    return {
        "activities": [
            {
                "id": a.id,
                "user_id": a.user_id,
                "username": users_map[a.user_id].username if a.user_id in users_map else "?",
                "display_name": users_map[a.user_id].display_name if a.user_id in users_map else "?",
                "action": a.action,
                "detail": a.detail,
                "ip_address": a.ip_address,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in activities
        ],
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if total else 0,
        }
    }


@router.get("/dashboard")
async def admin_dashboard(
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Admin dashboard summary."""
    # Total users
    total_users = (await db.execute(select(func.count()).select_from(User))).scalar()

    # Online users
    session_result = await db.execute(
        select(UserSession).where(UserSession.is_active == True)
    )
    online_user_ids = set()
    for s in session_result.scalars().all():
        online_user_ids.add(s.user_id)

    # Recent activities (last 10)
    recent = await db.execute(
        select(UserActivity).order_by(desc(UserActivity.created_at)).limit(10)
    )
    recent_activities = recent.scalars().all()

    user_ids = list(set(a.user_id for a in recent_activities))
    if user_ids:
        users_result = await db.execute(select(User).where(User.id.in_(user_ids)))
        users_map = {u.id: u for u in users_result.scalars().all()}
    else:
        users_map = {}

    return {
        "total_users": total_users,
        "online_users": len(online_user_ids),
        "recent_activities": [
            {
                "id": a.id,
                "username": users_map[a.user_id].display_name if a.user_id in users_map else "?",
                "action": a.action,
                "detail": a.detail,
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in recent_activities
        ]
    }
